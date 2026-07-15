"""
Amazon 产品采集系统 v2 - Worker 采集引擎（流水线 + 自适应并发）

架构：
  task_feeder  → [task_queue] → worker_pool (N个独立协程) → [result_queue] → batch_submitter
  
  adaptive_controller 实时调整 N 的大小

连接中央服务器 API 拉取任务、推送结果
启动方式：python worker.py --server http://x.x.x.x:8899
"""
import asyncio
import argparse
import logging
import os
import random
import re
import time
import uuid
import signal
import sys
from typing import Optional, Dict, List

import aiofiles
import httpx

from common import config
from worker.proxy import get_proxy_manager
from worker.session import AmazonSession
from worker.parser import AmazonParser as _ParserClass
from worker.metrics import MetricsCollector
from worker.adaptive import AdaptiveController, TokenBucket
from worker.ziputil import zip_effective_in_html

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class SessionSlot:
    """一个采集协程私有的 Amazon session + 轮换状态。

    每个 worker 协程独占一个 slot、一个 AmazonSession。协程**串行**使用自己的
    session（改邮编 → 采集 → 处理），所以切邮编无需 drain 全局在飞请求——只有本
    协程用这个 session。多个 slot 的不同邮编由此真正并行（消除单共享 session +
    drain 造成的串行）。

    轮换规则下沉到每个 slot 各自维护（不再全局）：
      - 被封 / 验证码 / 非美国区 → 即刻冷轮换（5s 防抖）
      - 空标题 / 降级页 → 累计计数，宽限期外/达阈值即轮换
      - 主动轮换 → 成功累计到 SESSION_ROTATE_EVERY
    一个 slot 轮换只影响它自己，其它 slot 继续采集（故不再需要全局热备 hot-swap）。
    """
    __slots__ = ("_w", "session", "_success_since_rotate", "_empty_title_count",
                 "_last_rotate_time", "_grace_until", "_restart_epoch")

    def __init__(self, worker):
        self._w = worker
        self.session: Optional[AmazonSession] = None
        self._success_since_rotate = 0
        self._empty_title_count = 0
        self._last_rotate_time = 0.0
        self._grace_until = 0.0
        self._restart_epoch = worker._restart_epoch

    async def ensure_ready(self) -> bool:
        """确保 slot 有可用 session；软重启 epoch 变化时强制重建。"""
        if self._restart_epoch != self._w._restart_epoch:
            self._restart_epoch = self._w._restart_epoch
            await self._close()
        if self.session is not None and self.session.is_ready():
            return True
        await self._close()
        self.session = await self._w._create_session_with_retry(delay=3)
        self._success_since_rotate = 0
        self._empty_title_count = 0
        return self.session is not None and self.session.is_ready()

    async def ensure_zip(self, target_zip: str) -> bool:
        """把本 slot 的 session 切到目标邮编（无 drain）。返回是否成功。

        Tier 2a：on_fetch 模式下切邮编只 POST、不发独立验证 GET（verify=False），
        邮编是否生效改由 _process_task 用商品页 HTML 判定。standalone 模式保持
        POST + 验证 GET 的旧行为。
        """
        if not target_zip or self.session is None:
            return True
        if self.session.zip_code == target_zip:
            return True
        verify = getattr(config, "ZIP_VERIFY_MODE", "standalone") != "on_fetch"
        return await self.session.change_zip_code(target_zip, verify=verify)

    def note_success(self):
        self._success_since_rotate += 1

    def should_rotate_proactive(self) -> bool:
        return self._success_since_rotate >= self._w._rotate_every

    async def rotate(self, reason: str = "轮换"):
        """冷轮换本 slot 的 session（5s 防抖）。只影响本 slot，其它 slot 不停。"""
        now = time.monotonic()
        if now - self._last_rotate_time < 5:
            return
        logger.info(f"🔄 [slot] Session {reason}，冷轮换")
        await self._close()
        try:
            await self._w.proxy_manager.report_blocked()
        except Exception:
            pass
        self.session = await self._w._create_session_with_retry(delay=1)
        self._success_since_rotate = 0
        self._empty_title_count = 0
        self._last_rotate_time = time.monotonic()
        self._grace_until = time.monotonic() + 3.0
        if self.session:
            logger.info("🔄 [slot] 冷轮换完成")
        else:
            logger.error("🔄 [slot] 冷轮换失败，将于下次 ensure_ready 重试")

    async def rotate_on_empty(self, asin: str, reason: str = ""):
        """空标题/降级页：累计计数；宽限期外则轮换本 slot（机制1+机制2）。"""
        self._empty_title_count += 1
        if time.monotonic() < self._grace_until:
            return  # 刚轮换过的宽限期内，不重复轮换
        if self._empty_title_count >= self._w._empty_title_rotate_threshold:
            await self.rotate(reason=f"空标题累计 {self._empty_title_count} 次")
        else:
            await self.rotate(reason=reason or "空标题")

    async def close(self):
        await self._close()

    async def _close(self):
        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass
            self.session = None


class Worker:
    """流水线异步采集 Worker"""

    def __init__(self, server_url: str, worker_id: str = None, concurrency: int = None,
                 zip_code: str = None, enable_screenshot: bool = True,
                 api_key: str = None, auto_restart_hours: float = 0):
        self.server_url = server_url.rstrip("/")
        self._api_key = api_key or os.environ.get("WORKER_API_KEY", "")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self._enable_screenshot = enable_screenshot
        # 定时自动重启（小时）：防止长跑引发的 Playwright 泄漏 / session 状态累积
        self._auto_restart_hours = auto_restart_hours or 0
        self._wants_restart = False  # 由定时协程置位；main() 关停后检查决定是否 os.execv
        # 启动健康检查：跟踪 settings 是否成功同步过、是否处理过任务
        self._has_synced_once = False
        # 启动健康超时（秒）：启动后此时长内若无任务也无心跳成功，则触发自动重启
        self._startup_grace_seconds = 60
        # session 初始化硬上限（秒）：超时即触发重启，避免 execv 后陷入死等
        self._init_timeout_seconds = 90

        # 组件
        self.proxy_manager = get_proxy_manager()
        self.parser = _ParserClass()
        # 会话不再全局共享：每个采集协程持有私有 SessionSlot（见 _worker_loop）。
        self._rate_limiter = TokenBucket()

        # 自适应并发控制
        self._metrics = MetricsCollector()
        max_c = concurrency or config.MAX_CONCURRENCY
        initial_c = config.INITIAL_CONCURRENCY
        self._controller = AdaptiveController(
            initial=initial_c,
            min_c=config.MIN_CONCURRENCY,
            max_c=max_c,
            metrics=self._metrics,
        )

        # per-ASIN 邮编切换：每个协程私有 session，切 zip 只影响本协程、无需 drain。
        # 不同协程的不同邮编由此真正并行（见 SessionSlot.ensure_zip）。

        # 任务队列（优先级队列：首次请求 priority=0 优先处理，重试请求 priority=1 低优先级）
        self._task_queue: asyncio.PriorityQueue = None
        self._task_seq = 0  # 单调递增序号，同优先级内 FIFO
        self._queue_size = getattr(config, "TASK_QUEUE_SIZE", 100)
        self._prefetch_threshold = getattr(config, "TASK_PREFETCH_THRESHOLD", 0.5)

        # 统计（双口径：采集层 success/failed + 提交层 accepted/stale）
        self._stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "accepted": 0,
            "stale": 0,
            "start_time": None,
        }

        # 运行控制
        self._running = False

        # 批量提交队列
        self._result_queue: asyncio.Queue = None
        self._batch_submitter_task: Optional[asyncio.Task] = None
        self._batch_size = 10
        self._batch_interval = 2.0  # 秒
        # 并行 batch_submitter 数量：单 submitter HTTP retry 时会卡 30+ 秒，
        # 期间 result_queue 持续填充，10 worker × 500 queue = 5000 卡住
        # 多协程共用同一个 _result_queue，一个 stuck 不影响其他
        self._submitter_count = 3

        # 实例级运行参数（不污染全局 config）
        self._max_retries = config.MAX_RETRIES

        # Session 轮换控制（阈值全局配置，实际计数/防抖/宽限期下沉到各 SessionSlot）
        self._rotate_every = config.SESSION_ROTATE_EVERY   # 主动轮换：每 N 次成功
        self._empty_title_rotate_threshold = 15            # 机制 2：空标题累计阈值
        # 软重启 epoch：server 下发 restart 或本地软重启时 +1，各 SessionSlot 在
        # 下次 ensure_ready 时感知并强制重建自己的 session（替代原全局 session 重建）。
        self._restart_epoch = 0
        # 会话初始化并发闸门：平滑冷启动/大规模轮换时的代理 CONNECT 突发（见
        # _create_session_with_retry）。稳态下 session 复用、初始化很少，几乎无副作用。
        self._session_init_sem = asyncio.Semaphore(
            getattr(config, "SESSION_INIT_CONCURRENCY", 3)
        )

        # Worker 协程管理
        self._worker_tasks: List[asyncio.Task] = []
        self._active_task_count = 0

        # 截图：独立子进程架构（采集与截图完全隔离事件循环）
        self._browsers_count = 1             # 截图浏览器实例数
        self._pages_per_browser = 5          # 每个浏览器并发 page 数
        cache_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshot_cache")
        server_key = re.sub(r"[^A-Za-z0-9_.-]", "_", self.server_url)
        worker_key = re.sub(r"[^A-Za-z0-9_.-]", "_", self.worker_id)
        self._screenshot_base_dir = os.path.join(cache_root, f"{server_key}__{worker_key}")
        self._screenshot_html_dir = os.path.join(self._screenshot_base_dir, "html")
        self._screenshot_process: Optional[asyncio.subprocess.Process] = None
        self._screenshot_pgid: Optional[int] = None
        self._screenshot_log_task: Optional[asyncio.Task] = None
        self._screenshot_lock = asyncio.Lock()  # 防止并发创建多个截图子进程
        self._screenshot_pending_batches: set = set()
        self._screenshot_batch_ids: Dict[str, int] = {}
        self._screenshot_gate = asyncio.Event()
        self._screenshot_gate.set()

        # 优雅关停：用于让长 wait_for / 重试循环立即解锁，避免 stop 后 5 分钟才退出
        self._shutdown_event = asyncio.Event()

        # 设置同步
        self._settings_version = 0

        # 全局并发协调
        self._global_block_epoch = 0   # 已处理的全局封锁 epoch
        self._recovery_jitter = 0.5    # Server 分配的恢复抖动系数

        # Tier 2a on_fetch 观测：商品页 glow 判定的命中分布，用于真机 A/B 判断安全性。
        #   match=生效(True) / mismatch=未生效(False) / unknown=商品页无 glow(None，宽松放行)
        # unknown 占比高 → 商品页不稳定暴露邮编 → on_fetch 不安全，应弃用或换信号。
        self._zip_onfetch = {"match": 0, "mismatch": 0, "unknown": 0}

    def _server_headers(self) -> dict:
        """与 ERP Server 通信的统一 header（含 API Key 认证）"""
        h = {}
        if self._api_key:
            h["X-Worker-Api-Key"] = self._api_key
        return h

    async def start(self):
        """启动 Worker（流水线架构）"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动（流水线模式）")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   初始并发: {self._controller.current_concurrency}")
        logger.info(f"   并发范围: [{config.MIN_CONCURRENCY}, {self._controller._max}]")
        logger.info(f"   邮编: {self.zip_code}")
        logger.info(f"   截图功能: {'开启' if self._enable_screenshot else '关闭'}")
        logger.info(f"   代理模式: TPS")
        logger.info(f"   邮编验证模式: {getattr(config, 'ZIP_VERIFY_MODE', 'standalone')} "
                    f"(on_fetch=省验证GET) | 会话初始化并发: "
                    f"{getattr(config, 'SESSION_INIT_CONCURRENCY', 3)}")

        self._running = True
        self._stats["start_time"] = time.time()

        # 初始化队列
        self._task_queue = asyncio.PriorityQueue(maxsize=self._queue_size)
        self._result_queue = asyncio.Queue(maxsize=500)  # 反压：提交卡住时自动减速采集

        # 启动前先从 Server 拉取设置（代理地址、邮编等），远程 Worker 无需本地配置
        await self._pull_initial_settings()

        # 初始化 session（此时 proxy_url 已从 Server 同步）
        await self._init_session()

        # 初始化阶段已触发重启意图（超时） → 跳过协程启动，让上层 cleanup + execv
        if self._wants_restart and not self._running:
            logger.warning("⚠️ 初始化阶段触发重启信号，跳过协程启动直接进入清理")
            await self._cleanup()
            logger.info(f"🛑 Worker [{self.worker_id}] 已停止")
            return

        # 启动自适应控制器
        await self._controller.start()

        # 启动核心协程
        try:
            coroutines = [
                self._task_feeder(),         # 1. 持续从 Server 拉任务
                self._worker_pool(),         # 2. 工人池：自适应并发
                # 3. N 个并行 batch_submitter（共用 _result_queue）
                # 单个 submitter HTTP retry 卡住时，其他继续工作，避免 result_queue 堆积
                *[self._batch_submitter() for _ in range(self._submitter_count)],
                self._screenshot_gate_monitor(),  # 4. 截图完成监控（检查子进程标记）
                self._settings_sync(),       # 5. 定期同步服务端设置
                self._startup_watchdog(),    # 6. 启动健康看门狗（60s 内必须有任务/心跳）
            ]
            if self._auto_restart_hours and self._auto_restart_hours > 0:
                coroutines.append(self._auto_restart_timer())
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            pass

        await self._cleanup()
        logger.info(f"🛑 Worker [{self.worker_id}] 已停止")
        self._print_stats()

    async def stop(self):
        """停止 Worker"""
        self._running = False
        # 触发关停事件，让所有长 wait_for（截图门控 / standby 重试 / settings 心跳等）立即解锁
        self._shutdown_event.set()
        # 同时把 screenshot_gate 也 set 一下，防止 task_feeder 卡在 wait
        self._screenshot_gate.set()
        # 提前停掉 AIMD 控制器：它的 _adjust_loop 是独立 task，不在主 gather() 列表中
        # 这样"指标"日志立刻停止刷屏（不必等 _cleanup() 才停）
        try:
            await self._controller.stop()
        except Exception:
            pass
        # 向任务队列放入 None 哨兵，唤醒所有等待的 worker
        # 哨兵用 priority=-1 确保最先被取出
        for _ in range(self._controller._max):
            try:
                self._task_seq += 1
                self._task_queue.put_nowait((-1, self._task_seq, None))
            except (asyncio.QueueFull, AttributeError):
                break

    # ═══════════════════════════════════════════════
    # 流水线三大组件
    # ═══════════════════════════════════════════════

    async def _task_feeder(self):
        """
        任务补给协程：持续从 Server 拉任务，保持队列不空

        当队列低于阈值时，主动拉取新任务填充。
        如果拉到高优先级任务（priority > 0），立即清空当前队列，
        让 Worker 秒级切换到新批次（旧任务靠超时回收）。

        退避策略（优化后）：
        - 服务器错误 → 快速重试 1s（不浪费时间）
        - 真正没任务 → 温和退避 2s → 3s → 5s（上限 5s，旧版 30s 太长）
        """
        logger.info("📡 任务补给协程启动")
        empty_streak = 0  # 连续"真正无任务"计数（不含服务器错误）

        while self._running:
            try:
                queue_size = self._task_queue.qsize()

                # 截图门控：队列已空且没有在途任务 + 有未完成的截图批次
                # 仅在最后一个任务真正处理完成后再写 _scraping_done，避免过早放行。
                if queue_size == 0 and self._active_task_count == 0 and self._screenshot_pending_batches:
                    scrape_complete_batches = set()
                    for batch in self._screenshot_pending_batches:
                        batch_id = self._screenshot_batch_ids.get(batch)
                        if batch_id and await self._is_batch_scrape_complete(batch_id):
                            scrape_complete_batches.add(batch)

                    # 为已采完的批次写 _scraping_done 标记（让截图子进程可以收尾该批）
                    for batch in scrape_complete_batches:
                        marker = os.path.join(self._screenshot_html_dir, batch, "_scraping_done")
                        if not os.path.exists(marker):
                            os.makedirs(os.path.dirname(marker), exist_ok=True)
                            with open(marker, "w") as f:
                                f.write(str(time.time()))

                    # 只要还有批次没采完（server 仍有 pending/processing，例如
                    # zip_switch_failed 重入队的任务），就【不门控】，落到下面正常拉取
                    # 把这些任务捞回来重试。否则 worker 会卡在这里空转轮询 /api/progress、
                    # 又不拉那些 pending 任务 → 活锁（批次永远到不了 100%）。
                    incomplete_batches = self._screenshot_pending_batches - scrape_complete_batches
                    if not incomplete_batches:
                        pending = len(scrape_complete_batches)
                        logger.info(f"⏸️ 等待截图完成后再拉取新任务（{pending} 个批次待处理）")
                        self._screenshot_gate.clear()

                        # 检测截图子进程是否存活，崩溃则强制放行
                        if self._screenshot_process and self._screenshot_process.returncode is not None:
                            logger.warning(f"📸 截图子进程已退出 (code={self._screenshot_process.returncode})，跳过门控")
                            self._screenshot_pending_batches.clear()
                            self._screenshot_batch_ids.clear()
                            self._screenshot_gate.set()
                            continue

                        # 门控超时：最多等 5 分钟，避免子进程异常导致 Worker 永久卡死
                        try:
                            await asyncio.wait_for(self._screenshot_gate.wait(), timeout=300)
                            if not self._running:
                                # shutdown 触发的解锁（stop() 里 set 了 gate）
                                break
                            logger.info("▶️ 截图批次已完成，继续拉取新任务")
                        except asyncio.TimeoutError:
                            logger.warning("⚠️ 截图门控超时（5分钟），强制放行继续拉取任务")
                            self._screenshot_pending_batches.clear()
                            self._screenshot_batch_ids.clear()
                            self._screenshot_gate.set()
                        continue
                    # 还有批次未采完 → 不 continue，落到下面正常拉取逻辑
                threshold = int(self._queue_size * self._prefetch_threshold)

                if queue_size < threshold:
                    # 拉取量 = 当前并发数（缩短暴露窗口），但不超过队列剩余空间
                    fetch_count = min(
                        self._controller.current_concurrency,
                        self._queue_size - queue_size,
                    )
                    fetch_count = max(fetch_count, 5)  # 至少拉 5 个

                    tasks = await self._pull_tasks(count=fetch_count)

                    if tasks is None:
                        # 服务器错误或网络异常 → 快速重试（不累加 empty_streak）
                        await asyncio.sleep(1)
                        continue

                    if tasks:
                        empty_streak = 0

                        # 检测是否有高优先级任务（优先采集）
                        has_priority = any(t.get("priority", 0) > 0 for t in tasks)
                        if has_priority and not self._task_queue.empty():
                            # 只清空非优先任务，保留已有的优先任务（防止无限循环）
                            dropped_tasks = []
                            kept_items = []
                            while not self._task_queue.empty():
                                try:
                                    item = self._task_queue.get_nowait()
                                    old_task = item[2] if isinstance(item, tuple) else item
                                    if old_task and isinstance(old_task, dict):
                                        if old_task.get("priority", 0) > 0:
                                            kept_items.append(item)
                                        else:
                                            dropped_tasks.append(old_task)
                                except asyncio.QueueEmpty:
                                    break
                            for item in kept_items:
                                await self._task_queue.put(item)
                            if dropped_tasks:
                                logger.info(f"🚀 检测到优先采集任务，已清空队列中 {len(dropped_tasks)} 个普通任务（保留 {len(kept_items)} 个优先任务）")
                                asyncio.create_task(self._release_tasks(dropped_tasks))

                        for task in tasks:
                            # 首次请求 priority=0（优先处理），重试请求 priority=1（低优先级）
                            prio = 0 if task.get("retry_count", 0) == 0 else 1
                            self._task_seq += 1
                            await self._task_queue.put((prio, self._task_seq, task))
                        logger.debug(f"📡 补给 {len(tasks)} 个任务 (队列: {self._task_queue.qsize()})")
                    else:
                        # 真正没有待处理任务 → 温和退避（上限 5s，避免长时间空闲）
                        empty_streak += 1
                        wait = min(2 * empty_streak, 5)
                        logger.info(f"📭 暂无任务，等待 {wait} 秒... (队列剩余: {queue_size})")
                        await asyncio.sleep(wait)
                else:
                    # 队列充足，短暂休息
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 任务补给异常: {e}")
                await asyncio.sleep(1)  # 异常也快速重试

        logger.info("📡 任务补给协程退出")

    async def _worker_pool(self):
        """
        工人池协程：管理动态数量的 worker 协程
        
        每个 worker 独立循环：acquire → 取任务 → 处理 → release → 循环
        """
        logger.info("⚙️ 工人池启动")
        
        # 启动初始 worker 协程，错开启动时间
        initial = self._controller.current_concurrency
        for i in range(initial):
            task = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(task)

        # 监控循环：根据并发变化动态增减 worker
        last_target = initial
        while self._running:
            await asyncio.sleep(2)  # 每 2 秒检查一次
            
            target = self._controller.current_concurrency
            current = len([t for t in self._worker_tasks if not t.done()])
            
            if target > current:
                # 需要更多 worker
                for i in range(target - current):
                    idx = len(self._worker_tasks)
                    task = asyncio.create_task(self._worker_loop(idx))
                    self._worker_tasks.append(task)
                if target != last_target:
                    logger.info(f"⚙️ Worker 扩容: {current} → {target}")
            
            last_target = target
            
            # 清理已完成的 task 引用
            self._worker_tasks = [t for t in self._worker_tasks if not t.done()]

        # 等待所有 worker 完成
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        
        logger.info("⚙️ 工人池退出")

    async def _worker_loop(self, worker_idx: int):
        """
        单个 worker 协程：持续取任务处理

        错开启动 → 取任务 → 处理（内部管控信号量）→ 循环

        注：信号量 acquire/release 已移入 _process_task 内部，仅包裹 HTTP 请求，
        令牌桶等待、session 就绪等待、重试 sleep 等不再占用信号量槽位。
        """
        # 错开启动，分散请求（也让各 slot 的 session 初始化错峰，缓解代理 CONNECT 突发）
        initial_c = self._controller.current_concurrency
        if initial_c > 0:
            stagger = worker_idx * (1.0 / initial_c)
            stagger = min(stagger, 2.0)  # 最多错开 2 秒
            if stagger > 0:
                await asyncio.sleep(stagger)

        # 本协程私有的 SessionSlot：独立 session + 轮换状态，无共享、切邮编无需 drain。
        slot = SessionSlot(self)
        try:
            while self._running:
                try:
                    # 1. 从优先级队列取任务（最多等 5 秒，不占信号量）
                    try:
                        item = await asyncio.wait_for(
                            self._task_queue.get(), timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        continue

                    # 2. 从优先级元组中提取 task dict: (priority, seq, task)
                    task = item[2] if isinstance(item, tuple) else item

                    # 3. 哨兵值 → 退出
                    if task is None:
                        break

                    # 3. 处理任务
                    # 指标（latency, success, blocked）在 _process_task 内部按每次 HTTP 请求记录，
                    # 确保 AIMD 看到的 p50 延迟是真实的 HTTP 往返时间，而非含重试/等待的总任务时间。
                    self._active_task_count += 1
                    try:
                        if task.get("task_type") == "discover_seller":
                            await self._process_seller_task(task, slot)
                        else:
                            await self._process_task(task, slot)
                    finally:
                        self._active_task_count = max(0, self._active_task_count - 1)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Worker-{worker_idx} 未捕获异常: {type(e).__name__}: {e}")
                    # 不提交 failed（避免需要手动重试），让任务留在 processing
                    # 由 Server 端超时回收机制自动重置为 pending 重新分发
                    await asyncio.sleep(1)
        finally:
            await slot.close()

    # ═══════════════════════════════════════════════
    # 核心处理逻辑
    # ═══════════════════════════════════════════════

    async def _apply_settings(self, s: dict, is_initial: bool = False) -> list:
        """统一应用设置字段，返回变更描述列表。

        is_initial=True:  启动时调用，无版本检查，处理 zip_code 和 initial_concurrency
        is_initial=False: 运行时同步，受版本守护和配额约束
        """
        changes = []
        has_quota = "_quota" in s

        # --- 代理地址 ---
        new_proxy_url = s.get("proxy_url") or s.get("tunnel_proxy_url", "")
        if new_proxy_url and new_proxy_url != config.PROXY_URL:
            config.PROXY_URL = new_proxy_url
            self.proxy_manager._proxy_url = new_proxy_url
            changes.append(f"proxy=***{new_proxy_url[-20:]}")

        # --- 邮编（仅初始同步）---
        if is_initial:
            new_zip = s.get("zip_code")
            if new_zip and self.zip_code == config.DEFAULT_ZIP_CODE and new_zip != self.zip_code:
                self.zip_code = new_zip
                changes.append(f"zip_code={new_zip}")

        # --- 令牌桶 QPS（运行时受配额守护）---
        if is_initial or not has_quota:
            new_rate = s.get("token_bucket_rate")
            if new_rate and self._rate_limiter and new_rate != self._rate_limiter.rate:
                self._rate_limiter.rate = new_rate
                changes.append(f"QPS={new_rate}")

        # --- 并发控制：min / max / initial ---
        new_min = s.get("min_concurrency")
        if new_min and new_min != self._controller._min:
            self._controller._min = new_min
            changes.append(f"min_c={new_min}")

        if is_initial or not has_quota:
            new_max = s.get("max_concurrency")
            if new_max and new_max != self._controller._max:
                self._controller._max = new_max
                changes.append(f"max_c={new_max}")

        if is_initial:
            new_initial = s.get("initial_concurrency")
            if new_initial and new_initial != self._controller._concurrency:
                clamped = max(self._controller._min, min(self._controller._max, new_initial))
                self._controller._concurrency = clamped
                self._controller._semaphore = asyncio.Semaphore(clamped)
                changes.append(f"initial_c={clamped}")

        # --- AIMD 调控参数 ---
        for attr, key in [
            ("_adjust_interval", "adjust_interval"),
            ("_target_latency", "target_latency"),
            ("_max_latency", "max_latency"),
            ("_target_success", "target_success_rate"),
            ("_min_success", "min_success_rate"),
            ("_block_threshold", "block_rate_threshold"),
            ("_cooldown_duration", "cooldown_after_block"),
        ]:
            val = s.get(key)
            if val is not None and val != getattr(self._controller, attr, None):
                setattr(self._controller, attr, val)
                changes.append(f"{key}={val}")

        # --- 带宽上限 ---
        new_bw = s.get("proxy_bandwidth_mbps")
        if new_bw is not None and new_bw != config.PROXY_BANDWIDTH_MBPS:
            config.PROXY_BANDWIDTH_MBPS = new_bw
            changes.append(f"bandwidth={new_bw}Mbps")

        # --- 其他运行参数 ---
        new_rotate = s.get("session_rotate_every")
        if new_rotate and new_rotate != self._rotate_every:
            self._rotate_every = new_rotate
            changes.append(f"rotate={new_rotate}")

        new_retries = s.get("max_retries")
        if new_retries and new_retries != self._max_retries:
            self._max_retries = new_retries
            changes.append(f"retries={new_retries}")

        new_timeout = s.get("request_timeout")
        if new_timeout and new_timeout != config.REQUEST_TIMEOUT:
            config.REQUEST_TIMEOUT = new_timeout
            changes.append(f"timeout={new_timeout}s")

        new_browsers = s.get("screenshot_browsers")
        if new_browsers and new_browsers != self._browsers_count:
            self._browsers_count = new_browsers
            changes.append(f"screenshot_browsers={new_browsers} (截图子进程下次启动时生效)")
        new_pages = s.get("screenshot_pages_per_browser")
        if new_pages and new_pages != self._pages_per_browser:
            self._pages_per_browser = new_pages
            changes.append(f"screenshot_pages_per_browser={new_pages} (截图子进程下次启动时生效)")

        return changes

    async def _pull_initial_settings(self):
        """启动时从 Server 拉取一次设置，确保所有运行参数与 Server 一致。"""
        logger.info("⚙️ 从服务器拉取初始设置...")
        try:
            async with httpx.AsyncClient(timeout=5, headers=self._server_headers()) as client:
                resp = await client.get(f"{self.server_url}/api/settings")
                if resp.status_code != 200:
                    logger.warning(f"⚠️ 拉取初始设置失败: HTTP {resp.status_code}")
                    return
                s = resp.json()

            # v3: unwrap settings from response wrapper
            settings_data = s.get("settings", s)
            changes = await self._apply_settings(settings_data, is_initial=True)
            self._settings_version = s.get("version", 0)

            if changes:
                logger.info(f"⚙️ 初始设置已同步: {', '.join(changes)}")
            else:
                logger.info("⚙️ 初始设置已确认（与本地一致）")

            # 启动健康检查：拉到了初始设置也算一次成功心跳
            self._has_synced_once = True

        except Exception as e:
            logger.warning(f"⚠️ 拉取初始设置异常（将使用本地配置）: {e}")

    async def _create_session_with_retry(self, max_attempts: int = 3,
                                         delay: float = 5) -> Optional[AmazonSession]:
        """创建并初始化 AmazonSession，失败时重试。成功返回 session，全部失败返回 None。
        优雅关停感知：shutdown_event 被 set 时立即放弃重试。

        会话池化后，冷启动 / 大规模轮换时可能有多个协程同时建 session；session
        初始化（首页 GET）不走令牌桶，若不加约束会瞬间打爆代理 ~5 QPS CONNECT 预算
        → 429 → 初始化失败 → 更多轮换的死亡螺旋。用 _session_init_sem 把并发初始化
        数量压到小常量，平滑 CONNECT 突发（稳态下 session 复用、初始化很少发生，
        此闸门几乎无副作用）。"""
        for attempt in range(max_attempts):
            if self._shutdown_event.is_set():
                return None
            async with self._session_init_sem:
                if self._shutdown_event.is_set():
                    return None
                session = AmazonSession(self.proxy_manager, self.zip_code)
                if await session.initialize():
                    return session
                logger.warning(f"⚠️ Session 初始化失败 (尝试 {attempt+1}/{max_attempts})")
                await session.close()
            if attempt < max_attempts - 1:
                # 等待重试间隔时也响应 shutdown，避免关停时再卡 5-15s
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay)
                    return None  # shutdown 触发
                except asyncio.TimeoutError:
                    pass  # 正常等待结束，继续下一次尝试
        return None

    async def _init_session(self):
        """启动连通性自检（带硬超时兜底，超时则触发重启而非死等）。

        采集会话不再全局共享——各采集协程的 SessionSlot 会各自懒建 session。
        此处只做一次启动自检：建一个 session 验证代理/网络可用，随即关闭。
        保留原有语义：execv 后若代理网络瞬时不通，初始化重试链可能耗 ~150s，
        一旦超过 `_init_timeout_seconds` 即置位 _wants_restart，让 run_worker.py
        走 os.execv 重新拉起一个干净进程。
        """
        logger.info(f"🔧 启动连通性自检... (硬超时 {self._init_timeout_seconds}s)")
        try:
            session = await asyncio.wait_for(
                self._create_session_with_retry(),
                timeout=self._init_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"❌ 启动自检超过 {self._init_timeout_seconds}s 未完成，触发自动重启"
            )
            self._wants_restart = True
            self._running = False
            self._shutdown_event.set()
            return
        if session:
            await session.close()
            logger.info("🔧 启动连通性自检通过（采集协程将各自懒建 session）")
            return
        logger.error("❌ 启动自检 3 次全部失败，Worker 将在处理任务时继续重试")

    async def _pull_tasks(self, count: int = None):
        """
        从服务器拉取任务（带 prefer_zip 偏好，让 server 优先返回相同邮编的任务）

        Returns:
            List[Dict] — 成功拉到的任务列表
            None — 服务器错误或网络异常（区别于"没有待处理任务"返回的空列表）
        """
        try:
            url = f"{self.server_url}/api/tasks/pull"
            params = {
                "worker_id": self.worker_id,
                "count": count or self._controller.current_concurrency,
                "enable_screenshot": "1" if self._enable_screenshot else "0",
            }
            # worker 默认 zip 作为派发偏好：均一邮编批次下让 server 优先派发同 zip 任务；
            # per-ASIN 邮编批次下每个任务自带 zip、此偏好无副作用（各协程私有 session
            # 各自按任务 zip 切换，见 SessionSlot.ensure_zip）。
            current_zip = self.zip_code or None
            if current_zip:
                params["prefer_zip"] = current_zip
            async with httpx.AsyncClient(timeout=10, headers=self._server_headers()) as client:
                resp = await client.get(url, params=params)
            if resp.status_code == 200:
                tasks = resp.json().get("tasks", [])
                # 本地二次排序：把 prefer_zip 匹配的放最前，剩下按 zip 聚类
                # （server 已排序，此步是双重保险，特别是任务返回时混有不同 zip 的情况）
                if tasks and current_zip:
                    tasks.sort(key=lambda t: (
                        0 if (t.get("zip_code") or "") == current_zip else 1,
                        t.get("zip_code") or "",
                        t.get("id") or 0,
                    ))
                return tasks
            logger.warning(f"拉取任务失败: HTTP {resp.status_code}")
            return None  # 服务器错误，快速重试
        except Exception as e:
            logger.error(f"拉取任务异常: {e}")
            return None  # 网络异常，快速重试

    async def _release_tasks(self, tasks_to_release: list):
        """通知 Server 归还未处理的任务（优先采集切换时调用）

        tasks_to_release: list of task dicts (must have "id" and "lease_epoch")
        """
        try:
            url = f"{self.server_url}/api/tasks/release"
            payload = {
                "worker_id": self.worker_id,
                "tasks": [{"task_id": t["id"], "lease_epoch": t.get("lease_epoch", 0)}
                          for t in tasks_to_release],
            }
            async with httpx.AsyncClient(timeout=10, headers=self._server_headers()) as client:
                resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"已归还 {data.get('released', 0)} 个旧任务到 pending")
            else:
                logger.warning(f"归还任务失败: HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"归还任务异常: {e}")

    async def _settings_sync(self):
        """定期与 Server 同步：上报 metrics + 拉取 settings + 接收配额"""
        logger.info("⚙️ 设置同步协程启动（每 30 秒）")
        while self._running:
            try:
                # 用 wait_for(shutdown_event) 替代 sleep(30)：
                # 关停时立刻退出，无需等满 30s 才检查 _running
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=30)
                    break  # shutdown 触发
                except asyncio.TimeoutError:
                    pass  # 正常 30s 周期到，继续同步
                if not self._running:
                    break

                # 收集本地 metrics 快照
                snap = self._metrics.snapshot()
                payload = {
                    "worker_id": self.worker_id,
                    "enable_screenshot": self._enable_screenshot,
                    "metrics": {
                        "total": snap["total"],
                        "success_rate": snap["success_rate"],
                        "block_rate": snap["block_rate"],
                        "latency_p50": snap["latency_p50"],
                        "latency_p95": snap["latency_p95"],
                        "inflight": snap["inflight"],
                        "bandwidth_bps": snap["bandwidth_bps"],
                        "current_concurrency": self._controller.current_concurrency,
                        "task_queue_size": self._task_queue.qsize() if self._task_queue else 0,
                        "result_queue_size": self._result_queue.qsize() if self._result_queue else 0,
                        "accepted": self._stats.get("accepted", 0),
                        "stale": self._stats.get("stale", 0),
                    },
                }

                # 优先使用新的综合同步端点
                s = None
                async with httpx.AsyncClient(timeout=5, headers=self._server_headers()) as client:
                    try:
                        resp = await client.post(
                            f"{self.server_url}/api/worker/sync",
                            json=payload,
                        )
                        if resp.status_code == 200:
                            s = resp.json()
                    except Exception:
                        pass

                    # 降级：旧版 Server 没有 /api/worker/sync
                    if s is None:
                        resp = await client.get(f"{self.server_url}/api/settings")
                        if resp.status_code == 200:
                            s = resp.json()

                if s is None:
                    continue

                # 心跳成功 → 标记启动健康检查通过（首次必经）
                if not self._has_synced_once:
                    self._has_synced_once = True

                # === 设置同步（版本守护）===
                # v3: unwrap settings from sync response
                ver = s.get("settings_version", s.get("version", 0))
                if ver > self._settings_version:
                    self._settings_version = ver
                    settings_data = s.get("settings", s)
                    changes = await self._apply_settings(settings_data, is_initial=False)
                    if changes:
                        logger.info(f"⚙️ 设置已同步 (v{ver}): {', '.join(changes)}")

                # === 配额执行（每次都执行，不受 version 限制）===
                quota = s.get("quota", s.get("_quota"))
                if quota:
                    new_max_c = quota.get("max_concurrency") or quota.get("concurrency")
                    if new_max_c and new_max_c != self._controller._max:
                        old_max = self._controller._max
                        self._controller._max = new_max_c
                        if self._controller._concurrency > new_max_c:
                            await self._controller._resize_semaphore(
                                self._controller._concurrency, new_max_c
                            )
                            self._controller._concurrency = new_max_c
                        logger.info(f"📊 配额: max_c {old_max}->{new_max_c}")

                    new_qps = quota.get("max_qps") or quota.get("qps")
                    if new_qps and self._rate_limiter and abs(new_qps - self._rate_limiter.rate) > 0.1:
                        old_qps = self._rate_limiter.rate
                        self._rate_limiter.rate = new_qps
                        logger.info(f"📊 配额: QPS {old_qps:.1f}->{new_qps:.1f}")

                # === 全局封锁处理 ===
                block_info = s.get("_global_block", {})
                if block_info.get("active"):
                    epoch = block_info.get("epoch", 0)
                    if epoch > self._global_block_epoch:
                        self._global_block_epoch = epoch
                        new_c = max(
                            self._controller._min,
                            self._controller._concurrency // 2,
                        )
                        if new_c < self._controller._concurrency:
                            await self._controller._resize_semaphore(
                                self._controller._concurrency, new_c
                            )
                            self._controller._concurrency = new_c
                            remaining = block_info.get("remaining_s", 30)
                            self._controller._cooldown_until = time.monotonic() + remaining
                        logger.warning(
                            f"⚠️ 全局封锁 epoch={epoch}, "
                            f"并发 -> {new_c}, 冷却 {block_info.get('remaining_s')}s"
                        )

                # === 恢复抖动系数 ===
                jitter = s.get("_recovery_jitter")
                if jitter is not None:
                    self._recovery_jitter = jitter
                    self._controller._recovery_jitter = jitter

                # === 软重启指令 ===
                if s.get("restart"):
                    logger.info("🔄 收到软重启指令，重建 session...")
                    await self._soft_restart()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"⚙️ 设置同步异常: {e}")

    def _calc_recv_speed(self) -> int:
        """计算 per-request 带宽限速（bytes/s），0 = 不限。"""
        bw_mbps = config.PROXY_BANDWIDTH_MBPS
        if bw_mbps <= 0:
            return 0
        pipe_bps = int(bw_mbps * 1_000_000 / 8)
        return pipe_bps // max(1, self._metrics.inflight)

    async def _is_batch_scrape_complete(self, batch_id: int) -> bool:
        """确认服务端该批次的采集任务是否已全部结束。"""
        url = f"{self.server_url}/api/progress"
        try:
            async with httpx.AsyncClient(timeout=5, headers=self._server_headers()) as client:
                resp = await client.get(url, params={"batch_id": batch_id})
            if resp.status_code != 200:
                return False
            progress = resp.json()
            total = int(progress.get("total", 0))
            finished = int(progress.get("done", 0)) + int(progress.get("failed", 0))
            return total > 0 and finished >= total
        except Exception:
            return False

    async def _apply_jitter(self):
        """微抖动：绑定目标节拍间隔，避免过度随机造成碰撞。"""
        _qps = self._rate_limiter.rate if self._rate_limiter else 5.0
        _jitter_max = min(0.3, 0.5 / _qps)
        await asyncio.sleep(random.uniform(0, _jitter_max))

    async def _process_task(self, task: Dict, slot: "SessionSlot") -> tuple:
        """
        处理单个采集任务

        返回: (success: bool, blocked: bool, resp_bytes: int)

        每个采集协程持有自己的 SessionSlot（私有 session + 轮换状态）。
        协程串行使用自己的 session（切邮编 → 采集 → 处理），切邮编无需 drain
        全局在飞请求；被封/空标题只轮换本 slot 的 session，其它 slot 不受影响。
        """
        asin = task["asin"]
        task_id = task["id"]
        lease_epoch = task.get("lease_epoch", 0)
        zip_code = task.get("zip_code", self.zip_code)
        max_retries = self._max_retries
        resp_bytes = 0
        last_error_type = "network"
        last_error_detail = ""
        attempt = 0
        while attempt < max_retries:
            try:
                # === Session 获取（本协程私有 slot，独立 session）===
                if not await slot.ensure_ready():
                    attempt += 1
                    logger.warning(f"ASIN {asin} session 未就绪 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue
                session = slot.session

                # === per-ASIN 邮编切换 ===
                # 只有本协程用这个 session，切邮编无需 drain 全局在飞请求（这是并行的关键）。
                target_zip = (zip_code or "").strip() or self.zip_code
                if not await slot.ensure_zip(target_zip):
                    # 切换失败：跳过此任务，由 server 超时回收重试（其他协程/其他 session）
                    last_error_type = "zip_switch_failed"
                    last_error_detail = f"current={session.zip_code} target={target_zip}"
                    attempt = max_retries  # 直接放弃，避免本地卡死
                    break

                # 令牌桶限流
                t_token_start = time.time()
                if self._rate_limiter:
                    await self._rate_limiter.acquire()
                t_token_wait = time.time() - t_token_start

                # 发起请求（信号量仅包裹 HTTP 请求，响应处理不占槽位）
                t_sem_start = time.time()
                await self._controller.acquire()
                t_sem_wait = time.time() - t_sem_start
                await self._apply_jitter()
                recv_speed = self._calc_recv_speed()
                req_start = time.time()
                try:
                    resp = await session.fetch_product_page(asin, max_recv_speed=recv_speed)
                    resp_bytes = len(resp.content) if resp and hasattr(resp, 'content') else 0
                finally:
                    req_elapsed = time.time() - req_start
                    self._controller.release()

                # 请求失败（超时/网络异常）→ 不换 IP，等待后重试
                if resp is None:
                    self._controller.record_result(req_elapsed, False, False, 0)
                    attempt += 1
                    logger.warning(f"ASIN {asin} 请求超时 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 真正被封（403/503/验证码）
                if session.is_blocked(resp):
                    # CAPTCHA 自动解决：如果是验证码页面，先尝试 OCR 解决
                    if session.is_captcha(resp):
                        captcha_solved = await session.solve_captcha(resp)
                        if captcha_solved:
                            logger.info(f"ASIN {asin} CAPTCHA 已自动解决，重新请求")
                            # 解决成功，不计为 blocked，直接重试（不增加 attempt）
                            continue

                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = f"HTTP {resp.status_code}"
                    logger.warning(f"ASIN {asin} 被封 HTTP {resp.status_code} (尝试 {attempt}/{max_retries})")
                    await slot.rotate(reason="被封锁")
                    await self._submit_result(
                        task_id, None, success=False,
                        error_type=last_error_type, error_detail=last_error_detail,
                        batch_id=task.get("batch_id"), lease_epoch=lease_epoch
                    )
                    self._stats["failed"] += 1
                    self._stats["total"] += 1
                    return (False, True, resp_bytes)

                # 404 处理
                if session.is_404(resp):
                    self._controller.record_result(req_elapsed, True, False, resp_bytes)
                    logger.info(f"ASIN {asin} 商品不存在 (404)")
                    result_data = self.parser._default_result(asin, zip_code)
                    result_data["title"] = "[商品不存在]"
                    result_data["batch_name"] = task.get("batch_name", "")
                    await self._submit_result(task_id, result_data, success=True, batch_id=task.get("batch_id"), lease_epoch=lease_epoch)
                    if task.get("needs_screenshot") and self._enable_screenshot:
                        await self._enqueue_screenshot_html(
                            asin=asin,
                            batch_name=task.get("batch_name", ""),
                            batch_id=task.get("batch_id"),
                            html_content=self._build_missing_product_html(asin),
                        )
                    self._stats["success"] += 1
                    self._stats["total"] += 1
                    return (True, False, resp_bytes)

                # 解析页面
                t_parse_start = time.time()
                result_data = self.parser.parse_product(resp.text, asin, zip_code)
                t_parse = time.time() - t_parse_start
                result_data["batch_name"] = task.get("batch_name", "")

                # 检查是否是拦截或空页面
                title = result_data.get("title", "")
                if title == "[验证码拦截]":
                    # CAPTCHA 自动解决：尝试 OCR 识别并提交
                    if session.is_captcha(resp):
                        captcha_solved = await session.solve_captcha(resp)
                        if captcha_solved:
                            logger.info(f"ASIN {asin} 解析层 CAPTCHA 已自动解决，重新请求")
                            continue  # 不增加 attempt，直接重试

                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "captcha"
                    last_error_detail = "validateCaptcha / Robot Check"
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await slot.rotate(reason="页面拦截")
                    continue

                if title == "[API封锁]":
                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = "api-services-support@amazon.com"
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await slot.rotate(reason="页面拦截")
                    continue

                if title in ["[页面为空]", "[HTML解析失败]"]:
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = title
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await slot.rotate_on_empty(asin, reason=title)
                    continue

                # 标题为空视为软拦截（降级页面），触发 session 轮换后重试
                if not title or title == "N/A":
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = "标题为空"
                    logger.warning(f"ASIN {asin} 标题为空 (尝试 {attempt}/{max_retries})")
                    await slot.rotate_on_empty(asin, reason="标题为空")
                    continue

                # variant 偏移检测：多属性产品偶发返回兄弟 variant 的页面
                # （如 B0G6KPHQ4G 请求时实际拿到了 B0GXF5LWC6 的内容）
                # 信号源：页面 <input id="ASIN" value="..."> / canonical link / JS currentAsin
                #
                # 处理策略修正（2026-05-20 复盘）：
                #   早期版本会 _rotate_session 后本地重试，导致两个问题：
                #     1. variant 偏移通常不是 session/cookie 状态引起的，多由 Amazon
                #        A/B test、地域缓存、库存差异引起，rotate 后重试**结果一样**；
                #     2. 大量 rotation 会瞬间打爆隧道代理 5 QPS 限额 → CONNECT 429
                #        → Session 初始化失败 → 全局吞吐崩溃。
                #   新策略：检测到偏移直接失败任务，不轮换 session、不本地重试；
                #   失败结果回传 server 后直接进入终态 failed，避免继续消耗配额。
                page_asin = result_data.get("_page_asin")
                if page_asin and page_asin.upper() != asin.upper():
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    last_error_type = "variant_offset"
                    last_error_detail = f"page={page_asin} requested={asin}"
                    logger.warning(
                        f"⚠️ ASIN {asin} variant 偏移：页面实际是 {page_asin}（不轮换不重试，直接终态失败）"
                    )
                    # 直接跳出 retry 循环，让外层走"任务失败"路径
                    attempt = max_retries
                    break

                # 邮编/货币校验：检测是否采集到了非美国地区的数据
                price = result_data.get("current_price", "")
                if price and price not in ["N/A", "不可售", "See price in cart", "No Featured Offer"]:
                    # v3: [非USD] 前缀或直接出现非美国货币符号
                    if "[非USD]" in price or any(c in price for c in ["¥", "€", "£", "CNY"]) or ("$" not in price and price.replace(",","").replace(".","").strip().isdigit()):
                        self._controller.record_result(req_elapsed, False, True, resp_bytes)
                        attempt += 1
                        last_error_type = "parse_error"
                        last_error_detail = f"非美国价格: {price}"
                        logger.warning(f"ASIN {asin} 非美国价格 '{price}' (尝试 {attempt}/{max_retries})")
                        await slot.rotate(reason="非美国区域数据")
                        continue

                # 核心字段缺失检测：有标题但价格/库存/品牌全为空 → 页面降级，重试
                # "No Featured Offer" 和 "不可售" 是有效状态，不算降级
                _is_nfo = result_data.get("current_price") == "No Featured Offer"
                _is_unavail = result_data.get("current_price") == "不可售"
                _na = {"", "N/A", "N/a", "n/a", "None", None, "0"}
                _core_fields = ["current_price", "buybox_price", "stock_status", "brand"]
                _is_degraded = all(result_data.get(f) in _na for f in _core_fields) and not _is_nfo and not _is_unavail

                if _is_degraded:
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = "解析不完整（核心字段全部缺失）"
                    logger.warning(f"ASIN {asin} 核心字段全部缺失，疑似降级页面 (尝试 {attempt}/{max_retries})")
                    await slot.rotate_on_empty(asin, reason="核心字段缺失")
                    continue

                # Tier 2a：邮编验证折叠——on_fetch 模式下切邮编不发独立验证 GET，
                # 改用刚采到的这张商品页 HTML 自身的 glow 挂件判定目标邮编是否生效。
                #   True  → 生效，正常写库（省掉了一次验证 GET）
                #   False → 明确未生效（glow 显示别的邮编/非美国区）→ 轮换换 IP 重采，不写脏数据
                #   None  → 商品页未暴露 glow 邮编 → 无法判定，宽松放行（非美国区已被上面的货币校验拦掉）
                if (getattr(config, "ZIP_VERIFY_MODE", "standalone") == "on_fetch"
                        and target_zip):
                    zip_eff = zip_effective_in_html(target_zip, resp.text)
                    if zip_eff is False:
                        self._zip_onfetch["mismatch"] += 1
                        self._controller.record_result(req_elapsed, False, False, resp_bytes)
                        attempt += 1
                        last_error_type = "zip_not_effective"
                        last_error_detail = f"商品页邮编未生效: target={target_zip}"
                        logger.warning(f"ASIN {asin} 邮编未生效（目标 {target_zip}）(尝试 {attempt}/{max_retries})")
                        await slot.rotate(reason="邮编未生效")
                        continue
                    # True=命中，None=商品页无 glow（宽松放行）。周期性汇总命中分布。
                    self._zip_onfetch["match" if zip_eff is True else "unknown"] += 1
                    _zt = sum(self._zip_onfetch.values())
                    if _zt % 100 == 0:
                        logger.info(
                            f"📍 on_fetch 命中分布 | 生效:{self._zip_onfetch['match']} "
                            f"未生效:{self._zip_onfetch['mismatch']} "
                            f"无glow:{self._zip_onfetch['unknown']} (共 {_zt})"
                        )

                # v3: No Featured Offer 产品请求 AOD AJAX 端点补充价格/运费/配送/FBA
                if result_data.get("current_price") == "No Featured Offer":
                    try:
                        aod_url = f"https://www.amazon.com/gp/product/ajax/aodAjaxMain/ref=dp_aod_unknown_mbc?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp"
                        olp_resp = await session.fetch_product_page_by_url(aod_url)
                        if olp_resp and hasattr(olp_resp, 'text') and olp_resp.text:
                            offer = self.parser.parse_offer_listing(olp_resp.text)
                            if offer and offer.get('price'):
                                result_data["current_price"] = offer['price']
                                result_data["buybox_price"] = offer['price']
                                if offer.get('is_fba'):
                                    result_data["is_fba"] = offer['is_fba']
                                if offer.get('shipping'):
                                    result_data["buybox_shipping"] = offer['shipping']
                                if offer.get('delivery'):
                                    result_data["delivery_date"] = offer['delivery']
                                result_data["stock_status"] = "In Stock (via offer-listing)"
                                # "See All Buying Options" 类页面：AOD 获取到价格但无库存信息，默认库存 5
                                if str(result_data.get("stock_count", "0")) in ("0", "", "N/A"):
                                    result_data["stock_count"] = "5"
                                logger.info(f"NFO {asin} OLP: {offer['price']} ship={offer.get('shipping','?')} {offer.get('is_fba','?')} del={offer.get('delivery','?')}")
                    except Exception as e:
                        logger.debug(f"NFO {asin} offer-listing 请求失败: {e}")

                # 成功
                self._controller.record_result(req_elapsed, True, False, resp_bytes)
                await self._submit_result(task_id, result_data, success=True, batch_id=task.get("batch_id"), lease_epoch=lease_epoch)
                self._stats["success"] += 1
                self._stats["total"] += 1

                title_short = result_data["title"][:40] if result_data["title"] else "N/A"
                logger.info(f"OK {asin} | {title_short}... | {result_data['current_price']}")
                # 链路计时日志（仅采样 20% 避免日志过多）
                if self._stats["total"] % 5 == 0:
                    logger.info(f"⏱️ 链路 | token:{t_token_wait:.2f}s sem:{t_sem_wait:.2f}s http:{req_elapsed:.2f}s parse:{t_parse:.3f}s bytes:{resp_bytes}")

                # 截图存证：写 HTML 到磁盘，由独立截图子进程渲染
                if task.get("needs_screenshot") and self._enable_screenshot:
                    await self._enqueue_screenshot_html(
                        asin=asin,
                        batch_name=task.get("batch_name", ""),
                        batch_id=task.get("batch_id"),
                        html_content=resp.text,
                    )

                # 主动轮换：每 N 次成功请求更换本 slot 的 session 防止被检测
                slot.note_success()
                if slot.should_rotate_proactive():
                    await slot.rotate(reason="主动轮换")

                return (True, False, resp_bytes)

            except Exception as e:
                attempt += 1
                err_name = type(e).__name__
                if "timeout" in err_name.lower() or "Timeout" in str(e):
                    last_error_type = "timeout"
                elif "connect" in err_name.lower() or "ConnectionError" in err_name:
                    last_error_type = "network"
                else:
                    last_error_type = "network"
                last_error_detail = f"{err_name}: {str(e)[:200]}"
                logger.error(f"ASIN {asin} 异常 (尝试 {attempt}/{max_retries}): {e}")
                await asyncio.sleep(2)

        # 所有重试用完，标记失败
        logger.error(f"ASIN {asin} 采集失败 (已重试 {max_retries} 次) [{last_error_type}]")
        await self._submit_result(task_id, None, success=False,
                                  error_type=last_error_type, error_detail=last_error_detail,
                                  batch_id=task.get("batch_id"), lease_epoch=lease_epoch)
        self._stats["failed"] += 1
        self._stats["total"] += 1
        return (False, False, resp_bytes)

    # ═══════════════════════════════════════════════
    # 卖家店铺发现任务（F-009：task_type='discover_seller'）
    # ═══════════════════════════════════════════════

    SELLER_MAX_PAGES = 75  # Amazon /s?me=... 列表上限约 75 页

    async def _process_seller_task(self, task: Dict, slot: "SessionSlot"):
        """处理 discover_seller 类型任务：翻页扫描卖家所有商品并提交 ASIN 列表。

        - 复用本协程私有的 SessionSlot、token bucket、信号量、CAPTCHA 自解 等基础设施
        - 失败用现有 _submit_result(success=False) 触发重试 / 终态失败
        - 成功用 _submit_seller_result 走专用端点，触发 server 端
          accept_seller_discovery_result：写发现表 + 按需衍生详情任务
        """
        from worker.parser import parse_seller_listing
        seller_id = (task["asin"] or "").strip().upper()
        task_id = task["id"]
        lease_epoch = task.get("lease_epoch", 0)
        batch_id = task.get("batch_id")
        max_retries = self._max_retries

        all_items: List[Dict] = []
        seen_asins = set()
        pages_scanned = 0
        truncated = False
        last_error_type = "network"
        last_error_detail = ""

        page = 1
        attempt = 0
        while page <= self.SELLER_MAX_PAGES:
            try:
                # 本协程私有 slot：卖家翻页复用同一个 session（无需切邮编）
                if not await slot.ensure_ready():
                    attempt += 1
                    if attempt >= max_retries:
                        last_error_type = "session_not_ready"
                        break
                    await asyncio.sleep(2)
                    continue
                session = slot.session

                if self._rate_limiter:
                    await self._rate_limiter.acquire()
                await self._controller.acquire()
                await self._apply_jitter()
                recv_speed = self._calc_recv_speed()
                req_start = time.time()
                try:
                    resp = await session.fetch_seller_listing_page(
                        seller_id, page=page, max_recv_speed=recv_speed
                    )
                    resp_bytes = len(resp.content) if resp and hasattr(resp, 'content') else 0
                finally:
                    req_elapsed = time.time() - req_start
                    self._controller.release()

                if resp is None:
                    self._controller.record_result(req_elapsed, False, False, 0)
                    attempt += 1
                    if attempt >= max_retries:
                        last_error_type = "timeout"
                        last_error_detail = f"page={page} 多次超时"
                        break
                    await asyncio.sleep(2)
                    continue

                if session.is_blocked(resp):
                    if session.is_captcha(resp):
                        if await session.solve_captcha(resp):
                            continue
                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = f"page={page} HTTP {resp.status_code}"
                    await slot.rotate(reason="卖家列表被封")
                    if attempt >= max_retries:
                        break
                    continue

                if session.is_404(resp):
                    # 404 视为该卖家没有更多页面（边界）
                    last_error_type = ""
                    break

                parsed = parse_seller_listing(resp.text)
                page_info = parsed.get("page_info", "")
                if page_info in ("captcha", "blocked"):
                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = f"page={page} parse:{page_info}"
                    await slot.rotate(reason=f"列表页 {page_info}")
                    if attempt >= max_retries:
                        break
                    continue

                self._controller.record_result(req_elapsed, True, False, resp_bytes)
                pages_scanned += 1

                items = parsed.get("items", [])
                added = 0
                for it in items:
                    a = it.get("asin")
                    if a and a not in seen_asins:
                        seen_asins.add(a)
                        all_items.append(it)
                        added += 1
                logger.info(f"seller={seller_id} page={page} 抓到 {added} 个新 ASIN，累计 {len(seen_asins)}")

                if not parsed.get("has_next") or not items:
                    break

                page += 1
                attempt = 0  # 翻页成功后重置该页重试计数

                # 主动轮换：每 N 页换一次本 slot 的 session
                slot.note_success()
                if slot.should_rotate_proactive():
                    await slot.rotate(reason="卖家翻页主动轮换")

            except Exception as e:
                attempt += 1
                err_name = type(e).__name__
                last_error_type = "timeout" if "timeout" in err_name.lower() else "network"
                last_error_detail = f"page={page} {err_name}: {str(e)[:200]}"
                logger.error(f"seller={seller_id} page={page} 异常 (尝试 {attempt}/{max_retries}): {e}")
                if attempt >= max_retries:
                    break
                await asyncio.sleep(2)

        if page > self.SELLER_MAX_PAGES:
            truncated = True

        # 没有任何成功页 → 整个任务失败，走现有重试通道
        if pages_scanned == 0:
            logger.error(f"seller={seller_id} 全部失败，标记失败 [{last_error_type}]")
            await self._submit_result(
                task_id, None, success=False,
                error_type=last_error_type or "discover_failed",
                error_detail=last_error_detail or "no page scanned",
                batch_id=batch_id, lease_epoch=lease_epoch,
            )
            self._stats["failed"] += 1
            self._stats["total"] += 1
            return

        meta = {
            "pages_scanned": pages_scanned,
            "truncated": truncated,
        }
        ok = await self._submit_seller_result(
            task_id=task_id,
            seller_id=seller_id,
            items=all_items,
            meta=meta,
            lease_epoch=lease_epoch,
            batch_id=batch_id,
        )
        if ok:
            self._stats["success"] += 1
            logger.info(
                f"✅ seller={seller_id} 完成: pages={pages_scanned} asins={len(all_items)} truncated={truncated}"
            )
        else:
            self._stats["failed"] += 1
            logger.error(f"seller={seller_id} 提交失败")
        self._stats["total"] += 1

    async def _submit_seller_result(self, task_id: int, seller_id: str,
                                     items: List[Dict], meta: Dict,
                                     lease_epoch: int, batch_id: Optional[int]) -> bool:
        """直接 POST /api/tasks/seller-result（不走批量队列，每个 seller 任务一次提交）。"""
        url = f"{self.server_url}/api/tasks/seller-result"
        payload = {
            "task_id": task_id,
            "batch_id": batch_id,
            "worker_id": self.worker_id,
            "lease_epoch": lease_epoch,
            "seller_id": seller_id,
            "items": items,
            "meta": meta,
        }
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("accepted"):
                        self._stats["accepted"] += 1
                        return True
                    if data.get("stale"):
                        self._stats["stale"] += 1
                        logger.warning(f"seller_result stale: task_id={task_id}")
                        return False
                logger.warning(f"seller_result HTTP {resp.status_code} (尝试 {attempt+1}/3)")
            except Exception as e:
                logger.error(f"seller_result 异常 (尝试 {attempt+1}/3): {type(e).__name__}: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        return False

    # ═══════════════════════════════════════════════
    # 结果提交（保持不变）
    # ═══════════════════════════════════════════════

    async def _submit_result(self, task_id: int, result_data: Optional[Dict], success: bool,
                             error_type: str = None, error_detail: str = None,
                             batch_id: int = None, lease_epoch: int = 0):
        """将结果放入批量提交队列（v3: 扁平化 payload + lease_epoch）"""
        if success and result_data:
            # v3: 扁平化结果到顶层，server 直接从 item 读取 asin 等字段
            payload = dict(result_data)
            payload["task_id"] = task_id
            payload["batch_id"] = batch_id
            payload["worker_id"] = self.worker_id
            payload["lease_epoch"] = lease_epoch
        else:
            payload = {
                "task_id": task_id,
                "batch_id": batch_id,
                "worker_id": self.worker_id,
                "lease_epoch": lease_epoch,
                "success": False,
            }
            if error_type:
                payload["error_type"] = error_type
                payload["error_detail"] = (error_detail or "")[:500]
        await self._result_queue.put(payload)

    async def _batch_submitter(self):
        """后台协程：每攒够 batch_size 个或每 batch_interval 秒批量提交"""
        batch: List[Dict] = []
        while self._running or not self._result_queue.empty():
            try:
                # 等待第一条数据到来（最多等 batch_interval 秒）
                try:
                    item = await asyncio.wait_for(
                        self._result_queue.get(), timeout=self._batch_interval
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    # 超时且无数据 → 继续等
                    if batch:
                        await self._submit_batch(batch)
                        batch = []
                    continue

                # 拿到第一条后，在剩余窗口内继续攒数据
                deadline = asyncio.get_event_loop().time() + self._batch_interval
                while len(batch) < self._batch_size:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(
                            self._result_queue.get(), timeout=remaining
                        )
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break  # 窗口到期

                # 提交攒到的批次
                if batch:
                    await self._submit_batch(batch)
                    batch = []

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"批量提交协程异常: {e}")
                await asyncio.sleep(1)

        # 退出前刷新剩余
        if batch:
            await self._submit_batch(batch)

    async def _flush_results(self):
        """刷新队列中所有剩余结果"""
        batch: List[Dict] = []
        while not self._result_queue.empty():
            batch.append(self._result_queue.get_nowait())
        if batch:
            await self._submit_batch(batch)

    async def _submit_batch(self, batch: List[Dict], retry: int = 3):
        """批量 POST 提交结果到服务器（含重试 + stale 统计）

        v3 优化（针对 result_queue 堆积问题）：
        - timeout 15s → 8s（server p99=94ms，8s 足够覆盖正常波动）
        - backoff 2^attempt → 0.5*2^attempt（1s/2s/4s → 0.5s/1s/2s）
        - 最坏耗时 52s → 27.5s，减少单 submitter 卡住时长
        - 配合 self._submitter_count=3 多协程并行，单点卡住不阻塞队列消化
        """
        url = f"{self.server_url}/api/tasks/result/batch"
        for attempt in range(retry):
            try:
                async with httpx.AsyncClient(timeout=8, headers=self._server_headers()) as client:
                    resp = await client.post(url, json={"results": batch})
                if resp.status_code == 200:
                    data = resp.json()
                    accepted = data.get("accepted", 0)
                    stale = data.get("stale", 0)
                    self._stats["accepted"] += accepted
                    self._stats["stale"] += stale
                    if stale > 0:
                        logger.debug(f"📊 批量提交: {accepted} accepted, {stale} stale")
                    return
                logger.warning(f"批量提交失败 HTTP {resp.status_code} (尝试 {attempt+1}/{retry})")
            except Exception as e:
                logger.error(f"批量提交异常 (尝试 {attempt+1}/{retry}): {type(e).__name__}: {e}")
            if attempt < retry - 1:
                # 0.5s / 1s / 2s 线性指数退避（原 1s/2s/4s 太保守）
                await asyncio.sleep(0.5 * (2 ** attempt))
        # 全部重试失败，回退并发提交
        logger.error("批量提交多次失败，回退并发逐条提交")
        await self._submit_batch_fallback(batch)

    async def _submit_batch_fallback(self, batch: List[Dict]):
        """fallback：并发逐条提交（批量接口不可用时）

        原本串行提交 10 条 × 10s timeout = 最坏 100s（一直卡住 submitter）。
        改成 asyncio.gather 并发 10 条 → 最坏 ~10s，吞吐 10×。
        """
        url = f"{self.server_url}/api/tasks/result"

        async def _post_one(client: httpx.AsyncClient, payload: Dict):
            try:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("stale"):
                        self._stats["stale"] += 1
                    elif data.get("ok"):
                        self._stats["accepted"] += 1
                else:
                    logger.warning(f"逐条提交失败: task_id={payload.get('task_id')} HTTP {resp.status_code}")
            except Exception as e:
                logger.error(f"逐条提交异常: task_id={payload.get('task_id')} {type(e).__name__}: {e}")

        # 共用一个 client 连接池，并发执行所有提交
        async with httpx.AsyncClient(timeout=8, headers=self._server_headers()) as client:
            await asyncio.gather(
                *[_post_one(client, p) for p in batch],
                return_exceptions=True,
            )

    # ═══════════════════════════════════════════════
    # 截图：独立子进程架构
    # ═══════════════════════════════════════════════

    async def _ensure_screenshot_process(self):
        """确保截图子进程已启动（加锁防止并发创建多个）"""
        if self._screenshot_process and self._screenshot_process.returncode is None:
            return
        async with self._screenshot_lock:
            # 双重检查
            if self._screenshot_process and self._screenshot_process.returncode is None:
                return
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshot.py")
            await self._reap_screenshot_descendants("截图子进程重启前清理残留浏览器")
            env = os.environ.copy()
            env["SCREENSHOT_BASE_DIR"] = self._screenshot_base_dir
            if self._api_key:
                env["WORKER_API_KEY"] = self._api_key
            self._screenshot_process = await asyncio.create_subprocess_exec(
                sys.executable, script,
                self.server_url,
                str(self._browsers_count),
                str(self._pages_per_browser),
                env=env,
                start_new_session=True,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            # start_new_session=True 下子进程成为新 session/pgroup leader，pgid 应等于 pid
            try:
                real_pgid = os.getpgid(self._screenshot_process.pid)
                if real_pgid != self._screenshot_process.pid:
                    logger.warning(
                        f"📸 子进程 pgid({real_pgid}) != pid({self._screenshot_process.pid})，killpg 可能漏杀后代"
                    )
                self._screenshot_pgid = real_pgid
            except Exception:
                self._screenshot_pgid = self._screenshot_process.pid
        # 异步转发子进程日志
        self._screenshot_log_task = asyncio.create_task(
            self._forward_screenshot_logs(self._screenshot_process)
        )
        logger.info(
            f"📸 截图子进程已启动 (PID: {self._screenshot_process.pid}, dir: {self._screenshot_base_dir})"
        )

    async def _forward_screenshot_logs(self, proc: asyncio.subprocess.Process):
        """将截图子进程的 stdout 转发到主进程日志"""
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                logger.info(f"[SS] {line.decode().rstrip()}")
        except Exception:
            pass

    async def _reap_screenshot_descendants(self, reason: str):
        """清理截图子进程残留的浏览器后代进程（先 killpg，再兜底 kill PID）。"""
        pgid = self._screenshot_pgid
        pid = self._screenshot_process.pid if self._screenshot_process else None
        if not pgid and not pid:
            return
        try:
            if pgid:
                os.killpg(pgid, signal.SIGKILL)
                logger.warning(f"📸 已强制清理截图进程组残留 (pgid={pgid}) | {reason}")
        except ProcessLookupError:
            pass
        except Exception as e:
            logger.warning(f"📸 清理截图进程组失败 (pgid={pgid}): {e}")
        # 兜底：即便 killpg 失败或 pgid 已变，仍尝试 kill 主进程自身
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception:
                pass
        self._screenshot_pgid = None

    async def _screenshot_gate_monitor(self):
        """监控截图子进程的 _uploaded 标记，完成后开门放行"""
        while self._running:
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=2)
                break  # shutdown 触发
            except asyncio.TimeoutError:
                pass
            if not self._screenshot_pending_batches:
                continue

            # 子进程已退出 → 立即放行
            if self._screenshot_process and self._screenshot_process.returncode is not None:
                logger.warning(f"📸 截图子进程已退出 (code={self._screenshot_process.returncode})，清除门控")
                await self._reap_screenshot_descendants("截图子进程异常退出后清理残留")
                self._screenshot_process = None
                self._screenshot_pending_batches.clear()
                self._screenshot_batch_ids.clear()
                self._screenshot_gate.set()
                continue

            completed = set()
            for batch in self._screenshot_pending_batches:
                marker = os.path.join(self._screenshot_base_dir, f"_uploaded_{batch}")
                if os.path.exists(marker):
                    completed.add(batch)

            if completed:
                for batch in completed:
                    # 清理标记文件
                    marker = os.path.join(self._screenshot_base_dir, f"_uploaded_{batch}")
                    try:
                        os.remove(marker)
                    except OSError:
                        pass
                    self._screenshot_batch_ids.pop(batch, None)
                self._screenshot_pending_batches -= completed
                logger.info(f"📸 截图批次已上传: {completed}")
                if not self._screenshot_pending_batches:
                    self._screenshot_gate.set()

    async def _startup_watchdog(self):
        """启动健康看门狗：worker 启动后 _startup_grace_seconds 秒内必须看到
        至少一次成功的心跳同步或处理至少一个任务，否则视为僵死并触发自动重启。
        防止 execv 后陷入"Python 解释器活着但所有协程都不工作"的状态。"""
        deadline = time.time() + self._startup_grace_seconds
        while self._running and time.time() < deadline:
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=2)
                return  # shutdown 触发，正常退出
            except asyncio.TimeoutError:
                pass
            # 任意一项达成即视为健康：处理过任务 OR 完成首次心跳同步
            if self._stats.get("total", 0) > 0 or self._has_synced_once:
                logger.info("✅ 启动健康检查通过（首次心跳/任务已就位）")
                return
        if not self._running:
            return
        # 超过宽限期仍无活动迹象 → 僵死
        logger.error(
            f"❌ 启动后 {self._startup_grace_seconds}s 仍无心跳/任务活动，触发自动重启"
        )
        self._wants_restart = True
        await self.stop()

    async def _auto_restart_timer(self):
        """定时触发进程级自动重启（全新 Python 解释器 + 全新 Playwright）。
        主用途：防止长跑后 Playwright/Chromium 状态累积、内存膨胀、孤儿进程。
        ±10% 随机抖动避免多 worker 同时重启。
        """
        hours = float(self._auto_restart_hours or 0)
        if hours <= 0:
            return
        base_sec = hours * 3600
        jitter = random.uniform(-0.10, 0.10) * base_sec
        delay = max(60, base_sec + jitter)
        restart_at = time.time() + delay
        logger.info(
            f"⏰ 自动重启已开启：{hours:.2f}h 后重启 (实际延迟含抖动={delay:.0f}s, "
            f"预计时间戳={restart_at:.0f})"
        )
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        if not self._running:
            return
        logger.info("⏰ 到达自动重启时间，开始优雅关停并重启进程...")
        self._wants_restart = True
        # 走正常 stop 路径：会触发主 start() 内的清理链（session/截图子进程/结果刷盘）
        await self.stop()

    async def _soft_restart(self):
        """软重启：让各采集协程重建自己的 session（新指纹、新 cookies），不停止协程。

        会话已下沉到每协程私有的 SessionSlot，这里只 bump _restart_epoch：各 slot
        在下次 ensure_ready 时感知到 epoch 变化，主动关闭并重建自己的 session。
        """
        try:
            # 通知所有 SessionSlot 重建 session（epoch 递增即触发）
            self._restart_epoch += 1

            # 重置 AIMD 控制器
            old_c = self._controller._concurrency
            await self._controller._resize_semaphore(old_c, config.INITIAL_CONCURRENCY)
            self._controller._concurrency = config.INITIAL_CONCURRENCY
            self._controller._cooldown_until = 0

            # 重置统计（保留 start_time）
            self._stats = {
                "total": 0, "success": 0, "failed": 0, "blocked": 0,
                "accepted": 0, "stale": 0,
                "start_time": self._stats.get("start_time"),
            }

            logger.info(
                f"🔄 软重启完成: 各协程将重建 session (epoch={self._restart_epoch}), "
                f"并发重置为 {config.INITIAL_CONCURRENCY}"
            )
        except Exception as e:
            logger.error(f"🔄 软重启失败: {e}")

    async def _stop_screenshot_process(self):
        """停止截图子进程"""
        proc = self._screenshot_process
        pgid = self._screenshot_pgid
        if proc and proc.returncode is None:
            try:
                if pgid:
                    os.killpg(pgid, signal.SIGTERM)
                else:
                    proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                try:
                    if pgid:
                        os.killpg(pgid, signal.SIGKILL)
                    else:
                        proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except asyncio.TimeoutError:
                    logger.warning("📸 截图子进程强杀后仍未及时退出")
            logger.info("📸 截图子进程已停止")
        await self._reap_screenshot_descendants("停止截图子进程后兜底清理")
        self._screenshot_process = None
        if self._screenshot_log_task:
            try:
                await asyncio.wait_for(self._screenshot_log_task, timeout=1)
            except Exception:
                pass
            self._screenshot_log_task = None

    # ═══════════════════════════════════════════════
    # 隧道模式 IP 轮换监控
    # ═══════════════════════════════════════════════

    # ═══════════════════════════════════════════════
    # 生命周期
    # ═══════════════════════════════════════════════

    async def _cleanup(self):
        """清理资源"""
        # 停止自适应控制器
        await self._controller.stop()
        # 刷新批量提交队列中的剩余结果
        if self._result_queue:
            await self._flush_results()
        if self._batch_submitter_task:
            self._batch_submitter_task.cancel()
            try:
                await self._batch_submitter_task
            except asyncio.CancelledError:
                pass
        # 采集 session 由各协程私有 SessionSlot 在 _worker_loop 退出时各自关闭，此处无需处理
        # 停止截图子进程
        await self._stop_screenshot_process()

    def _print_stats(self):
        """打印统计信息"""
        elapsed = time.time() - self._stats["start_time"] if self._stats["start_time"] else 0
        total = self._stats["total"]
        success = self._stats["success"]
        rate = success / total * 100 if total > 0 else 0
        speed = total / elapsed * 60 if elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info(f"📊 Worker [{self.worker_id}] 统计")
        logger.info(f"   总采集: {total}")
        logger.info(f"   成功: {success} ({rate:.1f}%)")
        logger.info(f"   失败: {self._stats['failed']}")
        logger.info(f"   被封: {self._stats['blocked']}")
        logger.info(f"   速度: {speed:.1f} 条/分钟")
        logger.info(f"   耗时: {elapsed:.0f} 秒")
        logger.info(f"   最终并发: {self._controller.current_concurrency}")
        # 最终指标快照
        logger.info(self._metrics.format_summary())
        logger.info("=" * 60)


    async def _enqueue_screenshot_html(self, asin: str, batch_name: str,
                                       batch_id: Optional[int], html_content: str):
        """将截图 HTML 写入隔离缓存目录并确保截图子进程已启动。"""
        html_dir = os.path.join(self._screenshot_html_dir, batch_name)
        os.makedirs(html_dir, exist_ok=True)
        html_path = os.path.join(html_dir, f"{asin}.html")
        tmp_path = html_path + ".tmp"
        async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
            await f.write(html_content)
        os.replace(tmp_path, html_path)
        self._screenshot_pending_batches.add(batch_name)
        if batch_id:
            self._screenshot_batch_ids[batch_name] = batch_id
        await self._ensure_screenshot_process()

    def _build_missing_product_html(self, asin: str) -> str:
        """为 404/已下架商品生成可截图的占位页，避免截图任务永久 pending。"""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Amazon Product Not Found - {asin}</title>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif;
      background: linear-gradient(180deg, #f6f7f9 0%, #e9edf3 100%);
      color: #111827;
    }}
    .wrap {{
      width: 1280px;
      height: 1300px;
      box-sizing: border-box;
      padding: 72px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #d5d9d9;
      border-radius: 20px;
      padding: 56px;
      box-shadow: 0 24px 60px rgba(17, 24, 39, 0.08);
    }}
    .tag {{
      display: inline-block;
      padding: 8px 14px;
      border-radius: 999px;
      background: #fef3c7;
      color: #92400e;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: 0.02em;
      margin-bottom: 24px;
    }}
    h1 {{
      margin: 0 0 16px;
      font-size: 54px;
      line-height: 1.1;
    }}
    p {{
      margin: 0 0 18px;
      font-size: 28px;
      line-height: 1.55;
      color: #374151;
    }}
    .asin {{
      margin-top: 36px;
      padding-top: 28px;
      border-top: 1px solid #e5e7eb;
      font-size: 26px;
      color: #6b7280;
      letter-spacing: 0.06em;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="tag">404 / Unavailable</div>
      <h1>Product Not Found</h1>
      <p>This ASIN returned an Amazon 404 response during collection.</p>
      <p>The item may have been removed, merged, or is no longer available in the current catalog.</p>
      <p>This placeholder page is generated so screenshot tracking reaches a terminal state.</p>
      <div class="asin">ASIN: {asin}</div>
    </div>
  </div>
</body>
</html>"""


def main():
    """Worker 入口"""
    arg_parser = argparse.ArgumentParser(description="Amazon Scraper Worker (Pipeline + Adaptive)")
    arg_parser.add_argument("--server", required=True, help="中央服务器地址 (如 http://192.168.1.100:8899)")
    arg_parser.add_argument("--worker-id", default=None, help="Worker ID（默认自动生成）")
    arg_parser.add_argument("--concurrency", type=int, default=None,
                            help=f"最大并发数上限（默认 {config.MAX_CONCURRENCY}，自适应控制器自动探索最优值）")
    arg_parser.add_argument("--zip-code", default=None, help=f"邮编（默认 {config.DEFAULT_ZIP_CODE}）")
    arg_parser.add_argument("--no-screenshot", action="store_true", help="禁用截图功能（仅采集数据）")
    arg_parser.add_argument("--api-key", default=None, help="ERP Server Worker API Key（也可通过 WORKER_API_KEY 环境变量设置）")
    arg_parser.add_argument("--auto-restart-hours", type=float, default=None,
                            help="定时自动重启的小时数（0=关闭；也可用环境变量 WORKER_AUTO_RESTART_HOURS）")
    args = arg_parser.parse_args()

    # CLI 优先，回落到环境变量
    auto_restart_hours = args.auto_restart_hours
    if auto_restart_hours is None:
        try:
            auto_restart_hours = float(os.environ.get("WORKER_AUTO_RESTART_HOURS", "0") or 0)
        except ValueError:
            auto_restart_hours = 0

    worker = Worker(
        server_url=args.server,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        zip_code=args.zip_code,
        enable_screenshot=not args.no_screenshot,
        api_key=args.api_key,
        auto_restart_hours=auto_restart_hours,
    )

    # 优雅退出
    loop = asyncio.new_event_loop()

    def signal_handler(sig, frame):
        logger.info("⏹️ 收到停止信号，正在退出...")
        loop.create_task(worker.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        loop.run_until_complete(worker.start())
    finally:
        loop.close()

    # 如果 worker 是因为定时器触发的重启退出，则 os.execv 重启进程
    if getattr(worker, "_wants_restart", False):
        logger.info(f"🔁 自动重启：execv 重新加载 {sys.executable} {' '.join(sys.argv)}")
        # 短暂停顿，让日志落盘
        time.sleep(1)
        os.execv(sys.executable, [sys.executable, *sys.argv])


if __name__ == "__main__":
    main()
