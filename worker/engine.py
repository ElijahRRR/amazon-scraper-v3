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

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class Worker:
    """流水线异步采集 Worker"""

    def __init__(self, server_url: str, worker_id: str = None, concurrency: int = None,
                 zip_code: str = None, enable_screenshot: bool = True):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self._enable_screenshot = enable_screenshot

        # 组件
        self.proxy_manager = get_proxy_manager()
        self.parser = _ParserClass()
        self._session: Optional[AmazonSession] = None
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

        # 任务队列（优先级队列：首次请求 priority=0 优先处理，重试请求 priority=1 低优先级）
        self._task_queue: asyncio.PriorityQueue = None
        self._task_seq = 0  # 单调递增序号，同优先级内 FIFO
        self._queue_size = getattr(config, "TASK_QUEUE_SIZE", 100)
        self._prefetch_threshold = getattr(config, "TASK_PREFETCH_THRESHOLD", 0.5)

        # 统计
        self._stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "start_time": None,
        }

        # 运行控制
        self._running = False

        # 批量提交队列
        self._result_queue: asyncio.Queue = None
        self._batch_submitter_task: Optional[asyncio.Task] = None
        self._batch_size = 10
        self._batch_interval = 2.0  # 秒

        # 实例级运行参数（不污染全局 config）
        self._max_retries = config.MAX_RETRIES

        # Session 轮换控制
        self._success_since_rotate = 0
        self._rotate_every = config.SESSION_ROTATE_EVERY
        self._rotate_lock = asyncio.Lock()
        self._last_rotate_time = 0.0  # 轮换防抖（monotonic）
        self._session_ready = asyncio.Event()  # Session 就绪信号
        self._rotation_epoch = 0              # 轮换计数（每次成功轮换 +1）
        self._rotation_grace_until = 0.0      # 轮换后宽限期（monotonic），期间不触发新轮换

        # 机制 2：空标题累计触发主动轮换
        self._empty_title_count = 0
        self._empty_title_rotate_threshold = 50

        # Hot Standby Session（TPS 模式专用：预热备用 session，消除轮换停摆）
        self._standby_session: Optional[AmazonSession] = None
        self._standby_ready = asyncio.Event()
        self._standby_warming = False

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

        # 设置同步
        self._settings_version = 0

        # 全局并发协调
        self._global_block_epoch = 0   # 已处理的全局封锁 epoch
        self._recovery_jitter = 0.5    # Server 分配的恢复抖动系数

    async def start(self):
        """启动 Worker（流水线架构）"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动（流水线模式）")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   初始并发: {self._controller.current_concurrency}")
        logger.info(f"   并发范围: [{config.MIN_CONCURRENCY}, {self._controller._max}]")
        logger.info(f"   邮编: {self.zip_code}")
        logger.info(f"   截图功能: {'开启' if self._enable_screenshot else '关闭'}")
        logger.info(f"   代理模式: TPS")

        self._running = True
        self._stats["start_time"] = time.time()

        # 初始化队列
        self._task_queue = asyncio.PriorityQueue(maxsize=self._queue_size)
        self._result_queue = asyncio.Queue()

        # 启动前先从 Server 拉取设置（代理地址、邮编等），远程 Worker 无需本地配置
        await self._pull_initial_settings()

        # 初始化 session（此时 proxy_url 已从 Server 同步）
        await self._init_session()

        # 启动自适应控制器
        await self._controller.start()

        # 启动核心协程
        try:
            coroutines = [
                self._task_feeder(),         # 1. 持续从 Server 拉任务
                self._worker_pool(),         # 2. 工人池：自适应并发
                self._batch_submitter(),     # 3. 批量回传结果
                self._screenshot_gate_monitor(),  # 4. 截图完成监控（检查子进程标记）
                self._settings_sync(),       # 5. 定期同步服务端设置
                self._standby_warmer(),      # 6. 热备 session 预热
            ]
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            pass

        await self._cleanup()
        logger.info(f"🛑 Worker [{self.worker_id}] 已停止")
        self._print_stats()

    async def stop(self):
        """停止 Worker"""
        self._running = False
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

                    if not scrape_complete_batches:
                        await asyncio.sleep(1)
                        continue

                    for batch in scrape_complete_batches:
                        marker = os.path.join(self._screenshot_html_dir, batch, "_scraping_done")
                        if not os.path.exists(marker):
                            os.makedirs(os.path.dirname(marker), exist_ok=True)
                            with open(marker, "w") as f:
                                f.write(str(time.time()))

                    incomplete_batches = self._screenshot_pending_batches - scrape_complete_batches
                    if incomplete_batches:
                        logger.info(
                            f"📸 批次仍在采集，暂不门控: {sorted(incomplete_batches)}"
                        )
                        await asyncio.sleep(1)
                        continue

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
                        logger.info("▶️ 截图批次已完成，继续拉取新任务")
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ 截图门控超时（5分钟），强制放行继续拉取任务")
                        self._screenshot_pending_batches.clear()
                        self._screenshot_batch_ids.clear()
                        self._screenshot_gate.set()
                    continue
                threshold = int(self._queue_size * self._prefetch_threshold)

                if queue_size < threshold:
                    # 拉取量 = 当前并发数的 2 倍（预取），但不超过队列剩余空间
                    fetch_count = min(
                        self._controller.current_concurrency * 2,
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
                            dropped_ids = []
                            kept_items = []
                            while not self._task_queue.empty():
                                try:
                                    item = self._task_queue.get_nowait()
                                    old_task = item[2] if isinstance(item, tuple) else item
                                    if old_task and isinstance(old_task, dict):
                                        if old_task.get("priority", 0) > 0:
                                            kept_items.append(item)
                                        else:
                                            dropped_ids.append(old_task["id"])
                                except asyncio.QueueEmpty:
                                    break
                            for item in kept_items:
                                await self._task_queue.put(item)
                            if dropped_ids:
                                logger.info(f"🚀 检测到优先采集任务，已清空队列中 {len(dropped_ids)} 个普通任务（保留 {len(kept_items)} 个优先任务）")
                                asyncio.create_task(self._release_tasks(dropped_ids))

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
        # 错开启动，分散请求
        initial_c = self._controller.current_concurrency
        if initial_c > 0:
            stagger = worker_idx * (1.0 / initial_c)
            stagger = min(stagger, 2.0)  # 最多错开 2 秒
            if stagger > 0:
                await asyncio.sleep(stagger)

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
                    await self._process_task(task)
                finally:
                    self._active_task_count = max(0, self._active_task_count - 1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker-{worker_idx} 未捕获异常: {type(e).__name__}: {e}")
                # 不提交 failed（避免需要手动重试），让任务留在 processing
                # 由 Server 端超时回收机制自动重置为 pending 重新分发
                await asyncio.sleep(1)

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
            async with httpx.AsyncClient(timeout=5) as client:
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

        except Exception as e:
            logger.warning(f"⚠️ 拉取初始设置异常（将使用本地配置）: {e}")

    async def _create_session_with_retry(self, max_attempts: int = 3,
                                         delay: float = 5) -> Optional[AmazonSession]:
        """创建并初始化 AmazonSession，失败时重试。成功返回 session，全部失败返回 None。"""
        for attempt in range(max_attempts):
            session = AmazonSession(self.proxy_manager, self.zip_code)
            if await session.initialize():
                return session
            logger.warning(f"⚠️ Session 初始化失败 (尝试 {attempt+1}/{max_attempts})")
            await session.close()
            if attempt < max_attempts - 1:
                await asyncio.sleep(delay)
        return None

    async def _init_session(self):
        """初始化 Amazon session（失败时重试，确保 _session_ready 最终被 set）"""
        logger.info("🔧 初始化 Amazon session...")
        self._session_ready.clear()
        self._session = await self._create_session_with_retry()
        self._success_since_rotate = 0
        if self._session:
            self._session_ready.set()
            return
        logger.error("❌ Session 初始化 3 次全部失败，Worker 将在处理任务时继续重试")
        self._session_ready.set()

    async def _rotate_session(self, reason: str = "主动轮换"):
        """
        轮换 session。

        优先使用热备 session（hot swap，<0.5s），不可用时回退到冷轮换。
        Burst 缓解：旧 session 延迟 5s 关闭（让 in-flight 请求自然完成），
        轮换后设 3s 宽限期（期间空标题/空页面不再触发额外轮换）。
        """
        async with self._rotate_lock:
            # 防抖：5秒内不重复轮换
            now = time.monotonic()
            if now - self._last_rotate_time < 5:
                logger.debug(f"🔄 跳过轮换（距上次不足5秒）")
                return
            logger.info(f"🔄 Session {reason}...")
            # 通知所有 worker：session 不可用，请等待
            self._session_ready.clear()
            old_session = self._session
            self._session = None

            # === 优先 Hot Swap：使用预热的备用 session ===
            if self._standby_session and self._standby_session.is_ready():
                self._session = self._standby_session
                self._standby_session = None
                self._standby_ready.clear()
                self._success_since_rotate = 0
                self._empty_title_count = 0
                self._last_rotate_time = time.monotonic()
                self._rotation_epoch += 1
                self._rotation_grace_until = time.monotonic() + 3.0
                self._session_ready.set()
                logger.info("🔄 Session 轮换成功（hot swap，瞬间切换）")
                # 旧 session 延迟关闭：让 in-flight 请求自然完成，避免 burst
                if old_session:
                    asyncio.create_task(self._delayed_close_session(old_session, delay=5))
                # 异步报告被封，让备用预热协程获取新代理
                await self.proxy_manager.report_blocked()
                return

            # === Fallback：冷轮换 ===
            logger.info("🔄 热备不可用，执行冷轮换...")
            # 冷轮换时旧 session 也延迟关闭
            if old_session:
                asyncio.create_task(self._delayed_close_session(old_session, delay=5))
            await self.proxy_manager.report_blocked()
            await asyncio.sleep(1)

            self._session = await self._create_session_with_retry(delay=3)
            self._success_since_rotate = 0
            self._empty_title_count = 0
            self._last_rotate_time = time.monotonic()
            self._rotation_epoch += 1
            self._rotation_grace_until = time.monotonic() + 3.0
            if self._session:
                logger.info("🔄 Session 冷轮换成功")
            else:
                logger.error("❌ Session 轮换 3 次全部失败")
            self._session_ready.set()

    async def _delayed_close_session(self, session: AmazonSession, delay: float = 5):
        """延迟关闭旧 session，让 in-flight 请求有时间完成"""
        try:
            await asyncio.sleep(delay)
            await session.close()
            logger.debug("🔄 旧 session 延迟关闭完成")
        except Exception as e:
            logger.debug(f"旧 session 延迟关闭异常（忽略）: {e}")

    async def _wait_for_rotation_before_retry(self, asin: str, reason: str = ""):
        """
        机制 1 + 机制 2 联合处理：空标题/空页面时等待 session 轮换后再重试。

        机制 1（独立触发）：直接请求 session 轮换，等待新 session 就绪后返回。
        机制 2（累计触发）：累计空标题计数，达到阈值时主动轮换。
        Burst 缓解：宽限期内不触发额外轮换，只等待当前轮换完成。
        """
        self._empty_title_count += 1
        current_epoch = self._rotation_epoch
        now = time.monotonic()

        # 宽限期内（轮换后 3s）：不触发新轮换，只等待新 session 就绪
        in_grace = now < self._rotation_grace_until
        if in_grace:
            logger.debug(f"ASIN {asin} {reason} 在宽限期内，等待新 session")
            await self._session_ready.wait()
            return

        # 机制 2：累计空标题达到阈值，优先触发
        if self._empty_title_count >= self._empty_title_rotate_threshold:
            logger.info(f"🔄 机制2: 空标题累计 {self._empty_title_count} 次，触发主动轮换")
            await self._rotate_session(reason=f"空标题累计 {self._empty_title_count} 次")
        else:
            # 机制 1：独立触发轮换（受 5s 防抖保护）
            await self._rotate_session(reason=f"{reason}")

        # 等待 epoch 变化（确认轮换完成），最多等 15s
        if self._rotation_epoch == current_epoch:
            for _ in range(30):
                await asyncio.sleep(0.5)
                if self._rotation_epoch != current_epoch or not self._running:
                    break

        # 确保新 session 就绪
        await self._session_ready.wait()

    async def _standby_warmer(self):
        """TPS 热备 Session 预热协程：后台维护一个已初始化的备用 session。"""
        logger.info("🔥 Hot Standby Session 预热协程启动")
        await self._session_ready.wait()
        await asyncio.sleep(3)

        while self._running:
            try:
                if self._standby_session is None or not self._standby_session.is_ready():
                    if not self._standby_warming:
                        self._standby_warming = True
                        self._standby_ready.clear()
                        standby = await self._create_session_with_retry(max_attempts=1, delay=0)
                        if standby:
                            old = self._standby_session
                            self._standby_session = standby
                            self._standby_ready.set()
                            if old:
                                await old.close()
                            logger.info("🔥 Hot Standby Session 就绪")
                        else:
                            logger.warning("⚠️ Standby Session 初始化失败，10s 后重试")
                        self._standby_warming = False

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Standby warmer 异常: {e}")
                self._standby_warming = False
                await asyncio.sleep(10)

        if self._standby_session:
            await self._standby_session.close()
            self._standby_session = None

    async def _pull_tasks(self, count: int = None):
        """
        从服务器拉取任务

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
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json().get("tasks", [])
            logger.warning(f"拉取任务失败: HTTP {resp.status_code}")
            return None  # 服务器错误，快速重试
        except Exception as e:
            logger.error(f"拉取任务异常: {e}")
            return None  # 网络异常，快速重试

    async def _release_tasks(self, task_ids: List[int]):
        """通知 Server 归还未处理的任务（优先采集切换时调用）"""
        try:
            url = f"{self.server_url}/api/tasks/release"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json={"task_ids": task_ids})
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
                await asyncio.sleep(30)
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
                    },
                }

                # 优先使用新的综合同步端点
                s = None
                async with httpx.AsyncClient(timeout=5) as client:
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
                    new_max_c = quota.get("concurrency")
                    if new_max_c and new_max_c != self._controller._max:
                        old_max = self._controller._max
                        self._controller._max = new_max_c
                        if self._controller._concurrency > new_max_c:
                            await self._controller._resize_semaphore(
                                self._controller._concurrency, new_max_c
                            )
                            self._controller._concurrency = new_max_c
                        logger.info(f"📊 配额: max_c {old_max}->{new_max_c}")

                    new_qps = quota.get("qps")
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
            async with httpx.AsyncClient(timeout=5) as client:
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

    async def _process_task(self, task: Dict) -> tuple:
        """
        处理单个采集任务

        返回: (success: bool, blocked: bool, resp_bytes: int)

        双模式分支：
        - TPS: 所有 worker 共享全局 self._session，被封时触发全局 _rotate_session
        - 隧道: 每次请求从 proxy_manager 分配通道，从 session_pool 取对应 session，
                被封时仅标记该通道，下次循环自动切到其他通道
        """
        asin = task["asin"]
        task_id = task["id"]
        zip_code = task.get("zip_code", self.zip_code)
        max_retries = self._max_retries
        resp_bytes = 0
        last_error_type = "network"
        last_error_detail = ""
        attempt = 0
        while attempt < max_retries:
            try:
                # === Session 获取 ===
                if not self._session_ready.is_set():
                    logger.debug(f"ASIN {asin} 等待 session 就绪...")
                    try:
                        await asyncio.wait_for(self._session_ready.wait(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning(f"ASIN {asin} 等待 session 超时 30s")
                        attempt += 1
                        continue
                if self._session is None or not self._session.is_ready():
                    attempt += 1
                    logger.warning(f"ASIN {asin} session 仍未就绪 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue
                session = self._session

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
                    await self._rotate_session(reason="被封锁")
                    await self._submit_result(
                        task_id, None, success=False,
                        error_type=last_error_type, error_detail=last_error_detail,
                        batch_id=task.get("batch_id")
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
                    await self._submit_result(task_id, result_data, success=True, batch_id=task.get("batch_id"))
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
                    await self._rotate_session(reason="页面拦截")
                    continue

                if title == "[API封锁]":
                    self._controller.record_result(req_elapsed, False, True, resp_bytes)
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = "api-services-support@amazon.com"
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await self._rotate_session(reason="页面拦截")
                    continue

                if title in ["[页面为空]", "[HTML解析失败]"]:
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = title
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await self._wait_for_rotation_before_retry(asin, reason=title)
                    continue

                # 标题为空视为软拦截（降级页面），触发 session 轮换后重试
                if not title or title == "N/A":
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = "标题为空"
                    logger.warning(f"ASIN {asin} 标题为空 (尝试 {attempt}/{max_retries})")
                    await self._wait_for_rotation_before_retry(asin, reason="标题为空")
                    continue

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
                        await self._rotate_session(reason="非美国区域数据")
                        continue

                # 核心字段缺失检测：有标题但价格/库存/品牌全为空 → 页面降级，重试
                # v3: "No Featured Offer" 和 "不可售" 是有效状态，不算降级
                _is_nfo = result_data.get("current_price") == "No Featured Offer"
                _is_unavail = result_data.get("current_price") == "不可售"
                _na = {"", "N/A", "N/a", "n/a", "None", None, "0"}
                _core_fields = ["current_price", "buybox_price", "stock_status", "brand"]
                _is_degraded = all(result_data.get(f) in _na for f in _core_fields) and not _is_nfo and not _is_unavail

                # 价格 N/A + 库存 999 = 页面部分解析但价格区块缺失，重试
                _price_na = result_data.get("current_price") in _na and result_data.get("buybox_price") in _na
                _stock_999 = str(result_data.get("stock_count", "")).strip() == "999"
                # v3: 有有效标题+品牌的页面不算降级（可能是变体选择页）
                _title = result_data.get("title", "")
                _brand = result_data.get("brand", "")
                _has_valid_info = (_title and _title not in _na and not _title.startswith("[")
                                   and _brand and _brand not in _na)
                _is_incomplete = _price_na and _stock_999 and not _is_nfo and not _is_unavail and not _has_valid_info

                if _is_degraded or _is_incomplete:
                    self._controller.record_result(req_elapsed, False, False, resp_bytes)
                    attempt += 1
                    reason = "核心字段全部缺失" if _is_degraded else "价格缺失+库存999"
                    last_error_type = "parse_error"
                    last_error_detail = f"解析不完整（{reason}）"
                    logger.warning(f"ASIN {asin} {reason}，疑似降级页面 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

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
                                logger.info(f"NFO {asin} OLP: {offer['price']} ship={offer.get('shipping','?')} {offer.get('is_fba','?')} del={offer.get('delivery','?')}")
                    except Exception as e:
                        logger.debug(f"NFO {asin} offer-listing 请求失败: {e}")

                # 成功
                self._controller.record_result(req_elapsed, True, False, resp_bytes)
                await self._submit_result(task_id, result_data, success=True, batch_id=task.get("batch_id"))
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

                # 主动轮换：每 N 次成功请求更换 session 防止被检测
                self._success_since_rotate += 1
                if self._success_since_rotate >= self._rotate_every:
                    await self._rotate_session(reason=f"主动轮换 (已完成 {self._success_since_rotate} 次)")

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
                                  error_type=last_error_type, error_detail=last_error_detail)
        self._stats["failed"] += 1
        self._stats["total"] += 1
        return (False, False, resp_bytes)

    # ═══════════════════════════════════════════════
    # 结果提交（保持不变）
    # ═══════════════════════════════════════════════

    async def _submit_result(self, task_id: int, result_data: Optional[Dict], success: bool,
                             error_type: str = None, error_detail: str = None,
                             batch_id: int = None):
        """将结果放入批量提交队列（v3: 扁平化 payload）"""
        if success and result_data:
            # v3: 扁平化结果到顶层，server 直接从 item 读取 asin 等字段
            payload = dict(result_data)
            payload["task_id"] = task_id
            payload["batch_id"] = batch_id
            payload["worker_id"] = self.worker_id
        else:
            payload = {
                "task_id": task_id,
                "batch_id": batch_id,
                "worker_id": self.worker_id,
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
        """批量 POST 提交结果到服务器（含重试）"""
        url = f"{self.server_url}/api/tasks/result/batch"
        for attempt in range(retry):
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(url, json={"results": batch})
                if resp.status_code == 200:
                    logger.debug(f"批量提交 {len(batch)} 条结果成功")
                    return
                logger.warning(f"批量提交失败 HTTP {resp.status_code} (尝试 {attempt+1}/{retry})")
            except Exception as e:
                logger.error(f"批量提交异常 (尝试 {attempt+1}/{retry}): {type(e).__name__}: {e}")
            if attempt < retry - 1:
                await asyncio.sleep(2 ** attempt)
        # 全部重试失败，回退逐条提交
        logger.error("批量提交多次失败，回退逐条提交")
        await self._submit_batch_fallback(batch)

    async def _submit_batch_fallback(self, batch: List[Dict]):
        """逐条提交 fallback（批量接口不可用时）"""
        url = f"{self.server_url}/api/tasks/result"
        async with httpx.AsyncClient(timeout=10) as client:
            for payload in batch:
                try:
                    resp = await client.post(url, json=payload)
                    if resp.status_code != 200:
                        logger.warning(f"逐条提交失败: task_id={payload.get('task_id')} HTTP {resp.status_code}")
                except Exception as e:
                    logger.error(f"逐条提交异常: task_id={payload.get('task_id')} {e}")

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
        """清理截图子进程残留的浏览器后代进程。"""
        pgid = self._screenshot_pgid
        if not pgid:
            return
        try:
            os.killpg(pgid, signal.SIGKILL)
            logger.warning(f"📸 已强制清理截图进程组残留 (pgid={pgid}) | {reason}")
        except ProcessLookupError:
            pass
        except Exception as e:
            logger.warning(f"📸 清理截图进程组失败 (pgid={pgid}): {e}")
        finally:
            self._screenshot_pgid = None

    async def _screenshot_gate_monitor(self):
        """监控截图子进程的 _uploaded 标记，完成后开门放行"""
        while self._running:
            await asyncio.sleep(2)
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

    async def _soft_restart(self):
        """软重启：重建 session（新指纹、新 cookies），不停止采集协程"""
        try:
            # 关闭当前 session
            if self._session:
                await self._session.close()
                self._session = None
            # 关闭备用 session
            if hasattr(self, '_standby_session') and self._standby_session:
                await self._standby_session.close()
                self._standby_session = None

            # 重建 session
            await self._init_session()

            # 重置 AIMD 控制器
            self._controller._concurrency = config.INITIAL_CONCURRENCY
            await self._controller._resize_semaphore(
                self._controller.current_concurrency, config.INITIAL_CONCURRENCY)
            self._controller._cooldown_until = 0

            # 重置统计
            self._stats = {"success": 0, "failed": 0, "total": 0}
            self._success_since_rotate = 0
            self._empty_title_count = 0
            self._rotation_epoch += 1

            logger.info(f"🔄 软重启完成: 新 session 已就绪, 并发重置为 {config.INITIAL_CONCURRENCY}")
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
        # 关闭 Session
        if self._session:
            await self._session.close()
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
    args = arg_parser.parse_args()

    worker = Worker(
        server_url=args.server,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        zip_code=args.zip_code,
        enable_screenshot=not args.no_screenshot,
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


if __name__ == "__main__":
    main()
