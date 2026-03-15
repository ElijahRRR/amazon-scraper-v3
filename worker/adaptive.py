"""
Amazon 产品采集系统 v2 - 自适应并发控制器

双模式架构：
  - TPS 模式: 全局 AIMD（单信号量 + 单 Metrics）
  - DPS 隧道模式: Per-channel 独立 AIMD（每 channel 独立信号量 + Metrics）

算法：
  - Additive Increase: 一切顺利 → 并发 +1
  - Multiplicative Decrease: 出问题 → 并发 × 0.7/0.75
  - 带宽感知: 带宽饱和时停止增长
  - 冷却机制: 减速后进入冷却期，防止连续多次减速形成雪崩
"""
import asyncio
import random
import time
import logging
from typing import Optional, Dict

from common import config
from worker.metrics import MetricsCollector

logger = logging.getLogger(__name__)


# ============================================================
# 共享信号量 resize 工具函数
# ============================================================

async def resize_semaphore(sem: asyncio.Semaphore, old: int, new: int,
                           drain_task: Optional[asyncio.Task]) -> Optional[asyncio.Task]:
    """安全调整信号量大小，返回新的 drain task（或 None）。"""
    if drain_task and not drain_task.done():
        drain_task.cancel()
        try:
            await drain_task
        except (asyncio.CancelledError, Exception):
            pass
    diff = new - old
    if diff > 0:
        for _ in range(diff):
            sem.release()
        return None
    elif diff < 0:
        return asyncio.create_task(_drain_permits(sem, abs(diff)))
    return None


async def _drain_permits(sem: asyncio.Semaphore, count: int):
    """后台排空 permit（可取消安全）。"""
    drained = 0
    try:
        for _ in range(count):
            await sem.acquire()
            drained += 1
    except asyncio.CancelledError:
        for _ in range(drained):
            sem.release()


# ============================================================
# Per-Channel 控制器（DPS 隧道模式专用）
# ============================================================

class ChannelController:
    """
    单 channel 的并发控制器（DPS 隧道模式下每个 channel 一个）。

    拥有独立的信号量、metrics、冷却状态，互不影响。
    1 个 channel 被封只影响该 channel 的并发，不波及其他。
    """

    def __init__(self, channel_id: int, initial: int = None, min_c: int = None, max_c: int = None):
        # 从 config 读取 per-channel 并发参数（可通过 Settings 页面配置）
        _initial = initial if initial is not None else getattr(config, "PER_CHANNEL_INITIAL_CONCURRENCY", 2)
        _min = min_c if min_c is not None else getattr(config, "PER_CHANNEL_MIN_CONCURRENCY", 1)
        _max = max_c if max_c is not None else getattr(config, "PER_CHANNEL_MAX_CONCURRENCY", 4)
        self.channel_id = channel_id
        self._concurrency = max(_min, min(_max, _initial))
        self._min = _min
        self._max = _max
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._drain_task: Optional[asyncio.Task] = None
        self.metrics = MetricsCollector(window_seconds=20.0)
        self._cooldown_until: float = 0.0
        self._cooldown_duration = 8  # DPS 独享 IP，冷却短
        self._block_decrease_factor = 0.75

    @property
    def current_concurrency(self) -> int:
        return self._concurrency

    async def acquire(self):
        await self._semaphore.acquire()
        self.metrics.request_start()

    def release(self):
        self.metrics.request_end()
        self._semaphore.release()

    def record_result(self, latency_s: float, success: bool, blocked: bool, resp_bytes: int = 0):
        self.metrics.record(latency_s, success, blocked, resp_bytes)

    async def evaluate(self):
        """
        Gradient2-AIMD 混合评估（单 channel 版本）。

        优先级：
        1. 冷却期 → 不动
        2. 封锁率高 → AIMD 乘性降低 + 冷却
        3. 成功率低 → AIMD 乘性降低 + 短冷却
        4. RTT gradient 上升 → 预防性 -1（不需要冷却，持续跟踪）
        5. 一切正常 → +1
        """
        snap = self.metrics.snapshot()
        if snap["total"] < 3:
            return

        old_c = self._concurrency
        now = time.monotonic()

        if now < self._cooldown_until:
            return  # 冷却中

        # --- AIMD 硬惩罚（封锁/失败）---
        if snap["block_rate"] > config.BLOCK_RATE_THRESHOLD:
            new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
            self._cooldown_until = now + self._cooldown_duration
            reason = f"ch{self.channel_id} 封锁率={snap['block_rate']:.0%}"
        elif snap["success_rate"] < config.MIN_SUCCESS_RATE:
            new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
            self._cooldown_until = now + max(5, self._cooldown_duration // 2)
            reason = f"ch{self.channel_id} 成功率={snap['success_rate']:.0%}"

        # --- Gradient2 预防性降速（RTT 上升趋势）---
        # 仅在带宽饱和时才降速，且不低于初始值
        elif snap["rtt_gradient"] < 0.85 and snap["bandwidth_pct"] > 0.50:
            initial_c = getattr(config, "PER_CHANNEL_INITIAL_CONCURRENCY", 2)
            new_c = max(initial_c, self._concurrency - 1)
            reason = (f"ch{self.channel_id} RTT↑ gradient={snap['rtt_gradient']:.2f} bw={snap['bandwidth_pct']:.0%} "
                      f"(short={snap['ewma_short']:.2f}s long={snap['ewma_long']:.2f}s)")

        # --- 加性恢复 ---
        elif snap["success_rate"] >= config.TARGET_SUCCESS_RATE and snap["rtt_gradient"] >= 0.90:
            new_c = min(self._max, self._concurrency + 1)
            reason = f"ch{self.channel_id} OK gradient={snap['rtt_gradient']:.2f} +1"
        else:
            return  # 稳态

        if new_c != old_c:
            await self._resize(old_c, new_c)
            self._concurrency = new_c
            if new_c < old_c:
                logger.info(f"ch{self.channel_id} 并发 {old_c}->{new_c} | {reason}")

    async def _resize(self, old_val: int, new_val: int):
        self._drain_task = await resize_semaphore(
            self._semaphore, old_val, new_val, self._drain_task)

    async def stop(self):
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()


# ============================================================
# 主控制器
# ============================================================

class AdaptiveController:
    """
    自适应并发控制器

    双模式：
    - TPS: 全局单信号量 + 单 Metrics（原逻辑）
    - Tunnel: per-channel 控制器代理，acquire/release/record_result 按 channel_id 分发

    核心接口：
    - current_concurrency: 当前允许的最大并发数（tunnel = 各 channel 之和）
    - acquire(channel_id) / release(channel_id): 获取/释放并发槽位
    - record_result(channel_id, ...): 记录请求结果
    - start() / stop(): 启动/停止后台评估
    """

    def __init__(
        self,
        initial: int = None,
        min_c: int = None,
        max_c: int = None,
        metrics: MetricsCollector = None,
    ):
        self._min = min_c or config.MIN_CONCURRENCY
        self._max = max_c or config.MAX_CONCURRENCY
        self._proxy_mode = config.PROXY_MODE

        # --- Tunnel 模式：per-channel 控制 ---
        self._channel_controllers: Dict[int, ChannelController] = {}
        if self._proxy_mode == "tunnel":
            num_ch = config.TUNNEL_CHANNELS
            for ch_id in range(1, num_ch + 1):
                self._channel_controllers[ch_id] = ChannelController(
                    channel_id=ch_id,
                    # 不传参数，让 ChannelController 自己从 config 读取
                    # PER_CHANNEL_INITIAL_CONCURRENCY / MIN / MAX
                )
            # 全局 concurrency = 所有 channel 之和
            self._concurrency = sum(
                cc.current_concurrency for cc in self._channel_controllers.values()
            )
            # 全局 metrics 仍然保留（用于 Server 上报和看板显示）
            self.metrics = metrics or MetricsCollector()
            self._semaphore = None  # tunnel 模式不使用全局信号量
            self._drain_task = None
        else:
            # --- TPS 模式：全局单信号量 ---
            self._concurrency = initial or config.INITIAL_CONCURRENCY
            self._concurrency = max(self._min, min(self._max, self._concurrency))
            self._semaphore = asyncio.Semaphore(self._concurrency)
            self._drain_task: Optional[asyncio.Task] = None
            self.metrics = metrics or MetricsCollector()

        self._adjust_lock = asyncio.Lock()
        self._cooldown_until: float = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._adjust_interval = config.ADJUST_INTERVAL_S
        self._target_latency = config.TARGET_LATENCY_S
        self._max_latency = config.MAX_LATENCY_S
        self._target_success = config.TARGET_SUCCESS_RATE
        self._min_success = config.MIN_SUCCESS_RATE
        self._block_threshold = config.BLOCK_RATE_THRESHOLD
        self._bw_soft_cap = config.BANDWIDTH_SOFT_CAP

        if self._proxy_mode == "tunnel":
            self._cooldown_duration = 8
            self._block_decrease_factor = 0.75
        else:
            self._cooldown_duration = 15
            self._block_decrease_factor = 0.7

        self._recovery_jitter = 0.5

    @property
    def current_concurrency(self) -> int:
        if self._channel_controllers:
            return sum(cc.current_concurrency for cc in self._channel_controllers.values())
        return self._concurrency

    async def acquire(self, channel_id: int = None):
        """获取并发槽位。tunnel 模式需传 channel_id。"""
        if channel_id and channel_id in self._channel_controllers:
            await self._channel_controllers[channel_id].acquire()
            self.metrics.request_start()  # 全局 metrics 也记录
        elif self._semaphore:
            await self._semaphore.acquire()
            self.metrics.request_start()

    def release(self, channel_id: int = None):
        """释放并发槽位"""
        if channel_id and channel_id in self._channel_controllers:
            self._channel_controllers[channel_id].release()
            self.metrics.request_end()
        elif self._semaphore:
            self.metrics.request_end()
            self._semaphore.release()

    def record_result(self, latency_s: float, success: bool, blocked: bool,
                      resp_bytes: int = 0, channel_id: int = None):
        """记录请求结果。tunnel 模式同时记录到 channel 和全局。"""
        self.metrics.record(latency_s, success, blocked, resp_bytes)
        if channel_id and channel_id in self._channel_controllers:
            self._channel_controllers[channel_id].record_result(
                latency_s, success, blocked, resp_bytes
            )

    async def start(self):
        """启动后台评估协程"""
        self._running = True
        self._task = asyncio.create_task(self._adjust_loop())
        if self._channel_controllers:
            ch_sum = self.current_concurrency
            logger.info(
                f"自适应控制器启动 (per-channel) | "
                f"{len(self._channel_controllers)} channels | 总并发={ch_sum} | "
                f"范围=[{self._min}, {self._max}]"
            )
        else:
            logger.info(f"自适应控制器启动 | 初始并发={self._concurrency} | 范围=[{self._min}, {self._max}]")

    async def stop(self):
        """停止控制器"""
        self._running = False
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        for cc in self._channel_controllers.values():
            await cc.stop()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _adjust_loop(self):
        """后台循环：每 ADJUST_INTERVAL_S 秒评估一次"""
        while self._running:
            try:
                await asyncio.sleep(self._adjust_interval)
                if not self._running:
                    break
                if self._channel_controllers:
                    await self._evaluate_channels()
                else:
                    await self._evaluate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"自适应控制器异常: {e}")

    async def _evaluate_channels(self):
        """Tunnel 模式：每个 channel 独立评估"""
        for cc in self._channel_controllers.values():
            await cc.evaluate()
        # 更新全局 concurrency 统计
        self._concurrency = sum(
            cc.current_concurrency for cc in self._channel_controllers.values()
        )
        # 输出全局汇总
        logger.info(self.metrics.format_summary() + f" | 总并发={self._concurrency}")

    async def _evaluate(self):
        """
        TPS 模式评估逻辑 — Gradient2-AIMD 混合

        优先级：
        1. 冷却期 → 不动
        2. 被封率高 → AIMD 乘性降低 + 冷却
        3. 成功率低 → AIMD 乘性降低 + 短冷却
        4. 延迟高（p50 超标）→ 减速
        5. RTT gradient 上升 → 预防性 -1（Gradient2，不需要冷却）
        6. 带宽饱和 → 不动
        7. 一切正常且 gradient 稳定 → +2
        8. 其他 → 不动
        """
        snap = self.metrics.snapshot()

        if snap["total"] < 5:
            logger.debug(f"样本不足 ({snap['total']}), 跳过调整")
            return

        async with self._adjust_lock:
            old_c = self._concurrency
            now = time.monotonic()
            in_cooldown = now < self._cooldown_until
            reason = ""

            if in_cooldown:
                new_c = self._concurrency
                remaining = int(self._cooldown_until - now)
                reason = f"冷却中 (剩余 {remaining}s) -> 维持"

            elif snap["block_rate"] > self._block_threshold:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                self._cooldown_until = now + self._cooldown_duration
                reason = f"封锁率 {snap['block_rate']:.0%} -> ×{self._block_decrease_factor}+冷却{self._cooldown_duration}s"

            elif snap["success_rate"] < self._min_success:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                soft_cooldown = max(15, self._cooldown_duration // 2)
                self._cooldown_until = now + soft_cooldown
                reason = f"成功率={snap['success_rate']:.0%} -> ×{self._block_decrease_factor}+冷却{soft_cooldown}s"

            elif snap["latency_p50"] > self._max_latency:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                soft_cooldown = max(15, self._cooldown_duration // 2)
                self._cooldown_until = now + soft_cooldown
                reason = f"延迟 p50={snap['latency_p50']:.2f}s > {self._max_latency:.0f}s -> 减速"

            # Gradient2 预防性降速：RTT 上升趋势（short > long × 1.15）
            # 仅在带宽饱和（>50%）时才降速：带宽不饱和说明延迟来自远端，不是我们的过载
            # 且不低于初始并发（gradient 是预防性的，不应过度收缩）
            elif (snap["rtt_gradient"] < 0.85 and snap["ewma_short"] > 0
                  and snap["bandwidth_pct"] > 0.50):
                initial_c = config.TUNNEL_INITIAL_CONCURRENCY if self._proxy_mode == "tunnel" else config.INITIAL_CONCURRENCY
                new_c = max(initial_c, self._concurrency - 1)
                reason = (f"RTT↑ gradient={snap['rtt_gradient']:.2f} bw={snap['bandwidth_pct']:.0%} "
                          f"(short={snap['ewma_short']:.2f}s > long={snap['ewma_long']:.2f}s)")

            elif snap["bandwidth_pct"] > self._bw_soft_cap:
                new_c = self._concurrency
                reason = f"带宽 {snap['bandwidth_pct']:.0%} > {self._bw_soft_cap:.0%} -> 维持"

            elif (snap["success_rate"] >= self._target_success
                  and snap["latency_p50"] < self._target_latency
                  and snap["rtt_gradient"] >= 0.90):
                # gradient ≥ 0.90 即可扩容（缩小死区 [0.85, 0.90)，加速收敛）
                if random.random() < (0.3 + 0.7 * self._recovery_jitter):
                    increment = 2
                    new_c = min(self._max, self._concurrency + increment)
                    reason = f"OK gradient={snap['rtt_gradient']:.2f} p50={snap['latency_p50']:.2f}s -> +{increment}"
                else:
                    new_c = self._concurrency
                    reason = f"OK gradient={snap['rtt_gradient']:.2f} -> 维持(抖动跳过)"

            else:
                new_c = self._concurrency
                reason = f"稳态 | gradient={snap['rtt_gradient']:.2f} p50={snap['latency_p50']:.2f}s"

            if new_c != old_c:
                await self._resize_semaphore(old_c, new_c)
                self._concurrency = new_c
                logger.info(f"并发调整 {old_c} -> {new_c} | {reason}")
            else:
                logger.debug(f"{reason} | 并发={self._concurrency}")

        logger.info(self.metrics.format_summary())

    async def _resize_semaphore(self, old_value: int, new_value: int):
        """安全地调整信号量大小（仅 TPS 模式使用）"""
        if not self._semaphore:
            return
        self._drain_task = await resize_semaphore(
            self._semaphore, old_value, new_value, self._drain_task)


class TokenBucket:
    """
    令牌桶限流器

    控制请求发起速率（QPS），与 Semaphore（并发连接数）互补：
    - Semaphore 控制同时在飞的请求数
    - TokenBucket 控制新请求的产生速率
    """

    def __init__(self, rate: float = None, burst: int = None,
                 initial_tokens: float = 0.0):
        self._rate = rate or config.TOKEN_BUCKET_RATE
        self._burst = burst or 1  # 默认 burst=1，禁止积累，严格均匀间隔
        self._tokens = initial_tokens  # 默认 0：冷启动也遵循节拍间隔
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """获取一个令牌，不够时等待"""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait)

    def _refill(self):
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)

    @property
    def burst(self) -> int:
        return self._burst

    @burst.setter
    def burst(self, value: int):
        self._burst = max(1, value)

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, value: float):
        """动态调整速率（不改变 burst，burst 由调用方单独设置）"""
        self._rate = max(0.1, value)


class ChannelRateLimiter:
    """
    Per-channel 令牌桶管理器（DPS 隧道模式专用）

    每个 channel 独立限流，替代全局 TokenBucket。
    DPS 独享 IP，各 channel 的 Session/Cookie 独立，无需全局限速。
    每 channel 默认 3 QPS（可配置），总 QPS = channels × per_channel_qps。
    """

    def __init__(self, channels: int = None, per_channel_rate: float = None):
        self._channels = channels or config.TUNNEL_CHANNELS
        self._per_channel_rate = per_channel_rate or getattr(
            config, "PER_CHANNEL_QPS", 3.0
        )
        self._buckets: dict[int, TokenBucket] = {}
        base_interval = 1.0 / self._per_channel_rate
        for ch_id in range(1, self._channels + 1):
            # 相位错开：每个通道的令牌初始偏移 = (ch_index / N) * interval
            # 通过设置 initial_tokens 为相位对应的令牌量实现
            # ch1 最先拿到令牌，ch8 最后，均匀铺开
            phase_offset = ((ch_id - 1) / self._channels) * base_interval
            initial_tokens = phase_offset * self._per_channel_rate  # 0 ~ 1-1/N
            self._buckets[ch_id] = TokenBucket(
                rate=self._per_channel_rate,
                burst=self._calc_burst(self._per_channel_rate),
                initial_tokens=initial_tokens,
            )
        logger.info(
            f"Per-channel 限流器初始化: {self._channels} channels × "
            f"{self._per_channel_rate} QPS = {self._channels * self._per_channel_rate:.1f} 总 QPS"
            f" (相位错开: {base_interval/self._channels*1000:.0f}ms 间隔)"
        )

    async def acquire(self, channel_id: int = None):
        """获取指定 channel 的令牌，channel_id 为 None 时不限流"""
        if channel_id is None or channel_id not in self._buckets:
            return
        await self._buckets[channel_id].acquire()

    def _calc_burst(self, rate: float) -> int:
        return 1  # strict pacing，禁止 burst 积累

    def resize(self, channels: int):
        """运行时调整 channel 数量"""
        base_interval = 1.0 / self._per_channel_rate
        for ch_id in range(1, channels + 1):
            if ch_id not in self._buckets:
                phase_offset = ((ch_id - 1) / channels) * base_interval
                initial_tokens = phase_offset * self._per_channel_rate
                self._buckets[ch_id] = TokenBucket(
                    rate=self._per_channel_rate,
                    burst=self._calc_burst(self._per_channel_rate),
                    initial_tokens=initial_tokens,
                )
        to_remove = [k for k in self._buckets if k > channels]
        for k in to_remove:
            del self._buckets[k]
        self._channels = channels

    @property
    def per_channel_rate(self) -> float:
        return self._per_channel_rate

    @per_channel_rate.setter
    def per_channel_rate(self, value: float):
        """动态调整每 channel 的 QPS 和 burst"""
        self._per_channel_rate = max(0.5, value)
        new_burst = self._calc_burst(self._per_channel_rate)
        for bucket in self._buckets.values():
            bucket.rate = self._per_channel_rate
            bucket.burst = new_burst
