"""
Amazon 产品采集系统 v2 - 滑动窗口指标采集器
实时跟踪请求延迟、成功率、被封率、带宽使用等关键指标
供自适应并发控制器 (adaptive.py) 消费
"""
import asyncio
import time
from collections import deque
from dataclasses import dataclass

from common import config


@dataclass
class RequestRecord:
    """单次请求的指标记录"""
    timestamp: float          # 完成时间戳（monotonic）
    latency_s: float          # 请求耗时（秒）
    success: bool             # 是否成功
    blocked: bool             # 是否被封（403/503/验证码）
    resp_bytes: int           # 响应体大小（字节）


class MetricsCollector:
    """
    滑动窗口指标采集器 + EWMA RTT 追踪（Gradient2 风格）

    保留最近 window_seconds 秒内的请求记录，实时计算：
    - 请求延迟 p50 / p95
    - 成功率 (success / total)
    - 被封率 (blocked / total)
    - 带宽使用率 (bytes/s vs 上限)
    - 当前在飞请求数
    - EWMA RTT 短窗口 / 长窗口（用于 Gradient2 预防性降速）
    """

    def __init__(self, window_seconds: float = 30.0):
        self._window = window_seconds
        self._records: deque[RequestRecord] = deque()
        self._lock = asyncio.Lock()
        self._inflight = 0

        # EWMA RTT 追踪（Gradient2 风格双窗口）
        # 短窗口反映近期趋势，长窗口作为基线
        self._ewma_short: float = 0.0   # 短期 EMA（alpha=0.3，~3 样本半衰期）
        self._ewma_long: float = 0.0    # 长期 EMA（alpha=0.05，~20 样本半衰期）
        self._ewma_initialized = False

    def record(self, latency_s: float, success: bool, blocked: bool, resp_bytes: int = 0):
        """记录一次请求完成（同步，可在协程外调用）"""
        rec = RequestRecord(
            timestamp=time.monotonic(),
            latency_s=latency_s,
            success=success,
            blocked=blocked,
            resp_bytes=resp_bytes,
        )
        self._records.append(rec)
        self._prune_sync()

        # 更新 EWMA RTT（仅对成功请求，避免超时/封锁噪声污染基线）
        if success and latency_s > 0:
            if not self._ewma_initialized:
                self._ewma_short = latency_s
                self._ewma_long = latency_s
                self._ewma_initialized = True
            else:
                self._ewma_short = 0.3 * latency_s + 0.7 * self._ewma_short
                self._ewma_long = 0.05 * latency_s + 0.95 * self._ewma_long

    def request_start(self):
        """标记一个请求开始（在飞 +1）"""
        self._inflight += 1

    def request_end(self):
        """标记一个请求结束（在飞 -1）"""
        self._inflight = max(0, self._inflight - 1)

    @property
    def inflight(self) -> int:
        return self._inflight

    def _prune_sync(self):
        """清理过期记录（无锁，依赖 CPython deque 原子性）"""
        cutoff = time.monotonic() - self._window
        while self._records and self._records[0].timestamp < cutoff:
            self._records.popleft()

    def snapshot(self) -> dict:
        """
        获取当前窗口内的汇总指标

        返回:
            {
                "total": int,
                "success_rate": float,     # 0.0 ~ 1.0
                "block_rate": float,       # 0.0 ~ 1.0
                "latency_p50": float,      # 秒
                "latency_p95": float,      # 秒
                "bandwidth_bps": float,    # bytes/s
                "bandwidth_pct": float,    # 带宽使用率 0.0 ~ 1.0
                "inflight": int,
                "window_seconds": float,
            }
        """
        self._prune_sync()
        records = list(self._records)

        total = len(records)
        if total == 0:
            return {
                "total": 0,
                "success_rate": 1.0,
                "block_rate": 0.0,
                "latency_p50": 0.0,
                "latency_p95": 0.0,
                "bandwidth_bps": 0.0,
                "bandwidth_pct": 0.0,
                "inflight": self._inflight,
                "window_seconds": self._window,
                "ewma_short": 0.0,
                "ewma_long": 0.0,
                "rtt_gradient": 1.0,
            }

        successes = sum(1 for r in records if r.success)
        blocks = sum(1 for r in records if r.blocked)
        success_rate = successes / total
        block_rate = blocks / total

        latencies = sorted(r.latency_s for r in records)
        p50 = self._percentile(latencies, 0.50)
        p95 = self._percentile(latencies, 0.95)

        total_bytes = sum(r.resp_bytes for r in records)
        time_span = records[-1].timestamp - records[0].timestamp if total > 1 else self._window
        time_span = max(time_span, 1.0)
        bandwidth_bps = total_bytes / time_span

        # 带宽使用率：resp_bytes 是解压后大小，估算压缩后实际传输量
        # Amazon HTML gzip 实测压缩比约 15:1（代理后台 1.8Mbps vs 代码 27Mbps）
        compressed_bps = bandwidth_bps / 15.0
        bandwidth_limit = config.PROXY_BANDWIDTH_MBPS * 1_000_000 / 8  # Mbps → Bytes/s
        bandwidth_pct = (compressed_bps / bandwidth_limit) if bandwidth_limit > 0 else 0.0

        # Gradient2: RTT gradient = long / short（>1 表示延迟上升）
        if self._ewma_initialized and self._ewma_short > 0:
            rtt_gradient = self._ewma_long / self._ewma_short
        else:
            rtt_gradient = 1.0  # 无数据时视为稳定

        return {
            "total": total,
            "success_rate": success_rate,
            "block_rate": block_rate,
            "latency_p50": p50,
            "latency_p95": p95,
            "bandwidth_bps": bandwidth_bps,
            "bandwidth_pct": bandwidth_pct,
            "inflight": self._inflight,
            "window_seconds": self._window,
            "ewma_short": self._ewma_short,
            "ewma_long": self._ewma_long,
            "rtt_gradient": rtt_gradient,
        }

    @staticmethod
    def _percentile(sorted_data: list, pct: float) -> float:
        """线性插值百分位数计算"""
        if not sorted_data:
            return 0.0
        n = len(sorted_data)
        if n == 1:
            return sorted_data[0]
        # 线性插值：index = pct * (n-1)
        idx_f = pct * (n - 1)
        lo = int(idx_f)
        hi = min(lo + 1, n - 1)
        frac = idx_f - lo
        return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])

    def format_summary(self) -> str:
        """格式化输出，用于日志"""
        s = self.snapshot()
        bw_display = s["bandwidth_bps"] / 1024  # KB/s
        return (
            f"指标 | 在飞:{s['inflight']} | "
            f"成功率:{s['success_rate']:.0%} | "
            f"封锁率:{s['block_rate']:.0%} | "
            f"p50:{s['latency_p50']:.2f}s p95:{s['latency_p95']:.2f}s | "
            f"RTT:{s['ewma_short']:.2f}/{s['ewma_long']:.2f}s g={s['rtt_gradient']:.2f} | "
            f"带宽:{bw_display:.0f}KB/s ({s['bandwidth_pct']:.0%})"
        )
