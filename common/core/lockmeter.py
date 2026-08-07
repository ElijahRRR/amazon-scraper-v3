"""common/core/lockmeter.py —— 写锁竞争 / 阶段耗时仪表（唯一真源）。

``LOCK_STATS`` 是**模块级全局容器**，``/api/_debug/lock-stats``
（server/api/debug.py）按对象读它。两个存储后端必须写进**同一个**容器，
否则那个端点在其中一个后端下永远返回空。定义放这里而不是
``common/database.py``：拿仪表不该顺带 import aiosqlite。
"""
import asyncio
import time
from collections import defaultdict
from typing import Any, Dict

# ============================================================
# 锁竞争 / 阶段耗时 侦查仪表（recon 阶段使用，可随时删除）
#
# 使用方式：
#   - async with self._write_lock:                  -> caller="other"
#   - async with self._write_lock("accept_results_batch"):  -> caller="accept_results_batch"
#
# 统计指标：每个 caller 的 acquire 等待时长 + 持锁时长
# 暴露接口：server/app.py 的 /api/_debug/lock-stats
# ============================================================

# 全局统计容器
LOCK_STATS: Dict[str, Any] = {
    "waits": defaultdict(list),       # caller -> [wait_ms, ...]
    "holds": defaultdict(list),       # caller -> [hold_ms, ...]
    "slow_holds": [],                 # [(ts, caller, hold_ms), ...]  仅 >200ms
    "stage_timings": defaultdict(list),  # stage_name -> [ms, ...]   内部分阶段
}

_MAX_SAMPLES = 10000   # 每个 caller 最多保留样本数（满了滚动）
_SLOW_HOLD_THRESHOLD_MS = 200


def _record_wait(caller: str, ms: float):
    arr = LOCK_STATS["waits"][caller]
    arr.append(ms)
    if len(arr) > _MAX_SAMPLES:
        del arr[: _MAX_SAMPLES // 2]


def _record_hold(caller: str, ms: float):
    arr = LOCK_STATS["holds"][caller]
    arr.append(ms)
    if len(arr) > _MAX_SAMPLES:
        del arr[: _MAX_SAMPLES // 2]
    if ms > _SLOW_HOLD_THRESHOLD_MS:
        sh = LOCK_STATS["slow_holds"]
        sh.append((time.time(), caller, round(ms, 2)))
        if len(sh) > 500:
            del sh[:250]


def record_stage(stage: str, ms: float):
    """供锁内分阶段计时用（如 accept_results_batch 内部）"""
    arr = LOCK_STATS["stage_timings"][stage]
    arr.append(ms)
    if len(arr) > _MAX_SAMPLES:
        del arr[: _MAX_SAMPLES // 2]


class _NamedLockCtx:
    """显式命名的 async context manager"""
    __slots__ = ("_parent", "_caller")

    def __init__(self, parent, caller: str):
        self._parent = parent
        self._caller = caller

    async def __aenter__(self):
        await self._parent._do_enter(self._caller)
        return self

    async def __aexit__(self, *args):
        await self._parent._do_exit()


class TimedLock:
    """asyncio.Lock 包装，自动按 caller 记录 wait/hold 时长。

    - `async with timed_lock:`          -> caller='other'（默认）
    - `async with timed_lock('name'):`  -> caller='name'（显式）
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._current_caller = "other"
        self._hold_t0 = 0.0

    # 显式命名：`async with self._write_lock("accept_results_batch"):`
    def __call__(self, caller: str = "other"):
        return _NamedLockCtx(self, caller)

    # 兼容旧调用：`async with self._write_lock:` -> caller='other'
    async def __aenter__(self):
        await self._do_enter("other")
        return self

    async def __aexit__(self, *args):
        await self._do_exit()

    async def _do_enter(self, caller: str):
        t0 = time.monotonic()
        await self._lock.acquire()
        wait_ms = (time.monotonic() - t0) * 1000
        _record_wait(caller, wait_ms)
        # 一旦拿到锁，此时无其他 coroutine 在临界区，写 self 不会 race
        self._current_caller = caller
        self._hold_t0 = time.monotonic()

    async def _do_exit(self):
        hold_ms = (time.monotonic() - self._hold_t0) * 1000
        _record_hold(self._current_caller, hold_ms)
        self._lock.release()


__all__ = [
    "LOCK_STATS",
    "TimedLock",
    "_NamedLockCtx",
    "_record_wait",
    "_record_hold",
    "record_stage",
]
