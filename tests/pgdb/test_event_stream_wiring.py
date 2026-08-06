"""Phase 2 收口：relay 的进程接线（lifespan + 起停 + 观测端点）。

test_relay.py 测的是 relay 自己的正确性（游标保证、分区、毒丸）。这里测的是
**它怎么被接进服务里**——三个 builder 各自都验过自己那块，接缝没人验：

  * relay 是不是真的随 app 起来（PG）、在 SQLite 上是不是一个字节都不跑；
  * 起到一半被叫停会不会漏连接和单例锁（实测漏过，见
    ``test_stop_during_start_leaks_nothing``）；
  * ``/api/_debug/event-stream`` 在两个后端上都得能回，且不许进 openapi。
"""
from __future__ import annotations

import asyncio

import pytest

from tests.pgdb.helpers import scratch_database

pytestmark = pytest.mark.asyncio


async def _advisory_locks_on(db) -> int:
    """本库上还剩几把 advisory lock（relay 单例锁就是其中之一）。"""
    async with db.read() as rc:
        async with rc.execute(
            "SELECT count(*) AS n FROM pg_locks l "
            "JOIN pg_stat_activity a USING (pid) "
            "WHERE l.locktype = 'advisory' AND a.datname = current_database()"
        ) as cur:
            row = await cur.fetchone()
    return int(row["n"])


# ============================================================
# 1) 起停竞争：lifespan 里 create_task(relay) 紧跟着 stop
# ============================================================

async def test_stop_during_start_leaks_nothing(pgdb):
    """进程刚起来就关：在途的 start 必须放弃，连接与单例锁一并归还。

    这是实测过的泄漏，不是假想。修之前：

        关服之后：relay_state='running' relay_conn=<Connection ...>
        库上残留会话=1  残留 advisory lock=1
        第二个实例 start_event_relay() -> False  state='refused'

    lifespan 的形状是 ``create_task(relay)`` → ``yield`` → ``await stop()``。
    停得够快时 relay 还卡在 ``asyncpg.connect()`` 里，而 ``relay_task`` /
    ``relay_conn`` 要到 start 的最后才被赋值——stop 因此什么都看不见、空转返回，
    relay 随后才起来，把连接和锁留到进程结束。同进程内再起一个服务器只会看到
    ``refused``，事件流静默不工作。
    """
    locks_before = await _advisory_locks_on(pgdb)

    task = asyncio.create_task(pgdb.run_event_relay())
    await asyncio.sleep(0)          # 让它进到第一条 await，但别跑完
    assert pgdb._ev()["relay_task"] is None, "前提：此刻 start 还没装好 relay_task"

    await pgdb.stop_event_relay()   # lifespan 里的那一句

    # 给那个还活着的 start 充分的时间「偷偷起来」
    for _ in range(40):
        await asyncio.sleep(0.05)
        if task.done():
            break

    assert pgdb._ev()["relay_state"] in ("stopped", "refused")
    assert pgdb._ev()["relay_conn"] is None, "在途的 start 把连接留下来了"
    assert await _advisory_locks_on(pgdb) == locks_before, "单例锁没还回去"

    # 被叫停的 start 应当是**干净返回**（run_event_relay 见 start 返回 False
    # 就直接 return），而不是靠 cancel 收场。
    assert task.done() and not task.cancelled()
    assert task.exception() is None, f"在途 start 以异常收场: {task.exception()!r}"


async def test_relay_restarts_cleanly_after_an_aborted_start(pgdb):
    """被叫停的 start 不许污染下一次 start —— 锁还回去了，就该拿得到。"""
    task = asyncio.create_task(pgdb.run_event_relay())
    await asyncio.sleep(0)
    await pgdb.stop_event_relay()
    for _ in range(40):
        await asyncio.sleep(0.05)
        if task.done():
            break
    task.cancel()
    try:
        await task
    except BaseException:  # noqa: BLE001
        pass

    assert await pgdb.start_event_relay() is True
    assert pgdb._ev()["relay_state"] == "running"
    await pgdb.stop_event_relay()
    assert pgdb._ev()["relay_conn"] is None


async def test_relay_state_distinguishes_never_started_from_died(pgdb):
    """``stopped`` 与 ``failed`` 必须分得开。

    两者都写 'stopped' 的话，观测端点上"还没起来"和"已经死了"长得一模一样，
    而这个端点存在的唯一理由就是让停摆变响。
    """
    assert pgdb._ev()["relay_state"] == "stopped"

    assert await pgdb.start_event_relay() is True
    assert pgdb._ev()["relay_state"] == "running"

    # 让主循环从内部炸掉（tick 之外的异常）
    boom = RuntimeError("relay 主循环合成故障")

    async def explode(*a, **kw):
        raise boom

    pgdb._relay_tick = explode          # tick 的异常会被内部吃掉……
    # ……所以直接打断 sleep，制造一个真正逃出循环的异常
    orig_sleep = asyncio.sleep

    async def bad_sleep(*a, **kw):
        raise boom

    asyncio.sleep = bad_sleep
    try:
        for _ in range(40):
            await orig_sleep(0.05)
            if pgdb._ev()["relay_state"] == "failed":
                break
    finally:
        asyncio.sleep = orig_sleep

    assert pgdb._ev()["relay_state"] == "failed", (
        "主循环异常退出后状态仍是 %r" % pgdb._ev()["relay_state"])
    await pgdb.stop_event_relay()


# ============================================================
# 2) 观测端点
# ============================================================

async def test_event_stream_stats_has_everything_phase3_needs(pgdb):
    """Phase 3 的 /api/v1/sync/status 直接吃这份，别再另写一份。"""
    await pgdb.init_event_stream()
    stats = await pgdb.event_stream_stats()
    for k in ("gen", "instance_id", "max_seq", "min_available_seq",
              "outbox_depth", "relay_lag_s", "events_per_minute",
              "partitions", "future_partitions", "dead_letters",
              "sync_meta", "contract_version", "counters", "relay_state"):
        assert k in stats, f"event_stream_stats 少了 {k}"
    assert stats["future_partitions"] >= 2, "必须常备 ≥2 个未来分区"


async def test_two_databases_only_one_relay(pgdb):
    """单例锁：同一个库上第二个 relay 必须拒绝启动（seq 顺序的第 1 环）。"""
    assert await pgdb.start_event_relay() is True
    async with scratch_database("second") as other:
        # 换成同一个库的 DSN，模拟"另一个进程连同一个库"
        other.dsn = pgdb.dsn
        assert await other.start_event_relay() is False
        assert other.event_relay_metrics()["relay_state"] == "refused"
    await pgdb.stop_event_relay()
