"""单条写连接上的**并发**行为：黄金夹具结构性看不到的那一类。

为什么黄金抓不到：
  * ``tests/golden/harness.py`` 把 4 个后台协程全换成了 no-op；
  * ``TestClient`` 是严格顺序的，永远只有一个在飞的请求。
于是"两个协程同时用同一条连接"这件事在黄金里根本不会发生 —— 但真实服务里
``_callback_dispatcher`` / ``_completion_watcher`` / ``_timeout_task_loop``
就是和请求处理并发跑的。

D-2 让 ``_db`` 是**一条**专用写连接。aiosqlite 把操作排进该连接自己的工作
线程，所以并发使用是合法的；asyncpg 不排队，直接抛
``InterfaceError: cannot perform operation: another operation is in progress``。
``ConnProxy._op_lock``（common/pgdb/pool.py）就是补这个差的，本文件守住它。

仓库里确实存在"不持 _write_lock 就碰 _db"的合法路径，它们在 SQLite 下完全
正常、因此不能改（equivalence-first）：
    common/pgdb/batches.py  list_callback_due   —— 回调派发协程定时调用
    server/app.py:1298 / 2230 / 2281 / 2289 / 2294 / 2309 —— 裸 _db 读
"""
from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_unlocked_read_survives_concurrent_locked_writes(pgdb):
    """list_callback_due（不持锁）与持锁写路径并发 —— 两边都必须活着。

    回归目标：修复前这里稳定抛
    ``InterfaceError: cannot perform operation: another operation is in progress``。
    """
    bid = await pgdb.create_batch("cc_a", callback_url="http://x/cb",
                                  external_id="e1")
    await pgdb.create_tasks(bid, ["B0CONC0001", "B0CONC0002"])
    await pgdb.mark_batch_completed(bid)

    errors = []

    async def writer():
        try:
            for i in range(30):
                await pgdb.create_batch(f"cc_w{i}")
                await pgdb.prioritize_batch(bid, 5)
        except Exception as e:  # noqa: BLE001
            errors.append(f"writer: {type(e).__name__}: {e}")

    async def dispatcher():
        try:
            for _ in range(30):
                await pgdb.list_callback_due("2099-01-01 00:00:00", limit=50)
                await asyncio.sleep(0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"list_callback_due: {type(e).__name__}: {e}")

    await asyncio.gather(writer(), dispatcher())
    assert not errors, errors


@pytest.mark.asyncio
async def test_raw_db_reads_survive_concurrent_writes(pgdb):
    """app.py 那些**不持锁**的裸 ``db._db`` 读（1298 / 2230 / 2281 / 2289 …）。

    这里用 app.py 里逐字相同的语句形状，确认垫片能扛住并发。
    """
    bid = await pgdb.create_batch("cc_raw")
    await pgdb.create_tasks(bid, ["B0RAW00001", "B0RAW00002"])

    errors = []

    async def writer():
        try:
            for i in range(25):
                await pgdb.create_batch(f"cc_r{i}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"writer: {type(e).__name__}: {e}")

    async def raw_reader():
        try:
            for _ in range(25):
                # app.py:1298
                async with pgdb._db.execute(
                        "SELECT file_path FROM screenshots WHERE batch_id=? "
                        "AND file_path IS NOT NULL", (bid,)) as c:
                    await c.fetchall()
                # app.py:2289
                async with pgdb._db.execute(
                        "SELECT asin FROM batch_asins WHERE batch_id = ?", (bid,)) as c:
                    await c.fetchall()
                await asyncio.sleep(0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"raw_reader: {type(e).__name__}: {e}")

    await asyncio.gather(writer(), raw_reader())
    assert not errors, errors


@pytest.mark.asyncio
async def test_startup_optimize_concurrent_with_writes(pgdb):
    """run_startup_optimize 与写并发。

    app.py:177 是 ``asyncio.create_task(db.run_startup_optimize())``，
    会和 worker 的第一波 pull/submit 撞在一起。它自己吞异常，所以这里额外
    断言 ANALYZE **真的**跑到了（否则"没炸"只是因为被 except 吃掉了）。
    """
    await pgdb.create_batch("cc_opt")

    errors = []

    async def writer():
        try:
            for i in range(20):
                await pgdb.create_batch(f"cc_o{i}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"writer: {type(e).__name__}: {e}")

    await asyncio.gather(writer(), pgdb.run_startup_optimize())
    assert not errors, errors

    analyzed = await pgdb._write_conn.fetchval(
        "SELECT count(*) FROM pg_stat_user_tables WHERE last_analyze IS NOT NULL")
    assert analyzed > 0, "ANALYZE 没有真的执行（异常被 except 吞了？）"


@pytest.mark.asyncio
async def test_optimize_records_the_optimize_caller_key(pgdb):
    """``/api/_debug/lock-stats`` 的 caller key 必须与 SQLite 一致。

    SQLite 版是 ``_write_lock("optimize")``（common/database.py:378）。少了它，
    活着的 PG 服务在 lock-stats 里就比 SQLite 少一个 ``optimize`` key。
    黄金看不到（harness 把这个方法整个 no-op 掉了），只能靠这条用例守。
    """
    from common.pgdb._shared import LOCK_STATS

    before = len(LOCK_STATS["holds"].get("optimize", []))
    await pgdb.run_startup_optimize()
    after = len(LOCK_STATS["holds"].get("optimize", []))
    assert after > before, (
        "run_startup_optimize 没有在 _write_lock('optimize') 下执行；"
        f"LOCK_STATS holds keys={sorted(LOCK_STATS['holds'])}")


@pytest.mark.asyncio
async def test_tx_helper_rolls_back_on_cancel(pgdb):
    """``_tx()`` 必须对 CancelledError 也回滚。

    那条唯一的写连接一旦卡在事务里就是全局性的（catalog_sync_audit.md:130
    记的正是这个故障）。这里直接在事务中间取消，然后验证连接还能用、
    且改动没有落库。
    """
    await pgdb.create_batch("cc_tx_base")

    async def victim():
        async with pgdb._tx() as conn:
            await conn.execute(
                "INSERT INTO batches (name) VALUES (?) ON CONFLICT DO NOTHING",
                ("cc_tx_ghost",))
            await asyncio.sleep(3600)      # 在事务中间被取消

    task = asyncio.create_task(victim())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # 连接没被事务卡住，还能正常用
    assert await pgdb.create_batch("cc_tx_after") > 0
    # 被取消的事务确实回滚了
    row = await pgdb.get_batch_by_name("cc_tx_ghost")
    assert row is None, "取消后的事务没有回滚"
