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


# ======================================================================
# D-15：按事务归属路由（F1/F2）+ 写锁释放时回收被遗弃的事务（F3/F4）
#
# 这一组全部是黄金**结构上**盖不到的：夹具 no-op 掉 4 个后台协程，TestClient
# 又是严格顺序的，所以"外人的事务"这个前置状态在黄金里根本不存在。
# ======================================================================

def _nul_read(pgdb):
    """server/app.py:2281 的逐字形状 + 一个 NUL 字节。

    PG 侧服务端报 CharacterNotInRepertoireError；修复前这条只读语句会挤进
    别人开着的事务里，把那个事务一起 abort 掉。
    PG_STRIP_NUL 覆盖不到它 —— app.py 自己拼参数，不过 text_affinity。
    """
    pat = "%bad\x00term%"
    return pgdb._db.execute(
        "SELECT d.asin FROM asin_data d WHERE (d.asin LIKE ? OR d.title LIKE ? "
        "OR d.brand LIKE ?)", (pat, pat, pat))


@pytest.mark.asyncio
async def test_foreign_failing_read_does_not_abort_a_write_transaction(pgdb):
    """F1：不持锁的读**报错**了，也不能毁掉并发的持锁写事务。

    修复前：accept_results_batch(6) -> InFailedSQLTransactionError，
    6 条结果全丢、任务卡在 processing（一个只读的删除预览请求造成的）。
    """
    bid = await pgdb.create_batch("cc_f1")
    asins = [f"B0RT1{i:05d}" for i in range(6)]
    await pgdb.create_tasks(bid, asins)
    tasks = await pgdb.pull_tasks("W1", count=6)
    items = [{"task_id": t["id"], "worker_id": "W1", "lease_epoch": t["lease_epoch"],
              "batch_id": bid, "success": True,
              "data": {"asin": t["asin"], "title": "t", "brand": "b"}}
             for t in tasks]

    intruder_errors = []

    async def intruder():
        for _ in range(25):
            try:
                async with _nul_read(pgdb) as c:
                    await c.fetchall()
            except Exception as e:  # noqa: BLE001
                intruder_errors.append(type(e).__name__)
            await asyncio.sleep(0)

    res, _ = await asyncio.gather(pgdb.accept_results_batch(items), intruder())

    # 入侵者自己该失败还是失败（语句本身就是非法的）
    assert intruder_errors, "探针没触发到服务端错误，测试失去意义"
    # 但事务持有者必须毫发无伤
    assert res["accepted"] == 6, res
    # 而且失败**只属于**入侵者：不能出现"事务已 abort"这种传染性错误
    assert "InFailedSQLTransactionError" not in intruder_errors, intruder_errors


@pytest.mark.asyncio
async def test_transaction_owner_still_reads_its_own_uncommitted_writes(pgdb):
    """路由**只**改道外人的读。事务持有者自己的读必须仍然读到自己没提交的改动
    —— 十处在途读（pull_tasks 的认领、lease 门、MAX(priority) …）全靠它。
    """
    async with pgdb._write_lock:
        async with pgdb._tx() as conn:
            await conn.execute(
                "INSERT INTO batches (name) VALUES (?) ON CONFLICT DO NOTHING",
                ("cc_own_read",))
            async with conn.execute(
                    "SELECT name FROM batches WHERE name = ?",
                    ("cc_own_read",)) as c:
                row = await c.fetchone()
            assert row is not None, "事务持有者读不到自己的未提交写入 —— 路由改道错了"


@pytest.mark.asyncio
async def test_locking_read_is_never_routed_to_the_read_pool(pgdb):
    """``SELECT ... FOR UPDATE`` 改道读池 = 悄悄丢掉行锁。

    pull_tasks 的认领与 mark_callback_attempt 的租约都建立在那把行锁上，
    所以这里验证认领仍然真的在发生。
    """
    bid = await pgdb.create_batch("cc_lockread")
    await pgdb.create_tasks(bid, [f"B0LR{i:06d}" for i in range(5)])
    got = await pgdb.pull_tasks("W1", count=5)
    assert len(got) == 5, got
    # 认领过的任务不能被第二个 worker 再拿到
    again = await pgdb.pull_tasks("W2", count=5)
    assert again == [], again


@pytest.mark.asyncio
async def test_abandoned_transaction_does_not_wedge_the_write_path(pgdb):
    """F3：一个异常逃出 ``async with _write_lock`` 的裸 BEGIN 块，不能把整条
    写路径永久焊死。

    修复前：之后每一次 BEGIN 都撞上"嵌套 BEGIN：上一个事务还没结束"，而读还
    一直返回 200 —— 健康检查全绿，写全挂。
    """
    with pytest.raises(Exception):
        async with pgdb._write_lock:
            await pgdb._db.execute("BEGIN")
            # 这条语句报错，异常直接逃出 async with（没有 try/except ROLLBACK）
            await pgdb._db.execute(
                "UPDATE tasks SET status='pending' WHERE id IN (?)", ("not-an-int",))
            await pgdb._db.execute("COMMIT")

    # 垫片的事务槽必须已经被回收，服务端也不能留着 idle-in-transaction
    assert pgdb._db._tx is None, "被遗弃的事务没有回收 —— 写路径已经焊死"
    assert not pgdb._write_conn.is_in_transaction(), \
        "服务端还留着一个开着的事务（握着锁、钉着 xmin horizon）"

    # 后续的写全部照常
    assert await pgdb.create_batch("cc_after_wedge") > 0
    bid = await pgdb.create_batch("cc_after_wedge2")
    await pgdb.create_tasks(bid, ["B0WEDGE0001"])
    assert len(await pgdb.pull_tasks("W9", count=1)) == 1
