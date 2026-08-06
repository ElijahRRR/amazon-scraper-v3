"""tests/pgdb/test_admin.py —— admin.py（生命周期 / 维护）+ 管理与删除端点的
SQLite ↔ PostgreSQL 差分验证。

思路：**同一份场景代码**跑两遍 —— 一遍针对 ``common.database.Database``
（临时 .db 文件），一遍针对 ``common.pgdb.Database``（临时 PG 库），
逐条比对结果。场景全部只用两边都支持的调用面：

    db._db.execute(sql, params)   # ? 占位符；PG 侧由 pool.translate_sql 改写
    db.read()                     # async with ... as rc, rc.execute(...) as c
    db._write_lock                # async with

也就是 server/app.py 里那 7 个事务 / 11 条裸 SQL / 6 处 read() 用的同一套协议，
所以这些用例同时也是"垫片能不能顶住 app.py"的验收。

黄金 64 步完全没覆盖 ``DELETE /api/database``、``/api/batches/delete-bulk``、
``DELETE /api/results``、``POST /api/results/delete-by-file``、
``POST /api/batches/{name}/retry`` 与 legacy 版 ``/api/tasks/release``——
这里补上。

跑法：
    .venv/bin/python -m pytest tests/pgdb/test_admin.py -q
    .venv/bin/python -m tests.pgdb.test_admin        # 打印逐行差分表
"""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from typing import Any, List, Tuple

import pytest

Rec = List[Tuple[str, Any]]


# ============================================================
# 场景：两边逐字跑同一段代码
# ============================================================

def _is_pg(db) -> bool:
    return type(db).__module__.startswith("common.pgdb")


def _ioi(db, sql: str) -> str:
    """``INSERT OR IGNORE`` 的两种方言。

    垫片只翻译 ``?`` 与 ``LIKE``，**不翻译** ``INSERT OR IGNORE``（app.py 里
    一条都没有，全在 common/database.py 内部，各 mixin 自己写 PG 版 SQL）。
    这里显式分叉，和 mixin 作者要做的事一样。
    """
    if _is_pg(db):
        return sql.replace("INSERT OR IGNORE INTO", "INSERT INTO", 1) \
            + " ON CONFLICT DO NOTHING"
    return sql


async def _rows(db, sql, params=()):
    """通过只读入口取 [(dict), ...]，键序无关。"""
    async with db.read() as rc, rc.execute(sql, params) as c:
        return [dict(r) for r in await c.fetchall()]


async def _scalar(db, sql, params=()):
    async with db.read() as rc, rc.execute(sql, params) as c:
        row = await c.fetchone()
    return None if row is None else row[0]


async def _counts(db) -> dict:
    out = {}
    for t in ("batches", "tasks", "screenshots", "asin_changes",
              "batch_asins", "asin_data"):
        out[t] = await _scalar(db, f"SELECT COUNT(*) FROM {t}")
    return out


async def _seed(db):
    """种数据。故意全部走 ``self._db`` + ``?`` 占位符，两边共用一份 SQL。"""
    async with db._write_lock:
        await db._db.execute("BEGIN")
        for name in ("adm_a", "adm_b", "adm_c"):
            await db._db.execute(
                "INSERT INTO batches (name, needs_screenshot, status) VALUES (?, ?, ?)",
                (name, 1, "running"))
        # tasks：batch 1 三条（含两条 failed，error_type 一个在免重试名单里），
        #        batch 2 两条 processing，batch 3 一条 pending
        rows = [
            (1, "B0ADMIN001", "10001", "done", None, None),
            (1, "B0ADMIN002", "10001", "failed", "parse_error", "boom"),
            (1, "B0ADMIN003", "10001", "failed", "not_found", "gone"),
            (2, "B0ADMIN004", "60601", "processing", None, None),
            (2, "B0ADMIN005", "60601", "processing", None, None),
            (3, "B0ADMIN006", "90210", "pending", None, None),
        ]
        for bid, asin, zipc, st, et, ed in rows:
            await db._db.execute(
                "INSERT INTO tasks (batch_id, asin, zip_code, status, error_type,"
                " error_detail, worker_id, lease_epoch, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (bid, asin, zipc, st, et, ed, "w1", 1, "2026-08-04 10:00:00"))
        for bid, asin, path in ((1, "B0ADMIN001", "/ss/1.png"),
                                (1, "B0ADMIN002", None),
                                (2, "B0ADMIN004", "/ss/4.png")):
            await db._db.execute(
                "INSERT INTO screenshots (asin, batch_id, status, file_path,"
                " updated_at) VALUES (?, ?, ?, ?, ?)",
                (asin, bid, "done", path, "2026-08-04 10:00:00"))
        for bid, asin, is_new in ((1, "B0ADMIN001", 1), (1, "B0ADMIN002", 0),
                                  (2, "B0ADMIN004", 1)):
            await db._db.execute(
                "INSERT INTO batch_asins (batch_id, asin, is_new) VALUES (?, ?, ?)",
                (bid, asin, is_new))
        for asin, title, brand in (("B0ADMIN001", "GoldenBrand Widget", "GoldenBrand"),
                                   ("B0ADMIN002", "lower widget", "acme"),
                                   ("B0ADMIN004", "Другой", "ACME")):
            await db._db.execute(
                _ioi(db, "INSERT OR IGNORE INTO asin_data"
                         " (asin, title, brand, site, zip_code)"
                         " VALUES (?, ?, ?, ?, ?)"),
                (asin, title, brand, "US", "10001"))
        for asin, bid, ct in (("B0ADMIN001", 1, "new"), ("B0ADMIN002", 1, "price"),
                              ("B0ADMIN004", 2, "new")):
            await db._db.execute(
                "INSERT INTO asin_changes (asin, batch_id, change_type) VALUES (?, ?, ?)",
                (asin, bid, ct))
        await db._db.execute("COMMIT")


async def scenario(db) -> Rec:
    """返回 [(标签, 值)]；两个后端必须逐条相等。"""
    r: Rec = []

    # ---------- 0) 公开面形状 ----------
    import inspect
    r.append(("shape.start_maintenance_is_sync",
              not inspect.iscoroutinefunction(db.start_maintenance)))
    for m in ("connect", "close", "read", "_open_read_pool",
              "run_startup_optimize", "maintenance_loop", "wal_checkpoint"):
        r.append((f"shape.has.{m}", hasattr(db, m)))
    r.append(("shape.read_pool_size_is_int", isinstance(db._read_pool_size, int)))

    # ---------- 1) 四个维护方法 ----------
    r.append(("admin.open_read_pool", await db._open_read_pool()))
    r.append(("admin.startup_optimize", await db.run_startup_optimize()))
    # 再跑一次：必须幂等、必须不抛
    r.append(("admin.startup_optimize_again", await db.run_startup_optimize()))

    # start_maintenance：同步、幂等、可取消
    r.append(("admin.start_maintenance_returns", db.start_maintenance(3600)))
    t1 = db._maintenance_task
    db.start_maintenance(3600)
    r.append(("admin.start_maintenance_idempotent", db._maintenance_task is t1))
    r.append(("admin.maintenance_task_alive", not t1.done()))

    # maintenance_loop 永不返回
    try:
        await asyncio.wait_for(db.maintenance_loop(3600), timeout=0.2)
        r.append(("admin.maintenance_loop_returns", "RETURNED"))
    except asyncio.TimeoutError:
        r.append(("admin.maintenance_loop_returns", "TIMEOUT(ok)"))

    # wal_checkpoint：两边的返回**都必须能过 app.py:307 的 `if res:` 守卫**。
    # SQLite 返回 (busy, log, checkpointed)，PG 恒 None —— 值本身不比对，
    # 只比对"调用点是否安全"。
    res = await db.wal_checkpoint("PASSIVE")
    r.append(("admin.wal_checkpoint_is_none_or_3tuple",
              res is None or (isinstance(res, tuple) and len(res) == 3)))
    if res:                                   # app.py:307-310 的写法
        _ = (res[0], res[1], res[2])
    r.append(("admin.wal_checkpoint_callsite_ok", True))

    # ---------- 2) 种数据 ----------
    await _seed(db)
    r.append(("seed.counts", tuple(sorted((await _counts(db)).items()))))
    r.append(("seed.batch_ids", tuple(x["id"] for x in
                                      await _rows(db, "SELECT id FROM batches ORDER BY id"))))
    r.append(("seed.task_ids", tuple(x["id"] for x in
                                     await _rows(db, "SELECT id FROM tasks ORDER BY id"))))

    # ---------- 3) app.py:337 —— read() + 位置下标 ----------
    async with db.read() as rc, rc.execute(
        "SELECT id FROM batches WHERE status='running' ORDER BY updated_at DESC LIMIT 30"
    ) as c:
        r.append(("read.running_ids", tuple(sorted(row[0] for row in await c.fetchall()))))

    # ---------- 4) app.py:442 —— read() + 具名列 ----------
    async with db.read() as rc, rc.execute(
        "SELECT id, name, external_id, callback_url, callback_attempts,"
        " callback_status, completed_at, status FROM batches WHERE id=?", (1,)
    ) as c:
        row = await c.fetchone()
    r.append(("read.batch_row_by_name", (row["id"], row["name"], row["status"],
                                         row["callback_attempts"], row["external_id"])))
    r.append(("read.batch_row_by_pos", (row[0], row[1], row[7])))
    r.append(("read.batch_row_dict_keys", tuple(sorted(dict(row).keys()))))

    # ---------- 5) app.py:1112 —— async for + dict(r) ----------
    got = []
    async with db.read() as rc, rc.execute(
        "SELECT asin, status, file_path FROM screenshots WHERE batch_id = ?"
        " ORDER BY asin ASC LIMIT ? OFFSET ?", (1, 10, 0)
    ) as c:
        async for rr in c:
            got.append(tuple(sorted(dict(rr).items(), key=lambda kv: kv[0])))
    r.append(("read.async_for_dict", tuple(got)))

    # ---------- 6) app.py:1371-1385 —— 失败聚合 ----------
    async with db.read() as rc, rc.execute(
        "SELECT error_type, COUNT(*) as cnt FROM tasks WHERE batch_id=?"
        " AND status='failed' GROUP BY error_type ORDER BY cnt DESC", (1,)
    ) as c:
        r.append(("read.error_summary",
                  tuple(sorted((x["error_type"], x["cnt"]) for x in await c.fetchall()))))

    # ---------- 7) app.py:2266-2280 —— 模糊搜索选中待删 ASIN ----------
    # SQLite 的 LIKE 折 ASCII 大小写；PG 的 LIKE 不折。垫片把
    # `col LIKE ?` 改写成 `ascii_lower(col) LIKE ascii_lower(?)`（决策 D-5）。
    for term in ("golden", "GOLDEN", "GoldenBrand", "acme", "ACME", "widget"):
        pat = f"%{term}%"
        async with db._db.execute(
            "SELECT d.asin FROM asin_data d WHERE (d.asin LIKE ? OR d.title LIKE ?"
            " OR d.brand LIKE ?) ORDER BY d.asin", (pat, pat, pat)
        ) as c:
            r.append((f"like.{term}", tuple(x[0] for x in await c.fetchall())))

    # ---------- 8) app.py:1248-1277 —— 批次重试（锁外读 rowcount）----------
    no_retry = ("not_found", "gone_404")
    ph = ",".join("?" * len(no_retry))
    async with db._db.execute(
        f"SELECT COUNT(*) FROM tasks WHERE batch_id=? AND status='failed'"
        f" AND COALESCE(error_type,'') IN ({ph})", (1, *no_retry)
    ) as c:
        r.append(("retry.skipped_no_retry", (await c.fetchone())[0]))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        cursor = await db._db.execute(
            f"UPDATE tasks SET status='pending', retry_count=0, worker_id=NULL"
            f" WHERE batch_id=? AND status='failed'"
            f" AND COALESCE(error_type,'') NOT IN ({ph})", (1, *no_retry))
        await db._db.execute("COMMIT")
    r.append(("retry.rowcount_after_lock", cursor.rowcount))   # app.py:1510 的写法
    r.append(("retry.statuses", tuple(x["status"] for x in await _rows(
        db, "SELECT status FROM tasks WHERE batch_id=1 ORDER BY id"))))

    # ---------- 9) app.py:1497-1511 —— legacy release ----------
    ids = [4, 5, 999]
    ph2 = ",".join("?" * len(ids))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        cur2 = await db._db.execute(
            f"UPDATE tasks SET status='pending', worker_id=NULL,"
            f" lease_epoch=lease_epoch+1, updated_at=? WHERE id IN ({ph2})"
            f" AND status='processing'", ("2026-08-04 11:00:00", *ids))
        await db._db.execute("COMMIT")
    r.append(("release.rowcount", cur2.rowcount))
    r.append(("release.epochs", tuple((x["id"], x["status"], x["lease_epoch"])
                                      for x in await _rows(
        db, "SELECT id, status, lease_epoch FROM tasks ORDER BY id"))))

    # ---------- 10) app.py:1295-1307 —— 删除单个批次 ----------
    async with db._db.execute(
        "SELECT file_path FROM screenshots WHERE batch_id=? AND file_path IS NOT NULL", (1,)
    ) as c:
        r.append(("delbatch.files", tuple(sorted(x["file_path"] for x in await c.fetchall()))))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute("DELETE FROM tasks WHERE batch_id=?", (1,))
        await db._db.execute("DELETE FROM batch_asins WHERE batch_id=?", (1,))
        await db._db.execute("DELETE FROM screenshots WHERE batch_id=?", (1,))
        await db._db.execute("DELETE FROM asin_changes WHERE batch_id=?", (1,))
        await db._db.execute("DELETE FROM batches WHERE id=?", (1,))
        await db._db.execute("COMMIT")
    r.append(("delbatch.counts", tuple(sorted((await _counts(db)).items()))))

    # ---------- 11) app.py:1339-1351 —— 批量删除（含 ROLLBACK 路径）----------
    bulk = [2, 3]
    ph3 = ",".join("?" * len(bulk))
    async with db.read() as rc, rc.execute(
        f"SELECT file_path FROM screenshots WHERE batch_id IN ({ph3})"
        f" AND file_path IS NOT NULL", bulk
    ) as c:
        r.append(("delbulk.files", tuple(sorted(x["file_path"] for x in await c.fetchall()))))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        try:
            for tbl in ("tasks", "batch_asins", "screenshots", "asin_changes"):
                await db._db.execute(
                    f"DELETE FROM {tbl} WHERE batch_id IN ({ph3})", bulk)
            await db._db.execute(f"DELETE FROM batches WHERE id IN ({ph3})", bulk)
            await db._db.execute("COMMIT")
        except Exception:
            try:
                await db._db.execute("ROLLBACK")
            except Exception:
                pass
            raise
    r.append(("delbulk.counts", tuple(sorted((await _counts(db)).items()))))

    # 显式验一次 ROLLBACK：故意写坏 SQL，然后确认连接还能用
    rolled = "n/a"
    async with db._write_lock:
        await db._db.execute("BEGIN")
        try:
            await db._db.execute("INSERT INTO batches (name) VALUES (?)", ("adm_rollback",))
            await db._db.execute("SELECT * FROM no_such_table_here")
            await db._db.execute("COMMIT")
        except Exception:
            try:
                await db._db.execute("ROLLBACK")
            except Exception:
                pass
            rolled = "rolled_back"
    r.append(("rollback.happened", rolled))
    r.append(("rollback.no_leak", await _scalar(
        db, "SELECT COUNT(*) FROM batches WHERE name=?", ("adm_rollback",))))
    r.append(("rollback.conn_usable", await _scalar(db, "SELECT COUNT(*) FROM batches")))

    # ---------- 12) app.py:2227-2242 / 2306-2321 —— 按 ASIN 删除 ----------
    asins = ["B0ADMIN004", "B0NOPE"]
    ph4 = ",".join("?" * len(asins))
    async with db._db.execute(
        f"SELECT file_path FROM screenshots WHERE asin IN ({ph4})"
        f" AND file_path IS NOT NULL", asins
    ) as c:
        r.append(("delasin.files", tuple(x["file_path"] for x in await c.fetchall())))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        for tbl in ("asin_changes", "screenshots", "batch_asins", "asin_data"):
            await db._db.execute(f"DELETE FROM {tbl} WHERE asin IN ({ph4})", asins)
        await db._db.execute("COMMIT")
    r.append(("delasin.counts", tuple(sorted((await _counts(db)).items()))))

    # ---------- 13) app.py:2650-2655 —— DELETE /api/database ----------
    # 重新灌一点数据，确保清空是真的在干活
    await _seed(db)
    r.append(("clear.before", tuple(sorted((await _counts(db)).items()))))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        for table in ["asin_changes", "asin_data", "batch_asins", "tasks",
                      "screenshots", "batches"]:
            await db._db.execute(f"DELETE FROM {table}")
        await db._db.execute("DELETE FROM sqlite_sequence")
        await db._db.execute("COMMIT")
    r.append(("clear.after", tuple(sorted((await _counts(db)).items()))))

    # 清空后自增必须从 1 重新开始（sqlite_sequence 行被删 == identity RESTART）
    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute("INSERT INTO batches (name) VALUES (?)", ("after_clear",))
        await db._db.execute("COMMIT")
    r.append(("clear.next_batch_id", await _scalar(
        db, "SELECT id FROM batches WHERE name=?", ("after_clear",))))
    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute(
            "INSERT INTO tasks (batch_id, asin, zip_code, status) VALUES (?, ?, ?, ?)",
            (1, "B0AFTER01", "10001", "pending"))
        await db._db.execute("COMMIT")
    r.append(("clear.next_task_id", await _scalar(
        db, "SELECT id FROM tasks WHERE asin=?", ("B0AFTER01",))))

    # ---------- 14) 自增烧号（基线钉死的行为）----------
    async with db._write_lock:
        await db._db.execute("BEGIN")
        for name in ("burn_a", "burn_b", "burn_c"):
            await db._db.execute(
                _ioi(db, "INSERT OR IGNORE INTO batches (name) VALUES (?)"), (name,))
        for name in ("burn_a", "burn_b", "burn_c"):   # 全冲突，仍然烧号
            await db._db.execute(
                _ioi(db, "INSERT OR IGNORE INTO batches (name) VALUES (?)"), (name,))
        for name in ("burn_d", "burn_e"):
            await db._db.execute(
                _ioi(db, "INSERT OR IGNORE INTO batches (name) VALUES (?)"), (name,))
        await db._db.execute("COMMIT")
    r.append(("burn.ids", tuple(x["id"] for x in await _rows(
        db, "SELECT id FROM batches WHERE name LIKE 'burn_%' ORDER BY id"))))

    # ---------- 15) 时间戳与布尔的落库形状 ----------
    row = (await _rows(db, "SELECT created_at, updated_at, needs_screenshot"
                           " FROM batches WHERE name=?", ("after_clear",)))[0]
    import re as _re
    r.append(("types.created_at_is_str", isinstance(row["created_at"], str)))
    r.append(("types.created_at_format",
              bool(_re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
                                 row["created_at"] or ""))))
    r.append(("types.updated_at_is_str", isinstance(row["updated_at"], str)))
    r.append(("types.needs_screenshot_is_int",
              type(row["needs_screenshot"]) is int))
    r.append(("types.needs_screenshot_value", row["needs_screenshot"]))

    # ---------- 16) close 后再 close ----------
    return r


# ============================================================
# 驱动：两个后端各跑一遍
# ============================================================

async def run_sqlite() -> Rec:
    from common.database import Database as SqliteDatabase
    tmp = tempfile.mkdtemp(prefix="admin_sqlite_")
    db = SqliteDatabase(os.path.join(tmp, "data", "scraper.db"))
    try:
        await db.connect()
        return await scenario(db)
    finally:
        await db.close()
        await db.close()          # 二次 close 必须安全
        shutil.rmtree(tmp, ignore_errors=True)


async def run_postgres() -> Rec:
    from tests.pgdb.helpers import scratch_database
    async with scratch_database("admin") as db:
        rec = await scenario(db)
    return rec


def diff(a: Rec, b: Rec) -> List[str]:
    problems = []
    ka = [k for k, _ in a]
    kb = [k for k, _ in b]
    if ka != kb:
        problems.append(f"步骤序列不一致: {set(ka) ^ set(kb)}")
    da, dbb = dict(a), dict(b)
    for k in ka:
        if k in dbb and da[k] != dbb[k]:
            problems.append(f"{k}: sqlite={da[k]!r}  pg={dbb[k]!r}")
    return problems


# ============================================================
# pytest 入口
# ============================================================

@pytest.mark.asyncio
async def test_admin_sqlite_vs_postgres_parity():
    try:
        pg = await run_postgres()
    except Exception as e:  # noqa: BLE001
        if "asyncpg" in type(e).__module__ or "Connect" in type(e).__name__:
            pytest.skip(f"PostgreSQL 不可达: {type(e).__name__}: {e}")
        raise
    sq = await run_sqlite()
    problems = diff(sq, pg)
    assert not problems, "SQLite ↔ PG 差异:\n  " + "\n  ".join(problems)


@pytest.mark.asyncio
async def test_admin_no_extra_lock_stats_callers():
    """run_startup_optimize / maintenance 不得往 LOCK_STATS 里塞新 caller key。

    基线 step 56 钉死 waits/holds 只有
    {accept_results_batch, other, pull_tasks}，多一个 ``optimize`` 就是
    "新增字段"差异。（SQLite 版会塞，PG 版刻意不塞 —— 见 admin.py 头注记 A。）
    """
    from common.pgdb._shared import LOCK_STATS
    try:
        from tests.pgdb.helpers import scratch_database
        cm = scratch_database("lockstats")
        db = await cm.__aenter__()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"PostgreSQL 不可达: {type(e).__name__}: {e}")
        return
    try:
        before = set(LOCK_STATS["waits"]) | set(LOCK_STATS["holds"])
        await db.run_startup_optimize()
        await db.wal_checkpoint("TRUNCATE")
        after = set(LOCK_STATS["waits"]) | set(LOCK_STATS["holds"])
        assert after - before == set(), f"多出的 caller key: {after - before}"
    finally:
        await cm.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_close_is_idempotent_and_cancels_maintenance():
    try:
        from tests.pgdb.helpers import scratch_name, create_scratch, drop_scratch
        from common.pgdb import Database
        name = scratch_name("adminclose")
        dsn = await create_scratch(name)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"PostgreSQL 不可达: {type(e).__name__}: {e}")
        return
    prev = os.environ.get("PG_DSN")
    os.environ["PG_DSN"] = dsn
    db = Database()
    try:
        await db.connect()
        db.start_maintenance(3600)
        task = db._maintenance_task
        assert task is not None and not task.done()
        await db.close()
        assert task.done(), "close() 必须取消维护协程"
        assert db._maintenance_task is None
        assert db._db is None, "close() 之后 _db 必须是 None（与 SQLite 版一致）"
        await db.close()          # 二次 close 必须安全
    finally:
        if prev is None:
            os.environ.pop("PG_DSN", None)
        else:
            os.environ["PG_DSN"] = prev
        await drop_scratch(name)


# ============================================================
# 脚本入口：打印差分表
# ============================================================

async def _main():
    pg = await run_postgres()
    sq = await run_sqlite()
    da, dbb = dict(sq), dict(pg)
    width = max(len(k) for k, _ in sq)
    print(f"{'STEP'.ljust(width)} | {'SQLITE':<46} | {'POSTGRES':<46} | OK")
    print("-" * (width + 105))
    bad = 0
    for k, _ in sq:
        va, vb = repr(da[k])[:46], repr(dbb.get(k, "<MISSING>"))[:46]
        ok = da[k] == dbb.get(k)
        bad += 0 if ok else 1
        print(f"{k.ljust(width)} | {va:<46} | {vb:<46} | {'✓' if ok else '✗'}")
    print("-" * (width + 105))
    print(f"{len(sq)} 步，差异 {bad} 处")
    return bad


if __name__ == "__main__":
    raise SystemExit(1 if asyncio.run(_main()) else 0)
