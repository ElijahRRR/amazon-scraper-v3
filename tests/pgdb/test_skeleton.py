"""骨架自检：契约层面的东西，不是业务逻辑。

分两类：
* 纯函数（SQL 翻译、类型强转、公开面）—— 不需要数据库，永远会跑。
* DDL / 垫片 —— 需要 PostgreSQL，conftest 连不上时整个目录会被跳过。
"""
from __future__ import annotations

import pytest

from common.pgdb import PUBLIC_API, Database
from common.pgdb.pool import (
    ascii_fold,
    as_int,
    qmark_to_numeric,
    rowcount_from_tag,
    text_affinity,
    translate_sql,
)
from common.pgdb.schema import EXPECTED_COLUMNS

# 仓库用 pytest-asyncio 的 strict 模式，async 用例必须逐个 @pytest.mark.asyncio。


# ---------------------------------------------------------------- 纯函数

def test_public_api_matches_sqlite_class():
    """pgdb 的公开面必须与 common.database.Database 完全一致——一个不多一个不少。"""
    import inspect

    from common.database import Database as SqliteDatabase

    sqlite_public = {
        n for n, _ in inspect.getmembers(SqliteDatabase)
        if not n.startswith("__") or n == "__init__"
    }
    # SQLite 侧那些不属于契约的内部名（TimedLock 的方法等）不在类上，无需排除
    missing = [n for n in PUBLIC_API if not hasattr(Database, n)]
    assert not missing, f"pgdb 缺方法: {missing}"
    absent_in_sqlite = [n for n in PUBLIC_API if n not in sqlite_public]
    assert not absent_in_sqlite, f"PUBLIC_API 里有 SQLite 版没有的名字: {absent_in_sqlite}"


def test_signatures_match_sqlite():
    """签名（形参名 + 默认值）逐个比对。调用方按关键字传参，改名就是破坏契约。"""
    import inspect

    from common.database import Database as SqliteDatabase

    def shape(fn):
        # 只比形参名 / 种类 / 默认值。注解不比：pgdb 用了
        # ``from __future__ import annotations``，注解是字符串，比了全是噪声。
        return [(p.name, p.kind, p.default)
                for p in inspect.signature(fn).parameters.values()]

    diffs = []
    for name in PUBLIC_API:
        a = getattr(SqliteDatabase, name, None)
        b = getattr(Database, name, None)
        if not callable(a) or not callable(b):
            continue
        try:
            sa, sb = shape(a), shape(b)
        except (TypeError, ValueError):
            continue
        if sa != sb:
            diffs.append(f"{name}:\n  sqlite {sa}\n  pgdb   {sb}")
    assert not diffs, "签名不一致:\n" + "\n".join(diffs)


def test_iter_results_is_async_generator():
    """调用方是 ``async for``，写成普通 coroutine 会在运行期才炸。"""
    import inspect

    assert inspect.isasyncgenfunction(Database.iter_results)


def test_start_maintenance_is_sync():
    """server/app.py:171 不 await 它。"""
    import inspect

    assert not inspect.iscoroutinefunction(Database.start_maintenance)


def test_shared_symbols_are_the_same_objects():
    """_shared 必须是**再导出**，不是副本。

    分叉一份 LOCK_STATS，/api/_debug/lock-stats 就永远返回空容器；
    分叉一份 ASIN_DATA_FIELDS，两个后端写入的列集会悄悄不同。
    """
    import common.database as sq
    from common.pgdb import _shared

    for name in _shared.__all__:
        assert getattr(_shared, name) is getattr(sq, name), f"{name} 不是同一个对象"


def test_qmark_translation_skips_literals_and_comments():
    assert qmark_to_numeric("SELECT ? , ?") == "SELECT $1 , $2"
    # 字符串字面量里的 ? 不能被替换
    assert qmark_to_numeric("SELECT 'a?b', ?") == "SELECT 'a?b', $1"
    assert qmark_to_numeric("SELECT 'it''s ?', ?") == "SELECT 'it''s ?', $1"
    assert qmark_to_numeric('SELECT "c?l", ?') == 'SELECT "c?l", $1'
    assert qmark_to_numeric("-- ?\nSELECT ?") == "-- ?\nSELECT $1"
    assert qmark_to_numeric("/* ? */ SELECT ?") == "/* ? */ SELECT $1"


def test_like_is_rewritten_to_ascii_fold():
    """SQLite 的 LIKE 只折叠 ASCII；ILIKE 折叠全 Unicode（实测 9 处不一致），
    所以统一改写成 ascii_lower。"""
    out = translate_sql("SELECT 1 FROM asin_data d WHERE d.title LIKE ?")
    assert out == ("SELECT 1 FROM asin_data d "
                   "WHERE ascii_lower(d.title) LIKE ascii_lower($1)")
    # 已经显式写好的形式不会被二次包裹
    src = "SELECT 1 WHERE ascii_lower(d.title) LIKE ascii_lower(?)"
    assert translate_sql(src).count("ascii_lower") == 2


def test_mixed_placeholder_styles_rejected():
    with pytest.raises(ValueError):
        translate_sql("SELECT ? WHERE x = $1")


def test_rowcount_from_tag():
    # lease 门靠它；返回字符串会让 'UPDATE 0' == 0 变成 False，静默放行过期结果
    assert rowcount_from_tag("UPDATE 3") == 3
    assert rowcount_from_tag("UPDATE 0") == 0
    assert rowcount_from_tag("INSERT 0 1") == 1
    assert rowcount_from_tag("DELETE 0") == 0


def test_text_affinity_matches_sqlite():
    """裸 str() 是错的：会得到 'True' / '0.30000000000000004' / '1e+21'。"""
    assert text_affinity(True) == "1"
    assert text_affinity(False) == "0"
    assert text_affinity(10) == "10"
    assert text_affinity(4.5) == "4.5"
    assert text_affinity(0.1 + 0.2) == "0.3"
    assert text_affinity(1e21) == "1.0e+21"
    assert text_affinity(10.0) == "10.0"
    assert text_affinity(None) is None
    assert text_affinity("x") == "x"


def test_ascii_fold_is_ascii_only():
    assert ascii_fold("GoldenBrand") == "goldenbrand"
    # É 不折叠（ASCII 部分照常折叠）——与 SQLite 的 LIKE 完全一致；
    # 实测 SQLite 不会把 'CAFÉ CREME' 匹配到 '%café%'，而 PG 的 ILIKE 会。
    assert ascii_fold("ÉCLAIR") == "Éclair"
    assert "É".lower() == "é"                   # 对照：Python 的 lower 会折叠


def test_as_int():
    assert as_int("7") == 7
    assert as_int(7) == 7
    assert as_int(None, 0) == 0
    assert as_int("nope", -1) == -1


# ---------------------------------------------------------------- 需要 PG

@pytest.mark.asyncio
async def test_ddl_creates_every_table_and_index(pgdb):
    conn = pgdb._write_conn
    tables = {r[0] for r in await conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname='public'")}
    assert set(EXPECTED_COLUMNS) <= tables

    indexes = {r[0] for r in await conn.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname='public'")}
    for expected in (
        "idx_batch_asins_asin", "idx_changes_asin", "idx_changes_type",
        "idx_changes_batch_type", "idx_tasks_status", "idx_tasks_batch",
        "idx_tasks_status_priority", "idx_tasks_status_worker",
        "idx_tasks_status_updated", "idx_tasks_batch_status",
        "idx_screenshots_status", "idx_screenshots_batch",
        "idx_seller_disc_seller", "idx_seller_disc_asin", "idx_seller_disc_batch",
        "idx_batches_callback_pending",
        "idx_asin_data_asin_trgm", "idx_asin_data_title_trgm",
        "idx_asin_data_brand_trgm",
    ):
        assert expected in indexes, f"缺索引 {expected}"


@pytest.mark.asyncio
async def test_init_tables_is_idempotent(pgdb):
    await pgdb.init_tables()
    await pgdb.init_tables()
    assert await pgdb.verify_schema(strict=False) == []


@pytest.mark.asyncio
async def test_timestamp_default_format_and_types(pgdb):
    """时间戳必须是 '%Y-%m-%d %H:%M:%S' 的 **str**，布尔必须是 int 0/1。

    server/app.py:1168 与 487 对 created_at 做 strptime(x[:19], ...)，
    datetime 对象不可下标 → TypeError → duration_seconds 静默变 null。
    """
    import re

    conn = pgdb._write_conn
    await conn.execute("INSERT INTO batches (name) VALUES ('ts-probe')")
    row = await conn.fetchrow(
        "SELECT created_at, updated_at, needs_screenshot FROM batches")
    assert isinstance(row["created_at"], str)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", row["created_at"])
    assert isinstance(row["needs_screenshot"], int)
    assert not isinstance(row["needs_screenshot"], bool)


@pytest.mark.asyncio
async def test_identity_burns_ids_like_sqlite_autoincrement(pgdb):
    """基线钉死了自增空洞：golden_batch_b 是 id 3 不是 2，task id 是 1,3,7,8。

    任何"先查再插 / WHERE NOT EXISTS"的预过滤都会改变烧号，进而挪动下游所有 id。
    """
    conn = pgdb._write_conn
    for names in (("a", "b", "c"), ("a", "b", "c"), ("d", "e")):
        for nm in names:
            await conn.execute(
                "INSERT INTO batches (name) VALUES ($1) ON CONFLICT DO NOTHING", nm)
    ids = [r[0] for r in await conn.fetch("SELECT id FROM batches ORDER BY id")]
    assert ids == [1, 2, 3, 7, 8]


@pytest.mark.asyncio
async def test_shim_cursor_protocol_and_rowcount(pgdb):
    """app.py 的 24 处裸 SQL 全靠这套形状。"""
    await pgdb._db.execute("BEGIN")
    cur = await pgdb._db.execute(
        "INSERT INTO batches (name) VALUES (?), (?)", ("s1", "s2"))
    assert cur.rowcount == 2
    cur = await pgdb._db.execute(
        "UPDATE batches SET status='x' WHERE name=?", ("s1",))
    assert cur.rowcount == 1
    await pgdb._db.execute("COMMIT")

    async with pgdb.read() as rc:
        async with rc.execute("SELECT id, name FROM batches ORDER BY id") as c:
            row = await c.fetchone()
            assert row[0] == 1 and row["name"] == "s1" and dict(row)["name"] == "s1"
        async with rc.execute("SELECT name FROM batches ORDER BY id") as c:
            assert [r[0] async for r in c] == ["s1", "s2"]


@pytest.mark.asyncio
async def test_delete_from_sqlite_sequence_resets_identity(pgdb):
    """server/app.py:2654 的 SQLite 专有语句由垫片翻译成 identity RESTART。"""
    conn = pgdb._write_conn
    await conn.execute("INSERT INTO batches (name) VALUES ('one')")
    await pgdb._db.execute("BEGIN")
    await pgdb._db.execute("DELETE FROM batches")
    await pgdb._db.execute("DELETE FROM sqlite_sequence")
    await pgdb._db.execute("COMMIT")
    await conn.execute("INSERT INTO batches (name) VALUES ('two')")
    assert await conn.fetchval("SELECT id FROM batches") == 1


@pytest.mark.asyncio
async def test_trgm_index_is_usable_by_the_rewritten_like(pgdb):
    """表达式索引与查询表达式必须逐字一致，否则索引白建。"""
    conn = pgdb._write_conn
    await conn.execute("SET enable_seqscan=off")
    plan = "\n".join(r[0] for r in await conn.fetch(
        "EXPLAIN SELECT id FROM asin_data d "
        "WHERE ascii_lower(d.title) LIKE ascii_lower('%goldenbrand%')"))
    assert "idx_asin_data_title_trgm" in plan, plan


@pytest.mark.asyncio
async def test_admin_methods_are_pg_noops(pgdb):
    assert await pgdb.wal_checkpoint("TRUNCATE") is None
    await pgdb.run_startup_optimize()
    await pgdb._open_read_pool()


@pytest.mark.asyncio
async def test_total_changes_raises_instead_of_lying(pgdb):
    """asyncpg 没有这个计数器；静默返回 0 会让上传响应里的 inserted 恒为 0。"""
    with pytest.raises(NotImplementedError):
        _ = pgdb._db.total_changes
