"""Phase 3 —— `server/api/sync.py` 的契约测试。

规格是计划 `.agent/pg_migration_plan.md` §5，对外文档是 `docs/sync_contract.md`。
这里测的是**契约本身**，不是实现细节：每个用例对应契约里一句话，改实现不该让
它们变红，改契约才该。

夹具形状：``httpx.AsyncClient + ASGITransport`` 直接打 ``server.app.app``，
**不跑 lifespan**（那会去连生产库、拉起 4 个后台协程和 relay）。
``server.app.db`` 被 monkeypatch 成 ``pgdb`` 这个一次性库，``DB_BACKEND``
在用例内设成 postgres —— 于是这个文件在 ``pytest tests/`` 与
``DB_BACKEND=postgres pytest tests/`` 两列里跑的是同一件事。
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

pytestmark = pytest.mark.asyncio


# ============================================================
# 夹具
# ============================================================

def _srv():
    import server.app as srv
    return srv


@pytest.fixture
def sync_env(pgdb, monkeypatch):
    """把一次性 PG 库接到真 app 上，并把后端切成 postgres。"""
    srv = _srv()
    monkeypatch.setenv("DB_BACKEND", "postgres")
    monkeypatch.setattr(srv, "db", pgdb, raising=False)
    return srv


@pytest.fixture
def client(sync_env):
    transport = httpx.ASGITransport(app=sync_env.app)
    return httpx.AsyncClient(transport=transport, base_url="http://sync-test")


_INSERT = (
    "INSERT INTO scraper.scrape_events (seq, source_id, gen, asin, marketplace,"
    " zip_requested, zip_observed, zip_verify, collected_at, recorded_at,"
    " outcome, completeness, error_type, batch_id, task_id, worker_id,"
    " attempt, parse_engine, review_hash, slow_hash, hash_ver, payload)"
    " VALUES ($1,$2,$3,$4,'amazon.com','10001','10001','confirmed',"
    " $5,$5,$6,$7,NULL,8123,5512907,'w-hk-02',0,'selectolax',"
    " 'v1:77ab12','v1:1f0c9a',1,$8::jsonb)"
)


async def _seed(conn, gen, seqs, *, outcome="ok", completeness=0,
                recorded_at=None, asin="B0CXXXXXXX"):
    """直插 scrape_events。

    走 outbox+relay 更真实，但那样就控制不了 seq —— 而本文件要测的东西
    （分区边界、保留期下界、空洞）恰恰是 **seq 取什么值**。relay 的正确性
    有 tests/pgdb/test_relay.py 的 43 个用例兜着，这里不重复。
    """
    for i, s in enumerate(seqs):
        await conn.execute(
            _INSERT, s, f"{gen}:evt-{s}", gen, asin,
            recorded_at or __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc),
            outcome, completeness,
            json.dumps({"asin": asin, "seq_probe": s, "title": f"t{i}"}))


async def _partitions(pgdb, pgconn):
    return await pgdb._list_partitions(pgconn)


def _declared_routes(app):
    """递归收集 (path, method, endpoint_name)。

    FastAPI 0.141 把 ``include_router`` 的结果包成 ``_IncludedRouter``，
    它的 ``matches()`` 返回的 child scope 是空的（endpoint 要到实际分发时才定），
    所以「路由解析到了谁」不能靠 matches() 问，只能走声明树。
    """
    out = []

    def walk(routes):
        for r in routes:
            for attr in ("routes", "original_router"):
                sub = getattr(r, attr, None)
                if sub is not None:
                    walk(getattr(sub, "routes", sub))
            path = getattr(r, "path", None)
            ep = getattr(r, "endpoint", None)
            if path and ep is not None:
                for m in sorted(getattr(r, "methods", None) or ["GET"]):
                    out.append((path, m, getattr(ep, "__name__", str(ep))))

    walk(app.routes)
    return out


# ============================================================
# 1) 挂载点：/api/v1 前缀是承重的
# ============================================================

async def test_the_api_v1_prefix_is_load_bearing(client, pgdb, pgconn):
    """计划 §Phase 3 的第一条：挂在 /api/results/* 或 /api/export/* 下会被吞。

    这不是理论。下面前两条断言逐字证明那两个 catch-all 会接走请求并回 404，
    而消费者按 §5.5 会把 404 读成「暂无数据」，游标**永不推进**——同步静默停摆，
    没有任何一侧会告警。第三条证明 /api/v1/* 下没有这个吞噬面。
    """
    async with client as c:
        # (a) 假如挂在 /api/results/sync：被 @app.get("/api/results/{asin}") 吃掉
        r = await c.get("/api/results/sync")
        assert r.status_code == 404, r.text
        # (b) 假如挂在 /api/export/sync：被 @app.get("/api/export/{batch_name}") 吃掉
        r = await c.get("/api/export/sync")
        assert r.status_code == 404, r.text
        # (c) 真正的挂载点：解析到 sync_records，200，且形状是契约的形状
        r = await c.get("/api/v1/sync/records", params={"after_seq": 0})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["contract_version"] == 1
        assert body["records"] == [] and body["has_more"] is False

    # 四条路由真的挂在 app 上，各挂一次，指向我们自己的 handler
    declared = _declared_routes(_srv().app)
    for path, method, name in (
            ("/api/v1/sync/records", "GET", "sync_records"),
            ("/api/v1/sync/status", "GET", "sync_status"),
            ("/api/v1/sync/counts", "GET", "sync_counts"),
            ("/api/v1/sync/ack", "POST", "sync_ack")):
        hits = [d for d in declared if d[0] == path and d[1] == method]
        assert hits == [(path, method, name)], (path, hits)
    # /api/v1 下没有任何 path-param 路由 —— 也就没有 catch-all 吞噬面
    v1 = {d[0] for d in declared if d[0].startswith("/api/v1")}
    assert not any("{" in p for p in v1), v1


async def test_openapi_json_does_not_change(client):
    """``include_in_schema=False``：``/openapi.json`` 是黄金基线 step 5，
    逐字节钉死，且 erpAPI 可能在消费它。四个端点一个都不许出现在里面。"""
    async with client as c:
        r = await c.get("/openapi.json")
    assert r.status_code == 200
    paths = set(r.json()["paths"])
    leaked = sorted(p for p in paths if p.startswith("/api/v1"))
    assert leaked == [], f"新端点泄漏进 openapi: {leaked}"
    # 逐条点名，免得将来改了前缀之后上一条断言变成空过
    for p in ("/api/v1/sync/records", "/api/v1/sync/status",
              "/api/v1/sync/counts", "/api/v1/sync/ack"):
        assert p not in paths, p
    # 反向哨兵：既有的 /api/worker/sync 必须**还在** schema 里 ——
    # 证明这条断言看的是真 schema，而不是一个碰巧为空的字典
    assert "/api/worker/sync" in paths


async def test_importing_the_sync_router_does_not_drag_in_pgdb():
    """``server/api/sync.py`` 被 app.py 模块级 import，所以它不许 import pgdb。

    common/dbfactory.py 的设计约束是「SQLite 部署不需要装 asyncpg」。
    在 sync.py 里写一句 ``from common.pgdb.schema import EVENT_OUTCOMES``
    就会把整个 pgdb 包（连带 asyncpg）拖进每一次启动。
    """
    import ast
    import pathlib
    from server.api import sync as _sync
    src = pathlib.Path(_sync.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.col_offset == 0:
            if (node.module or "").startswith(("common.pgdb", "asyncpg",
                                               "server.app")):
                offenders.append((node.lineno, node.module))
        if isinstance(node, ast.Import) and node.col_offset == 0:
            for a in node.names:
                if a.name.startswith(("common.pgdb", "asyncpg", "server.app")):
                    offenders.append((node.lineno, a.name))
    assert offenders == [], f"模块级 import 了不该 import 的东西: {offenders}"


async def test_valid_outcomes_matches_the_schema():
    """字面量副本不许与 schema 漂开（上一个用例的另一半）。"""
    from common.pgdb.schema import EVENT_OUTCOMES
    from server.api import sync
    assert sync.VALID_OUTCOMES == tuple(EVENT_OUTCOMES)


async def test_the_router_has_an_empty_auth_hook():
    """鉴权留口：router 级 ``dependencies``，现在是空的，加的时候是一行。

    钉住「空」这件事本身：有人**顺手**往里塞一个依赖（比如全局限流），
    对一个还没有任何鉴权的服务来说就是所有 worker 与 erpAPI 当场 401/403。
    """
    from server.api import sync
    assert sync.SYNC_DEPENDENCIES == []
    assert sync.router.dependencies == []
    assert all(route.include_in_schema is False for route in sync.router.routes)


# ============================================================
# 2) 空结果：200，永不 404
# ============================================================

async def test_empty_stream_is_200_with_empty_list_never_404(client):
    """契约 §5.1 表格第一行。**故意**与既有导出端点不同（那些找不到就 404）。"""
    async with client as c:
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "limit": 500})
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["records"] == []
    assert b["has_more"] is False
    assert b["count"] == 0
    assert b["next_after_seq"] == 0, "空页不许推进游标"
    assert b["min_available_seq"] == 1 and b["max_seq"] == 0


async def test_a_cursor_inside_the_window_with_no_new_rows_is_200_empty(
        client, pgdb, pgconn):
    """已经追平的消费者：200 + 空，而不是 404，也不是 409。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 3})
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["records"] == [] and b["has_more"] is False
    assert b["next_after_seq"] == 3
    assert b["max_seq"] == 3


# ============================================================
# 3) 分页：跨分区边界
# ============================================================

async def test_pagination_across_a_partition_boundary(client, pgdb, pgconn):
    """整个套件此前从没让**消费者**跨过一条分区边界。

    p0 是 ``[MINVALUE, SPAN)``，p1 是 ``[SPAN, 2*SPAN)``。在边界两侧各撒几行，
    然后按契约的循环走一遍游标：严格递增、一条不漏、一条不重、
    ``next_after_seq`` 逐页推进、最后一页 ``has_more=false``。
    """
    gen = pgdb.event_gen
    parts = await _partitions(pgdb, pgconn)
    assert len(parts) >= 2, parts
    edge = parts[0][2]                      # p0 的上界 == p1 的下界
    seqs = [edge - 3, edge - 2, edge - 1, edge, edge + 1, edge + 2]
    await _seed(pgconn, gen, seqs)

    # 真的落在两个不同的分区里（否则这个用例是空过的）
    homes = await pgconn.fetch(
        "SELECT seq, tableoid::regclass::text AS part FROM scraper.scrape_events"
        " ORDER BY seq")
    assert len({r["part"] for r in homes}) == 2, [dict(r) for r in homes]

    got, cursor, pages = [], edge - 4, 0
    async with client as c:
        while True:
            r = await c.get("/api/v1/sync/records",
                            params={"after_seq": cursor, "limit": 2})
            assert r.status_code == 200, r.text
            b = r.json()
            pages += 1
            page_seqs = [rec["seq"] for rec in b["records"]]
            assert page_seqs == sorted(page_seqs), "页内必须严格按 seq 升序"
            assert all(s > cursor for s in page_seqs), "after_seq 是独占下界"
            assert b["count"] == len(page_seqs)
            got.extend(page_seqs)
            cursor = b["next_after_seq"]
            assert cursor == (page_seqs[-1] if page_seqs else cursor)
            if not b["has_more"]:
                break
            assert pages < 20, "分页没有收敛"

    assert got == seqs, got
    assert pages == 3, pages
    assert len(got) == len(set(got)), "有重复投递"


async def test_partition_boundary_rows_come_back_whole(client, pgdb, pgconn):
    """跨边界不只是「数量对」：payload / 时间戳 / 哈希都得原样回来。"""
    gen = pgdb.event_gen
    edge = (await _partitions(pgdb, pgconn))[0][2]
    await _seed(pgconn, gen, [edge - 1, edge])
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": edge - 2})
    recs = r.json()["records"]
    assert [x["seq"] for x in recs] == [edge - 1, edge]
    for rec in recs:
        assert isinstance(rec["payload"], dict), "payload 必须是对象，不是 JSON 字符串"
        assert rec["payload"]["seq_probe"] == rec["seq"]
        assert rec["source_id"] == f"{gen}:evt-{rec['seq']}"
        assert rec["gen"] == gen
        assert rec["marketplace"] == "amazon.com"
        assert rec["recorded_at"].endswith("Z"), rec["recorded_at"]
        assert rec["hash_ver"] == 1
        assert rec["review_hash"] == "v1:77ab12"


# ============================================================
# 4) 两条 409
# ============================================================

async def test_409_cursor_below_retention(client, pgdb, pgconn):
    """``after_seq + 1 < min_available_seq`` → 掉出保留窗口。

    **不得**返回「跳过空洞后的下一批」（计划 T8）——那会让消费者拿到一段
    有洞的数据并且毫不知情。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [500, 501, 502])       # 下界是 500
    async with client as c:
        # 498 的下一条是 499 < 500 -> 掉窗
        r = await c.get("/api/v1/sync/records", params={"after_seq": 498})
        assert r.status_code == 409, r.text
        b = r.json()
        assert b["error"] == "cursor_below_retention"
        assert b["min_available_seq"] == 500 and b["max_seq"] == 502
        assert b["after_seq"] == 498
        assert b["gen"] == gen
        assert "records" not in b, "409 不许夹带半页数据"

        # 边界正例：499 的下一条正好是 500 -> 放行，不是 409
        r = await c.get("/api/v1/sync/records", params={"after_seq": 499})
        assert r.status_code == 200, r.text
        assert [x["seq"] for x in r.json()["records"]] == [500, 501, 502]


async def test_409_cursor_ahead_of_stream(client, pgdb, pgconn):
    """``after_seq > max_seq`` → 疑似恢复/回滚。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 4})
        assert r.status_code == 409, r.text
        b = r.json()
        assert b["error"] == "cursor_ahead_of_stream"
        assert b["max_seq"] == 3 and b["after_seq"] == 4
        # 边界正例：正好等于 max_seq 是「追平了」，不是「超前」
        r = await c.get("/api/v1/sync/records", params={"after_seq": 3})
        assert r.status_code == 200


async def test_409_is_server_enforced_not_consumer_optional(client, pgdb, pgconn):
    """守卫不看任何请求参数：没有开关能关掉它。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [500])
    async with client as c:
        for extra in ({}, {"limit": 1}, {"outcomes": "ok"},
                      {"limit": 1000, "outcomes": "ok,not_found"}):
            params = {"after_seq": 1}
            params.update(extra)
            r = await c.get("/api/v1/sync/records", params=params)
            assert r.status_code == 409, (extra, r.text)


async def test_a_stream_pruned_to_empty_still_409s_a_stale_cursor(
        client, pgdb, pgconn):
    """保留期把表裁空之后，落后的游标必须还是 409，不能变成 200 空。

    这是 ``_window()`` 存在的理由。照直取 ``COALESCE(min(seq),0)=0``：
    ``after_seq + 1 < 0`` 永假 → 一个丢了全部数据的消费者拿到 200 空，
    于是**永远等下去**，两侧都不告警。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1000, 1001])
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('max_seq_ever','1001') "
        "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")
    await pgconn.execute("DELETE FROM scraper.scrape_events")   # ≈ DROP 分区

    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 900})
        assert r.status_code == 409, r.text
        assert r.json()["error"] == "cursor_below_retention"
        assert r.json()["min_available_seq"] == 1002

        # 已经追平的消费者不该被这段逻辑误伤
        r = await c.get("/api/v1/sync/records", params={"after_seq": 1001})
        assert r.status_code == 200, r.text
        assert r.json()["records"] == []


async def test_a_pruned_stream_does_not_rewind_max_seq(client, pgdb, pgconn):
    """裁空之后 ``max_seq`` 不许掉回 0。

    §5.5 第 2 行：``st.max_seq < stored_max_seq_ever`` 是消费侧**硬停**。
    一次正常的保留期裁剪触发全网对账，那是纯粹自伤。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1000, 1001])
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('max_seq_ever','1001') "
        "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")
    await pgconn.execute("DELETE FROM scraper.scrape_events")
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 1001})
        s = await c.get("/api/v1/sync/status")
    assert r.json()["max_seq"] == 1001
    assert s.json()["max_seq"] == 1001


# ============================================================
# 5) 一个快照
# ============================================================

async def test_bounds_and_page_share_one_repeatable_read_snapshot(
        client, pgdb, pgconn, monkeypatch):
    """保留期竞态：MIN 与页查询之间跑完一次裁剪，守卫必须仍然看到旧下界。

    做法是在**第一次** ``_bounds()`` 之后、页查询之前，从另一条连接提交一次
    DELETE。如果三条语句不在同一个快照里，页查询会看到裁剪后的表、
    而守卫用的是裁剪前的下界 —— 消费者拿到一个跳过了被裁区间的 200。

    对照组在同一个用例末尾：请求结束后再读一次，证明那次 DELETE 确实提交了、
    确实改变了下界 —— 所以请求内部的不变性只可能来自快照。
    """
    from server.api import sync

    gen = pgdb.event_gen
    await _seed(pgconn, gen, [10, 11, 12, 13])

    real_bounds = sync._bounds
    calls = {"n": 0}

    async def bounds_then_prune(conn):
        out = await real_bounds(conn)
        calls["n"] += 1
        if calls["n"] == 1:
            # 另一条连接，独立事务，真提交
            import asyncpg
            other = await asyncpg.connect(dsn=pgdb.dsn)
            try:
                await other.execute(
                    "DELETE FROM scraper.scrape_events WHERE seq <= 11")
            finally:
                await other.close()
        return out

    monkeypatch.setattr(sync, "_bounds", bounds_then_prune)
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 9})

    assert r.status_code == 200, r.text
    b = r.json()
    assert calls["n"] == 2, "页查询后没有复核下界"
    assert b["min_available_seq"] == 10, "快照没守住：读到了裁剪后的下界"
    assert [x["seq"] for x in b["records"]] == [10, 11, 12, 13], \
        "快照没守住：页查询看见了别人已提交的 DELETE"

    # 对照：那次 DELETE 是真的
    assert await pgconn.fetchval(
        "SELECT min(seq) FROM scraper.scrape_events") == 12


async def test_the_read_transaction_really_is_repeatable_read_and_readonly():
    """``_snapshot()`` 的隔离级别不是注释，是可查询的服务端状态。"""
    from tests.pgdb.helpers import scratch_database
    from server.api import sync
    import server.app as srv

    async with scratch_database("snapiso") as db:
        old = getattr(srv, "db", None)
        srv.db = db
        try:
            async with sync._snapshot() as conn:
                assert await conn.fetchval(
                    "SELECT current_setting('transaction_isolation')") == \
                    "repeatable read"
                assert await conn.fetchval(
                    "SELECT current_setting('transaction_read_only')") == "on"
        finally:
            srv.db = old


async def test_the_post_page_recheck_is_load_bearing(
        client, pgdb, pgconn, monkeypatch):
    """复核用的是**两次下界的较大者**（保守方向）。

    这是一条断言式的防线：REPEATABLE READ 下两次必然相等，所以它平时是零成本。
    这里人为让第二次读到一个更高的下界（= 有人把隔离级别降成 READ COMMITTED
    之后会发生的事），断言请求转成 409 而不是把半页数据发出去。
    """
    from server.api import sync

    gen = pgdb.event_gen
    await _seed(pgconn, gen, [10, 11, 12])

    real_bounds = sync._bounds
    calls = {"n": 0}

    async def drifting_bounds(conn):
        calls["n"] += 1
        if calls["n"] >= 2:
            return (100, 120)          # 「裁剪刚刚跑完」
        return await real_bounds(conn)

    monkeypatch.setattr(sync, "_bounds", drifting_bounds)
    async with client as c:
        r = await c.get("/api/v1/sync/records", params={"after_seq": 9})
    assert r.status_code == 409, r.text
    assert r.json()["error"] == "cursor_below_retention"
    assert r.json()["min_available_seq"] == 100


# ============================================================
# 6) outcome 过滤
# ============================================================

async def test_outcome_filtering(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 4], outcome="ok")
    await _seed(pgconn, gen, [2, 5], outcome="not_found")
    await _seed(pgconn, gen, [3], outcome="blocked")

    async with client as c:
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "outcomes": "ok"})
        b = r.json()
        assert [x["seq"] for x in b["records"]] == [1, 4]
        assert b["outcomes"] == ["ok"]
        assert b["next_after_seq"] == 4, "游标只推进到真正投递过的那一条"

        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "outcomes": "ok,blocked"})
        assert [x["seq"] for x in r.json()["records"]] == [1, 3, 4]

        # 过滤下的 has_more 必须按**过滤后**的集合算：
        # 用 "seq < max_seq" 判断会在这里谎报「还有」。
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "outcomes": "blocked"})
        b = r.json()
        assert [x["seq"] for x in b["records"]] == [3]
        assert b["has_more"] is False and b["max_seq"] == 5

        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "outcomes": "stale"})
        b = r.json()
        assert b["records"] == [] and b["has_more"] is False
        assert b["next_after_seq"] == 0, "空页不推进（否则会跳过没投递的行）"


async def test_outcome_filtering_rejects_unknown_values(client):
    """静默忽略一个拼错的 outcome = 消费者以为自己在筛、其实拿的是全集。"""
    async with client as c:
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "outcomes": "ok,oops"})
        assert r.status_code == 422, r.text
        b = r.json()
        assert b["error"] == "invalid_parameter"
        assert "oops" in b["detail"]
        assert set(b["allowed"]) == {"ok", "not_found", "blocked",
                                     "parse_failed", "stale"}


async def test_parameter_bounds(client):
    async with client as c:
        assert (await c.get("/api/v1/sync/records",
                            params={"after_seq": -1})).status_code == 422
        assert (await c.get("/api/v1/sync/records",
                            params={"after_seq": 0, "limit": 0})).status_code == 422
        assert (await c.get("/api/v1/sync/records",
                            params={"after_seq": 0, "limit": 1001})).status_code == 422
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "limit": 1000})
        assert r.status_code == 200 and r.json()["limit"] == 1000
        # 默认 200，与 /api/results 的 le=200 互不影响
        r = await c.get("/api/v1/sync/records", params={"after_seq": 0})
        assert r.json()["limit"] == 200
        # 超出 bigint：必须是 422，不是 500。消费者把游标存进 JS Number
        # 或者做一次坏加法就能造出这种值，而 asyncpg 会直接抛 OverflowError。
        huge = 2 ** 63
        assert (await c.get("/api/v1/sync/records",
                            params={"after_seq": huge})).status_code == 422
        assert (await c.get("/api/v1/sync/counts",
                            params={"from_seq": huge, "to_seq": huge})
                ).status_code == 422
        assert (await c.get("/api/v1/sync/counts",
                            params={"from_seq": 0, "to_seq": huge})
                ).status_code == 422
        assert (await c.post("/api/v1/sync/ack",
                             json={"gen": "x", "ack_seq": huge})
                ).status_code == 422


async def test_limit_is_respected_and_has_more_is_exact(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, list(range(1, 8)))
    async with client as c:
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "limit": 7})
        assert r.json()["count"] == 7 and r.json()["has_more"] is False
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": 0, "limit": 6})
        assert r.json()["count"] == 6 and r.json()["has_more"] is True


# ============================================================
# 7) ack
# ============================================================

async def test_ack_is_monotonic_and_never_goes_backwards(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3, 4, 5])
    async with client as c:
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 3})
        assert r.status_code == 200, r.text
        assert r.json()["ack_seq"] == 3 and r.json()["advanced"] is True

        # 后退：收下但不生效
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 1})
        assert r.status_code == 200, r.text
        assert r.json()["ack_seq"] == 3, "ack 后退了"
        assert r.json()["sent_ack_seq"] == 1
        assert r.json()["advanced"] is False

        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 5})
        assert r.json()["ack_seq"] == 5 and r.json()["lag_records"] == 0

    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k = 'ack_seq'") == "5"
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k = 'ack_at'")


async def test_ack_is_monotonic_under_concurrency(client, pgdb, pgconn):
    """并发 ack 不许互相覆盖 —— 单调是在 SQL 里用 GREATEST 做的，不是读改写。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, list(range(1, 51)))
    async with client as c:
        await asyncio.gather(*[
            c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": n})
            for n in (7, 42, 13, 5, 30, 21)])
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 1})
    assert r.json()["ack_seq"] == 42


async def test_ack_gen_mismatch_is_409(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1])
    async with client as c:
        r = await c.post("/api/v1/sync/ack",
                         json={"gen": "deadbeefcafe", "ack_seq": 1})
        assert r.status_code == 409, r.text
        b = r.json()
        assert b["error"] == "gen_mismatch"
        assert b["gen"] == gen and b["sent_gen"] == "deadbeefcafe"
    # 拒绝之后不许留下任何痕迹
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k = 'ack_seq'") is None


async def test_ack_beyond_the_stream_head_is_409(client, pgdb, pgconn):
    """确认一段本实例从没发过的 seq = 授权 Phase 6 裁掉消费者没拿到的数据。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 99})
        assert r.status_code == 409, r.text
        assert r.json()["error"] == "ack_ahead_of_stream"
        assert r.json()["max_seq"] == 3
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k = 'ack_seq'") is None


async def test_ack_rejects_bad_shapes(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1])
    async with client as c:
        for body in ({}, {"gen": gen}, {"ack_seq": 1}, {"gen": "", "ack_seq": 1},
                     {"gen": gen, "ack_seq": "1"}, {"gen": gen, "ack_seq": -1},
                     {"gen": gen, "ack_seq": True}, {"gen": 7, "ack_seq": 1}):
            r = await c.post("/api/v1/sync/ack", json=body)
            assert r.status_code == 422, (body, r.status_code, r.text)


async def test_ack_seq_starts_as_null_not_zero(client):
    """计划 §Phase 6：初值给 0 会让 ``min(时间下界, 0) = 0``，保留期一行不裁。"""
    async with client as c:
        b = (await c.get("/api/v1/sync/status")).json()
    assert b["ack_seq"] is None
    assert b["lag_records"] is None


# ============================================================
# 8) counts
# ============================================================

async def test_counts_arithmetic(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [10, 20, 30], outcome="ok")
    await _seed(pgconn, gen, [40, 50], outcome="not_found")
    await _seed(pgconn, gen, [60], outcome="blocked")

    async with client as c:
        b = (await c.get("/api/v1/sync/counts",
                         params={"from_seq": 0, "to_seq": 100})).json()
        assert b["count"] == 6
        assert b["min_seq"] == 10 and b["max_seq"] == 60
        assert b["by_outcome"] == {"ok": 3, "not_found": 2, "blocked": 1}
        assert sum(b["by_outcome"].values()) == b["count"]
        assert b["span"] == 101
        assert b["stream_min_available_seq"] == 10 and b["stream_max_seq"] == 60
        # 下界 0 在窗口（从 10 起）之下 -> 这段可能被裁过
        assert b["range_fully_retained"] is False

        # 闭区间：两端都算在内
        b = (await c.get("/api/v1/sync/counts",
                         params={"from_seq": 20, "to_seq": 50})).json()
        assert b["count"] == 4 and b["min_seq"] == 20 and b["max_seq"] == 50

        # 分段对账相加 == 整段
        left = (await c.get("/api/v1/sync/counts",
                            params={"from_seq": 10, "to_seq": 30})).json()
        right = (await c.get("/api/v1/sync/counts",
                             params={"from_seq": 31, "to_seq": 60})).json()
        whole = (await c.get("/api/v1/sync/counts",
                             params={"from_seq": 10, "to_seq": 60})).json()
        assert left["count"] + right["count"] == whole["count"] == 6
        assert whole["range_fully_retained"] is True
        # 上界超过流头**不**影响 range_fully_retained：那只是「还没采到」，
        # 不是「没保留」。判据只看下界。
        beyond = (await c.get("/api/v1/sync/counts",
                              params={"from_seq": 10, "to_seq": 99999})).json()
        assert beyond["range_fully_retained"] is True
        assert beyond["count"] == 6 and beyond["stream_max_seq"] == 60

        # 空区间不是错误
        b = (await c.get("/api/v1/sync/counts",
                         params={"from_seq": 61, "to_seq": 70})).json()
        assert b["count"] == 0 and b["min_seq"] is None
        assert b["min_recorded_at"] is None


async def test_counts_hour_buckets_sum_to_the_total(client, pgdb, pgconn):
    import datetime as _dt
    gen = pgdb.event_gen
    t0 = _dt.datetime(2026, 8, 4, 9, 30, tzinfo=_dt.timezone.utc)
    await _seed(pgconn, gen, [1, 2], recorded_at=t0)
    await _seed(pgconn, gen, [3], recorded_at=t0 + _dt.timedelta(hours=1))
    await _seed(pgconn, gen, [4, 5, 6], recorded_at=t0 + _dt.timedelta(hours=2))

    async with client as c:
        b = (await c.get("/api/v1/sync/counts",
                         params={"from_seq": 0, "to_seq": 100,
                                 "bucket": "hour"})).json()
    assert b["bucket"] == "hour"
    assert [x["count"] for x in b["buckets"]] == [2, 1, 3]
    assert sum(x["count"] for x in b["buckets"]) == b["count"] == 6
    # 桶边界是 UTC 整点，与会话 TimeZone 无关
    assert [x["bucket_start"] for x in b["buckets"]] == [
        "2026-08-04T09:00:00Z", "2026-08-04T10:00:00Z", "2026-08-04T11:00:00Z"]
    assert b["buckets"][0]["min_seq"] == 1 and b["buckets"][2]["max_seq"] == 6


async def test_counts_rejects_bad_ranges(client):
    async with client as c:
        r = await c.get("/api/v1/sync/counts",
                        params={"from_seq": 0, "to_seq": 8_000_000})
        assert r.status_code == 422, r.text
        assert r.json()["error"] == "range_too_wide"
        assert r.json()["span"] == 8_000_001

        r = await c.get("/api/v1/sync/counts",
                        params={"from_seq": 0, "to_seq": 7_999_999})
        assert r.status_code == 200, "上限本身必须放行"

        assert (await c.get("/api/v1/sync/counts",
                            params={"from_seq": 50, "to_seq": 10})
                ).status_code == 422
        assert (await c.get("/api/v1/sync/counts",
                            params={"from_seq": -1, "to_seq": 10})
                ).status_code == 422
        assert (await c.get("/api/v1/sync/counts",
                            params={"from_seq": 0, "to_seq": 10,
                                    "bucket": "minute"})).status_code == 422


# ============================================================
# 9) status
# ============================================================

async def test_status_has_every_field_the_contract_promises(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        r = await c.get("/api/v1/sync/status")
    assert r.status_code == 200, r.text
    b = r.json()
    for k in ("gen", "instance_id", "min_available_seq", "max_seq", "ack_seq",
              "lag_records", "relay_lag_seconds", "outbox_depth",
              "oldest_recorded_at", "newest_recorded_at", "retention_forced",
              "forced_prune_log", "db_size_bytes", "free_disk_bytes",
              "observed_daily_insert_rate", "contract_version",
              "server_time_utc", "relay_state"):
        assert k in b, f"/status 少了 §5.2 要求的 {k}"
    assert b["gen"] == gen
    assert b["min_available_seq"] == 1 and b["max_seq"] == 3
    assert b["retention_forced"] is False and b["forced_prune_log"] == []
    assert b["db_size_bytes"] > 0 and b["free_disk_bytes"] > 0
    assert b["oldest_recorded_at"].endswith("Z")


async def test_status_and_records_never_disagree_about_the_window(
        client, pgdb, pgconn):
    """两个端点的 ``min_available_seq`` / ``max_seq`` 必须同源。

    不同源的话消费者会看到「status 说还有数据，records 回 409」这种不可能状态，
    而它按 §5.5 只能理解成「服务端坏了」。
    """
    gen = pgdb.event_gen
    async with client as c:
        for seqs in ([], [5], [6, 7], [900]):
            if seqs:
                await _seed(pgconn, gen, seqs)
            s = (await c.get("/api/v1/sync/status")).json()
            r = (await c.get("/api/v1/sync/records",
                             params={"after_seq": 0})).json()
            assert s["min_available_seq"] == r["min_available_seq"], seqs
            assert s["max_seq"] == r["max_seq"], seqs
        # 裁空之后（两个端点都得走 _window 的空表分支）
        await pgconn.execute(
            "INSERT INTO scraper.sync_meta (k,v) VALUES ('max_seq_ever','900') "
            "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")
        await pgconn.execute("DELETE FROM scraper.scrape_events")
        s = (await c.get("/api/v1/sync/status")).json()
        r = (await c.get("/api/v1/sync/records",
                         params={"after_seq": 900})).json()
        assert s["min_available_seq"] == r["min_available_seq"] == 901
        assert s["max_seq"] == r["max_seq"] == 900


async def test_status_reports_ack_and_lag_after_an_ack(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, list(range(1, 11)))
    async with client as c:
        await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 4})
        b = (await c.get("/api/v1/sync/status")).json()
    assert b["ack_seq"] == 4
    assert b["lag_records"] == 6
    assert b["ack_at"].endswith("Z")


async def test_status_surfaces_the_forced_prune_log(client, pgdb, pgconn):
    """Phase 6 写、Phase 3 读：持久列表，不是瞬时布尔（消费者宕机正是触发前提）。"""
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('forced_prune_log', $1) "
        "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        json.dumps([{"at": "2026-08-04T09:00:00Z", "dropped_through_seq": 41000000,
                     "reason": "disk"}]))
    async with client as c:
        b = (await c.get("/api/v1/sync/status")).json()
        r = (await c.get("/api/v1/sync/records", params={"after_seq": 0})).json()
    assert b["retention_forced"] is True
    assert b["forced_prune_log"][0]["dropped_through_seq"] == 41000000
    assert r["retention_forced"] is True


# ============================================================
# 10) completeness：Phase 2 留给 Phase 3 的必答题
# ============================================================

async def test_completeness_ok_is_the_conjunction_and_is_false_until_phase4(
        client, pgdb, pgconn):
    """Phase 2 观测不到 HTML 区块存在性，所有行的 ``completeness`` 都是 0。

    契约必须显式规定消费侧拿到 0 怎么办，否则按 §5.5 的 ``completeness_ok`` 门
    **没有一行进得了 catalog.products**，沃尔玛侧会一直是空的而没人知道为什么。
    这里把「0 → false」「7（未标 MEASURED）→ 仍然 false」「15 → true」三种
    都钉死。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1], completeness=0)     # Phase 2 的现实
    await _seed(pgconn, gen, [2], completeness=7)     # 三块齐全但没标"测过"
    await _seed(pgconn, gen, [3], completeness=15)    # 测过 + 三块齐全
    await _seed(pgconn, gen, [4], completeness=11)    # 测过但缺一块 (8|2|1)
    async with client as c:
        recs = (await c.get("/api/v1/sync/records",
                            params={"after_seq": 0})).json()["records"]
    assert [r["completeness"] for r in recs] == [0, 7, 15, 11]
    assert [r["completeness_ok"] for r in recs] == [False, False, True, False]


# ============================================================
# 11) 资源纪律
# ============================================================

async def test_read_endpoints_do_not_need_the_write_lock(client, pgdb, pgconn):
    """借池连接、不碰 D-2 的那条写连接。

    握着写锁（= 一次 accept_results_batch 正在提交）时四个端点必须照常响应。
    反过来也成立：一个狂拉的消费者饿不死 worker 交结果的路径。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        async with pgdb._write_lock:
            assert (await c.get("/api/v1/sync/records",
                                params={"after_seq": 0})).status_code == 200
            assert (await c.get("/api/v1/sync/status")).status_code == 200
            assert (await c.get("/api/v1/sync/counts",
                                params={"from_seq": 0, "to_seq": 10})
                    ).status_code == 200
            r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 2})
            assert r.status_code == 200, r.text


async def test_every_page_returns_its_connection_to_the_pool(client, pgdb, pgconn):
    """一页 = 一次借还。不学 iter_results 攥着一条连接跑完整趟导出。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, list(range(1, 21)))
    idle_before = pgdb._pool.get_idle_size()
    async with client as c:
        for _ in range(pgdb._pool.get_max_size() + 5):
            r = await c.get("/api/v1/sync/records",
                            params={"after_seq": 0, "limit": 5})
            assert r.status_code == 200
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 5})
        assert r.status_code == 200
    assert pgdb._pool.get_idle_size() == idle_before, "连接没还回池里"


async def test_a_slow_consumer_cannot_be_served_from_the_write_connection(
        pgdb, monkeypatch):
    """池不可用时宁可 503，绝不去开那条写连接的事务。

    ``db.read()`` 在 ``_pool is None`` 时会退回 ``_write_proxy``（pool.py:940）。
    在它上面开事务 = 把别人正在提交的采集结果拖进我们的只读事务里。
    """
    from server.api import sync
    srv = _srv()
    monkeypatch.setenv("DB_BACKEND", "postgres")
    monkeypatch.setattr(srv, "db", pgdb, raising=False)
    monkeypatch.setattr(pgdb, "_pool", None, raising=False)

    transport = httpx.ASGITransport(app=srv.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://sync-test") as c:
        for path, kw in (("/api/v1/sync/records", {"params": {"after_seq": 0}}),
                         ("/api/v1/sync/status", {}),
                         ("/api/v1/sync/counts",
                          {"params": {"from_seq": 0, "to_seq": 5}})):
            r = await c.get(path, **kw)
            assert r.status_code == 503, (path, r.status_code, r.text)
            assert r.json()["error"] == "event_stream_unavailable"


# ============================================================
# 12) SQLite：503，绝不 404
# ============================================================

async def test_sqlite_backend_503s_and_never_404s(monkeypatch):
    """后端是 SQLite 时四个端点仍然存在，回 503。

    不挂路由 = 404，而 404 正是消费者会读成「暂无数据」并静默停摆的那个码。
    503 不会被误读。黄金样本不受影响：64 步不碰 /api/v1/*。
    """
    srv = _srv()
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    monkeypatch.setattr(srv, "db", None, raising=False)
    transport = httpx.ASGITransport(app=srv.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://sync-test") as c:
        cases = [
            ("GET", "/api/v1/sync/records", {"params": {"after_seq": 0}}),
            ("GET", "/api/v1/sync/status", {}),
            ("GET", "/api/v1/sync/counts", {"params": {"from_seq": 0, "to_seq": 5}}),
            ("POST", "/api/v1/sync/ack", {"json": {"gen": "x", "ack_seq": 1}}),
        ]
        for method, path, kw in cases:
            r = await c.request(method, path, **kw)
            assert r.status_code == 503, (path, r.status_code, r.text)
            b = r.json()
            assert b["error"] == "event_stream_unavailable"
            assert b["backend"] == "sqlite"
