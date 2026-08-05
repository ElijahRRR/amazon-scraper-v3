"""Phase 6 —— 保留期（DROP 分区）+ ack 闩锁的契约测试。

规格是计划 `.agent/pg_migration_plan.md` §Phase 6 与审计 §3.6；
实现是 `common/pgdb/retention.py` + `server/api/sync.py`。

⚠ **黄金样本对本阶段是瞎的**，一个字都不能拿来当证据：
64 步里没有任何一步碰 `/api/v1/*`，`crawl_time` 之类的时间键全在
`_VOLATILE_KEYS` 里被刷成 `<VOLATILE>`，而分区 DROP 更是连库都不碰它。
所以这里的每一条都自带证据：真的建分区、真的灌行、真的 DROP、
真的用 HTTP 打端点看返回码。

------------------------------------------------------------------
分区形状（生产默认 span = 2000 万，不改环境变量）
------------------------------------------------------------------
    p0 [MINVALUE, 20_000_000)     老数据
    p1 [20_000_000, 40_000_000)   老数据
    p2 [40_000_000, 60_000_000)   新数据 + 序列停在这里 = 写入头，永不可裁

`setval` 把序列推到 p2 里，于是 p0/p1 「密封」（未来不可能再有行落进来），
p2 是头。这是用生产 span 造出多分区场景的唯一诚实做法 ——
改 `PG_EVENT_PARTITION_SPAN` 得在 import 之前动环境变量，那会污染整个进程。
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os

import asyncpg
import httpx
import pytest

from common.pgdb import retention as rt

pytestmark = pytest.mark.asyncio

P1_LO = 20_000_000
P2_LO = 40_000_000


# ============================================================
# 夹具与助手
# ============================================================

def _srv():
    import server.app as srv
    return srv


@pytest.fixture
def sync_env(pgdb, monkeypatch):
    srv = _srv()
    monkeypatch.setenv("DB_BACKEND", "postgres")
    monkeypatch.setattr(srv, "db", pgdb, raising=False)
    return srv


@pytest.fixture
def client(sync_env):
    transport = httpx.ASGITransport(app=sync_env.app)
    return httpx.AsyncClient(transport=transport, base_url="http://retention-test")


_INSERT = (
    "INSERT INTO scraper.scrape_events (seq, source_id, gen, asin, marketplace,"
    " zip_requested, zip_observed, zip_verify, collected_at, recorded_at,"
    " outcome, completeness, error_type, batch_id, task_id, worker_id,"
    " attempt, parse_engine, review_hash, slow_hash, hash_ver, payload)"
    " VALUES ($1,$2,$3,'B0RETENT01','amazon.com','10001','10001','confirmed',"
    " $4,$4,'ok',0,NULL,8123,5512907,'w-hk-02',0,'selectolax',"
    " 'v1:77ab12','v1:1f0c9a',1,$5::jsonb)"
)


async def _seed(conn, gen, seqs, *, age_days: float = 0.0):
    ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=age_days)
    for s in seqs:
        await conn.execute(_INSERT, s, f"{gen}:evt-{s}", gen, ts,
                           json.dumps({"seq_probe": s}))


async def _setval(conn, value: int):
    name = await conn.fetchval(
        "SELECT pg_get_serial_sequence('scraper.scrape_events', 'seq')")
    await conn.execute(f"SELECT setval('{name}', {value}, true)")


async def _three_partitions(pgdb, conn, *, old_days=60.0, new_days=0.0,
                            p0=(1, 2, 3), p1=None, p2=None):
    """造出「两个密封的老分区 + 一个在写的新分区」这个标准形状。"""
    p1 = [P1_LO + i for i in (1, 2, 3)] if p1 is None else p1
    p2 = [P2_LO + i for i in (1, 2, 3)] if p2 is None else p2
    gen = pgdb.event_gen
    await _seed(conn, gen, p0, age_days=old_days)
    await _seed(conn, gen, p1, age_days=old_days)
    await _seed(conn, gen, p2, age_days=new_days)
    await _setval(conn, max(p2))
    return gen


async def _partition_names(conn):
    rows = await conn.fetch(rt._PARTITION_CATALOG_SQL)
    return sorted(r["name"] for r in rows)


# ============================================================
# 1) 下界代数 —— P6-2 的第一层（纯函数，不用库）
# ============================================================

async def test_the_ack_floor_can_not_be_expressed_as_zero():
    """计划 §Phase 6 点名的陷阱：`ack_seq` 初值给 0，保留期静默变 no-op。

    这里钉死的是**类型层面**的不变式：「没有 ack」只有一种表示法（None），
    而 0 会当场炸。三种「没有」的输入必须全部映射到 None。
    """
    assert rt.read_ack_floor(None) is None          # 键不存在
    assert rt.read_ack_floor("0") is None           # 存进去过一个 0
    assert rt.read_ack_floor("") is None
    assert rt.read_ack_floor("  ") is None
    assert rt.read_ack_floor("abc") is None         # 手工写坏了
    assert rt.read_ack_floor("-5") is None
    assert rt.read_ack_floor("41808200") == 41808200
    assert rt.read_ack_floor("  7 ") == 7

    with pytest.raises(rt.RetentionInvariantError):
        rt.combine_floors(age_floor=1000, ack_floor=0, hard_floor=0, ack_slack=0)


async def test_floor_formula_matches_the_plan_line_for_line():
    """floor = max(硬下界, min(age_floor, ack_floor - slack))。"""
    f = rt.combine_floors
    # 从未 ack：ack 侧不设下界，退化成纯时间（计划 T9）
    assert f(age_floor=1000, ack_floor=None, hard_floor=0, ack_slack=200) == 1000
    # 消费者落后：ack 侧压住时间下界
    assert f(age_floor=1000, ack_floor=500, hard_floor=0, ack_slack=200) == 300
    # 消费者领先：时间下界压住 ack
    assert f(age_floor=1000, ack_floor=9000, hard_floor=0, ack_slack=200) == 1000
    # 应急下界在 max 外面，可以越过 ack —— 这正是「强制裁剪」的定义
    assert f(age_floor=10, ack_floor=20, hard_floor=9999, ack_slack=200) == 9999
    # slack 不会把下界推成负数
    assert f(age_floor=1000, ack_floor=50, hard_floor=0, ack_slack=200) == 0


async def test_configured_slack_can_never_drop_below_the_contract_overlap(pgdb):
    """P6-5：契约 §7 把 OVERLAP=200 写给了消费者，服务端不许比它留得少。"""
    pgdb._retention_overrides = {"ack_slack_seq": 0}
    assert pgdb.retention_config()["ack_slack_seq"] == rt.CONSUMER_OVERLAP
    pgdb._retention_overrides = {"ack_slack_seq": -10_000}
    assert pgdb.retention_config()["ack_slack_seq"] == rt.CONSUMER_OVERLAP
    pgdb._retention_overrides = {"ack_slack_seq": 50_000}
    assert pgdb.retention_config()["ack_slack_seq"] == 50_000


# ============================================================
# 2) P6-2：从来没被 ack 过的库，保留期照样裁
# ============================================================

async def test_retention_prunes_a_database_that_has_never_been_acked(pgdb, pgconn):
    """计划 T9。`ack_seq` 若被初始化成 0，这个用例会看到 0 个分区被裁。"""
    gen = await _three_partitions(pgdb, pgconn)
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='ack_seq'") is None, \
        "前提：这个库从来没有人 ack 过"

    before = await _partition_names(pgconn)
    summary = await pgdb.run_retention_pass()
    after = await _partition_names(pgconn)

    assert summary["ack_seq"] is None
    assert summary["dropped_count"] == 2, summary
    assert [d["partition"] for d in summary["dropped"]] == \
        ["scrape_events_p0", "scrape_events_p1"]
    assert set(before) - set(after) == {"scrape_events_p0", "scrape_events_p1"}
    # 老行没了，新行一条不少
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 3
    assert await pgconn.fetchval("SELECT min(seq) FROM scraper.scrape_events") \
        == P2_LO + 1
    # 而且一条都不是 DELETE 出来的
    assert all(not d.get("forced") for d in summary["dropped"]), \
        "纯按时间下界裁，不该被记成强制裁剪"


async def test_retention_is_a_partition_drop_not_a_row_delete(pgdb, pgconn):
    """计划 §2.1：按 seq 分区的**全部意义**就是保留期 = DROP 分区。

    两面证据：
      1. 源码里对 scrape_events 一条 DELETE 都没有；
      2. 裁完之后 pg_class 里少了两张表 —— DELETE 做不到这件事。
    """
    import inspect
    src = inspect.getsource(rt)
    assert "DELETE FROM scraper.scrape_events" not in src
    assert "DROP TABLE" in src

    await _three_partitions(pgdb, pgconn)
    n_before = await pgconn.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid "
        "JOIN pg_class p ON p.oid=i.inhparent WHERE p.relname='scrape_events'")
    await pgdb.run_retention_pass()
    n_after = await pgconn.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid "
        "JOIN pg_class p ON p.oid=i.inhparent WHERE p.relname='scrape_events'")
    assert n_after == n_before - 2
    # 死元组为 0：DELETE 会留下一堆，DROP 一个都不留
    assert await pgconn.fetchval(
        "SELECT COALESCE(sum(n_dead_tup),0) FROM pg_stat_user_tables "
        "WHERE schemaname='scraper' AND relname LIKE 'scrape_events%'") == 0


# ============================================================
# 3) P6-1：只裁「整段都在 floor 之下」的分区
# ============================================================

async def test_a_partition_survives_if_even_one_row_is_above_the_floor(
        pgdb, pgconn):
    """`max(seq) <= floor` 是硬条件。p1 里放一行**新**数据，整个 p1 就必须活着。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3], age_days=60)
    await _seed(pgconn, gen, [P1_LO + 1, P1_LO + 2], age_days=60)
    await _seed(pgconn, gen, [P1_LO + 3], age_days=0)          # ← 这一行是新的
    await _seed(pgconn, gen, [P2_LO + 1], age_days=0)
    await _setval(pgconn, P2_LO + 1)

    summary = await pgdb.run_retention_pass()

    assert [d["partition"] for d in summary["dropped"]] == ["scrape_events_p0"]
    assert summary["stopped_because"] == "above_floor"
    names = await _partition_names(pgconn)
    assert "scrape_events_p1" in names and "scrape_events_p0" not in names
    # p1 里那两条老行**没有**被单独删掉 —— 保留期不删行
    assert await pgconn.fetchval(
        "SELECT count(*) FROM scraper.scrape_events WHERE seq < $1", P2_LO) == 3


async def test_the_head_partition_is_never_dropped_even_when_fully_below_floor(
        pgdb, pgconn):
    """承重约束 3：判「还会不会收到新行」只能靠序列水位。

    把**所有**行都做成很老的，于是时间下界 = 流头，三个分区在
    `max(seq) <= floor` 上全部成立。但 p2 是写入头，裁掉它 relay 下一条
    INSERT 就会撞 `no partition of relation`，整条流停摆。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1], age_days=99)
    await _seed(pgconn, gen, [P1_LO + 1], age_days=99)
    await _seed(pgconn, gen, [P2_LO + 1], age_days=99)
    await _setval(pgconn, P2_LO + 1)

    summary = await pgdb.run_retention_pass()

    assert "scrape_events_p2" not in [d["partition"] for d in summary["dropped"]]
    assert "scrape_events_p2" in await _partition_names(pgconn)
    # 而且流还能写：拿一个新号插进去不炸
    seq = await pgconn.fetchval("SELECT nextval(pg_get_serial_sequence("
                                "'scraper.scrape_events','seq'))")
    await _seed(pgconn, gen, [seq])
    assert await pgconn.fetchval(
        "SELECT count(*) FROM scraper.scrape_events WHERE seq = $1", seq) == 1


async def test_retention_drops_a_prefix_only_and_never_punches_a_hole(
        pgdb, pgconn):
    """中间分区不可裁时必须立刻停，绝不跳过去裁它后面的。

    跳过去 = 流中间出现一个洞，而 `/records` 的 409 判据是
    `after_seq + 1 < min(seq)` —— 洞在 min 之上，守卫看不见，
    消费者会拿到一个静默跳过了整段的 200。这是本阶段唯一能造出静默丢数据的写法。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2], age_days=60)              # p0：可裁
    await _seed(pgconn, gen, [P1_LO + 1], age_days=0)          # p1：新，不可裁
    await _seed(pgconn, gen, [P2_LO + 1], age_days=60)         # p2：老但是头
    await _setval(pgconn, P2_LO + 1)
    # p3 也造出来并密封在 p2 之后是不可能的（p2 就是头），所以这里检查的是
    # 「p1 挡住之后 p2 不会被越过」。
    summary = await pgdb.run_retention_pass()

    assert [d["partition"] for d in summary["dropped"]] == ["scrape_events_p0"]
    assert summary["stopped_because"] == "above_floor"
    names = await _partition_names(pgconn)
    assert {"scrape_events_p1", "scrape_events_p2"} <= set(names)
    # 剩下的 seq 必须仍然是「一段连续的尾巴」：min 之上没有被挖掉的分区
    rows = [r["seq"] for r in await pgconn.fetch(
        "SELECT seq FROM scraper.scrape_events ORDER BY seq")]
    assert rows == [P1_LO + 1, P2_LO + 1]


async def test_ack_below_the_age_floor_holds_retention_back(pgdb, pgconn):
    """`min(age_floor, ack_floor)`：消费者落后时，ack 才是那个决定性的下界。

    两步，第二步是 P6-5 的核心：消费者刚跨进 p1 时**连 p0 都不能裁** ——
    裁掉 p0 之后 `min(seq)` 会跳到 p1 的头，而消费者按契约要从
    `cursor - OVERLAP` 回拉，那个位置就落在 p0 里了。
    """
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3], age_days=60)
    await _seed(pgconn, gen, [P1_LO + i for i in range(1, 301)], age_days=60)
    await _seed(pgconn, gen, [P2_LO + 1], age_days=0)
    await _setval(pgconn, P2_LO + 1)
    pgdb._retention_overrides = {"ack_slack_seq": rt.CONSUMER_OVERLAP}

    async def ack(v):
        await pgconn.execute(
            "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq',$1) "
            "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v", str(v))

    # (1) 消费者刚读到 p1 的第一条：一个分区都不能动
    await ack(P1_LO + 1)
    s1 = await pgdb.run_retention_pass()
    assert s1["dropped_count"] == 0
    assert s1["stopped_because"] == "would_break_overlap"
    assert "scrape_events_p0" in await _partition_names(pgconn)

    # (2) 消费者往前走了 300 条，重叠窗口整个离开了 p0 —— 这时 p0 才可以裁
    await ack(P1_LO + 300)
    s2 = await pgdb.run_retention_pass()
    assert [d["partition"] for d in s2["dropped"]] == ["scrape_events_p0"]
    assert s2["stopped_because"] == "above_floor"
    assert "scrape_events_p1" in await _partition_names(pgconn), \
        "消费者只确认到 p1 末尾，整个 p1 必须留着"


# ============================================================
# 4) P6-2 的第二层：库层面拒收 ack_seq = 0
# ============================================================

async def test_the_database_itself_refuses_to_store_ack_seq_zero(pgdb, pgconn):
    """结构性防线：不是「代码今天写对了」，是**存不进去**。"""
    with pytest.raises(asyncpg.CheckViolationError):
        await pgconn.execute(
            "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq','0')")
    # 别的键不受影响
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('whatever','0')")
    # 已有的正常值也不能被改成 0
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq','5')")
    with pytest.raises(asyncpg.CheckViolationError):
        await pgconn.execute(
            "UPDATE scraper.sync_meta SET v='0' WHERE k='ack_seq'")


async def test_a_phantom_ack_seq_zero_is_repaired_and_retention_resumes(
        pgdb, pgconn):
    """已经踩过陷阱的库：自愈掉幽灵 0，再把防线装上，保留期恢复工作。"""
    # 先把防线拆了，模拟一个 Phase 6 之前建的库
    await pgconn.execute(
        f"ALTER TABLE scraper.sync_meta DROP CONSTRAINT {rt.ACK_SEQ_CHECK}")
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq','0') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")
    await _three_partitions(pgdb, pgconn)

    summary = await pgdb.run_retention_pass()

    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='ack_seq'") is None
    assert await pgconn.fetchval(
        "SELECT 1 FROM pg_constraint WHERE conname=$1", rt.ACK_SEQ_CHECK) == 1
    assert summary["dropped_count"] == 2, "自愈之后保留期必须恢复正常裁剪"
    assert pgdb._rt()["counters"]["phantom_ack_repaired"] == 1


async def test_ack_zero_over_http_is_a_no_op_not_a_stored_zero(client, pgdb, pgconn):
    """消费者冷启动时 ack 0 是合法的，但它绝不能变成保留期的下界。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1, 2, 3])
    async with client as c:
        r = await c.post("/api/v1/sync/ack", json={"gen": gen, "ack_seq": 0})
        assert r.status_code == 200, r.text
        b = r.json()
        assert b["ack_seq"] is None and b["advanced"] is False
        assert b["sent_ack_seq"] == 0
        s = (await c.get("/api/v1/sync/status")).json()
    assert s["ack_seq"] is None and s["lag_records"] is None
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='ack_seq'") is None
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='ack_at'") is None, \
        "ack_at 也不能写：那是「有人 ack 过」的假证据"


# ============================================================
# 5) P6-4：min_available_seq 永远现算
# ============================================================

async def test_min_available_seq_is_computed_live_not_read_from_sync_meta(
        client, pgdb, pgconn):
    """往 sync_meta 里塞一个假的缓存键，两个端点都必须视而不见。

    这是「有没有缓存」的判决性证据：真有缓存路径的话，塞进去的值会露头。
    """
    gen = await _three_partitions(pgdb, pgconn)
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('min_available_seq','999999') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")

    async with client as c:
        s = (await c.get("/api/v1/sync/status")).json()
        r = (await c.get("/api/v1/sync/records", params={"after_seq": 0})).json()
        assert s["min_available_seq"] == 1 and r["min_available_seq"] == 1

        # 裁掉一个分区之后，两个端点立刻跟着动 —— 没有任何一步去刷缓存
        await pgconn.execute("DROP TABLE scraper.scrape_events_p0")
        s = (await c.get("/api/v1/sync/status")).json()
        r = (await c.get("/api/v1/sync/records", params={"after_seq": 0})).json()
    assert s["min_available_seq"] == P1_LO + 1
    assert r["min_available_seq"] == P1_LO + 1
    # 假缓存键还在库里，从头到尾没被任何一个端点读过
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='min_available_seq'") == "999999"
    assert gen


async def test_a_cached_bound_in_sync_meta_makes_the_pass_refuse_to_run(
        pgdb, pgconn):
    """P6-4 的守门人：谁将来往 sync_meta 里加边界缓存，这里当场炸。

    缓存值必然落后于一次分区 DROP，而落后的下界会让掉窗的游标拿到 200 +
    一段跳过了被裁区间的数据。没有任何一侧会察觉，所以这条不能只是注释。
    """
    await _three_partitions(pgdb, pgconn)
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('retention_floor','123') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")
    with pytest.raises(rt.RetentionInvariantError) as e:
        await pgdb.run_retention_pass()
    assert "retention_floor" in str(e.value)
    assert "scrape_events_p0" in await _partition_names(pgconn), \
        "拒绝执行意味着一个分区都没裁"


# ============================================================
# 6) P6-3：强制裁剪闩锁
# ============================================================

async def test_disk_emergency_prunes_above_ack_and_latches_it(pgdb, pgconn):
    gen = await _three_partitions(pgdb, pgconn, old_days=0.0, new_days=0.0)
    # 全部是新数据 ⇒ 时间下界 = 0 ⇒ 正常路径一个都裁不了
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq','1') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")
    calm = await pgdb.run_retention_pass()
    assert calm["dropped_count"] == 0, calm

    # 磁盘告急：下界被抬到 ack 之上
    pgdb._retention_overrides = {"disk_floor_bytes": 10 ** 18,
                                 "disk_target_bytes": 10 ** 18}
    summary = await pgdb.run_retention_pass()

    assert summary["dropped_count"] == 2
    assert summary["forced_count"] == 2
    assert all(d["forced"] for d in summary["dropped"])
    raw = await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='forced_prune_log'")
    log = json.loads(raw)
    assert len(log) == 2
    first = log[0]
    for key in ("from_seq", "to_seq", "ack_seq_at_time", "ts"):
        assert key in first, f"计划逐字要求记 {key}"
    assert first["from_seq"] == 1 and first["to_seq"] == 3
    assert first["ack_seq_at_time"] == 1
    assert first["ts"].endswith("Z")
    assert log[1]["from_seq"] == P1_LO + 1
    assert gen


async def test_the_forced_prune_latch_survives_a_restart(pgdb, pgconn, monkeypatch):
    """闩锁必须是持久的：消费者宕机正是触发它的前提。

    这里把整个 Database 关掉再用**新实例**连回同一个库 —— 内存里的一切都没了，
    `/status` 还得继续报这条。
    """
    await _three_partitions(pgdb, pgconn, old_days=0.0)
    pgdb._retention_overrides = {"disk_floor_bytes": 10 ** 18,
                                 "disk_target_bytes": 10 ** 18}
    summary = await pgdb.run_retention_pass()
    assert summary["forced_count"] >= 1
    dsn = pgdb.dsn

    from common.pgdb import Database
    monkeypatch.setenv("PG_DSN", dsn)
    fresh = Database()
    await fresh.connect()
    try:
        assert fresh._rt()["last_pass"] is None, "新进程内存里当然什么都没有"
        srv = _srv()
        monkeypatch.setenv("DB_BACKEND", "postgres")
        monkeypatch.setattr(srv, "db", fresh, raising=False)
        transport = httpx.ASGITransport(app=srv.app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://restart") as c:
            b = (await c.get("/api/v1/sync/status")).json()
            r = (await c.get("/api/v1/sync/records",
                             params={"after_seq": P2_LO})).json()
        assert b["retention_forced"] is True
        assert len(b["forced_prune_log"]) == summary["forced_count"]
        assert b["forced_prune_log"][0]["to_seq"] is not None
        assert r["retention_forced"] is True
    finally:
        await fresh.close()


async def test_the_latch_only_clears_when_the_consumer_acks_each_entry(
        client, pgdb, pgconn):
    gen = await _three_partitions(pgdb, pgconn, old_days=0.0)
    pgdb._retention_overrides = {"disk_floor_bytes": 10 ** 18,
                                 "disk_target_bytes": 10 ** 18}
    summary = await pgdb.run_retention_pass()
    assert summary["forced_count"] == 2

    async with client as c:
        b = (await c.get("/api/v1/sync/status")).json()
        assert len(b["forced_prune_log"]) == 2
        ids = [e["id"] for e in b["forced_prune_log"]]

        # 确认第一条：第二条必须还在
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": gen, "prune_ids": [ids[0]]})
        assert r.status_code == 200, r.text
        assert r.json()["acknowledged"] == [ids[0]]
        b = (await c.get("/api/v1/sync/status")).json()
        assert [e["id"] for e in b["forced_prune_log"]] == [ids[1]]
        assert b["retention_forced"] is True

        # 重复确认同一条：不是错误，但也不会凭空多确认一条
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": gen, "prune_ids": [ids[0]]})
        assert r.status_code == 200 and r.json()["acknowledged"] == []
        assert r.json()["unknown_ids"] == []

        # 确认第二条：闩锁清空
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": gen, "prune_ids": [ids[1]]})
        assert r.json()["acknowledged"] == [ids[1]]
        b = (await c.get("/api/v1/sync/status")).json()
        assert b["forced_prune_log"] == [] and b["retention_forced"] is False

    # 条目本身没删，只是打了标记 —— 事后还能复盘裁掉了哪一段
    log = json.loads(await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='forced_prune_log'"))
    assert len(log) == 2 and all(e["acknowledged_at"] for e in log)


async def test_ack_prune_guards(client, pgdb, pgconn):
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [1])
    async with client as c:
        # gen 不符 → 409，与 /ack 同口径
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": "deadbeefcafe", "prune_ids": [1]})
        assert r.status_code == 409 and r.json()["error"] == "gen_mismatch"
        # 形状不对 → 422
        for body in ({}, {"gen": gen}, {"gen": gen, "prune_ids": []},
                     {"gen": gen, "prune_ids": "1"},
                     {"gen": gen, "prune_ids": [None]},
                     {"gen": gen, "prune_ids": [True]},
                     {"gen": "", "prune_ids": [1]},
                     {"gen": gen, "all": "yes"}):
            r = await c.post("/api/v1/sync/ack-prune", json=body)
            assert r.status_code == 422, (body, r.status_code, r.text)
        # 不存在的 id 如实回出去，不当成成功
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": gen, "prune_ids": [4242]})
        assert r.status_code == 200
        assert r.json()["acknowledged"] == [] and r.json()["unknown_ids"] == ["4242"]


# ============================================================
# 7) P6-5：OVERLAP 与 ack 下界的相容性
# ============================================================

async def _overlap_fixture(pgdb, pgconn):
    """消费者已经拉完并确认到 p1 末尾的标准形状。返回 (gen, cursor)。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, list(range(1, 301)), age_days=60)
    await _seed(pgconn, gen, [P1_LO + i for i in range(1, 301)], age_days=60)
    await _seed(pgconn, gen, [P2_LO + 1, P2_LO + 2], age_days=0)
    await _setval(pgconn, P2_LO + 2)
    cursor = P1_LO + 300
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq',$1) "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v", str(cursor))
    return gen, cursor


async def test_control_a_floor_that_ignores_the_window_produces_the_spurious_409(
        client, pgdb, pgconn, monkeypatch):
    """**配对对照组**：Phase 3 verify2 Finding 2 的 arm 1a，原样复现。

    这里把两道防线一起拆掉，退回到「计划字面上那条规则」：
      * ``ack_slack_seq = 0``      —— 下界正好压在 ack_seq 上
      * ``_visible_floor_after``   —— 只看 ``max(seq) <= floor``，不看裁完之后
                                      消费者的窗口落在哪
    于是 p1 被裁掉，一个**完全守规矩**的消费者（从 `cursor - OVERLAP` 回拉）
    当场拿到 409。没有这个对照组，实验组的 200 证明不了任何事。
    """
    gen, cursor = await _overlap_fixture(pgdb, pgconn)
    OVERLAP = rt.CONSUMER_OVERLAP
    monkeypatch.setattr(rt.RetentionMixin, "_visible_floor_after",
                        lambda self, parts, dropping, next_seq: -1)
    pgdb._retention_overrides = {"ack_slack_seq": 0}
    monkeypatch.setattr(rt, "CONSUMER_OVERLAP", 0)

    summary = await pgdb.run_retention_pass()
    assert [d["partition"] for d in summary["dropped"]] == \
        ["scrape_events_p0", "scrape_events_p1"]

    async with client as c:
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": max(0, cursor - OVERLAP),
                                "limit": 500})
    assert r.status_code == 409, r.text
    assert r.json()["error"] == "cursor_below_retention"
    assert gen


async def test_a_conforming_consumer_pulling_from_cursor_minus_overlap_gets_no_409(
        client, pgdb, pgconn):
    """实验组：同一个场景、同一个消费者，200。

    余量刻意调到**最紧的合法值**（正好等于契约的 OVERLAP=200，出厂默认是 1000）：
    松的配置证不了什么 —— 它可能只是「碰巧什么都没裁」。压到下限还能既裁掉
    p0、又让消费者的重叠回拉整段拿回来，这个结论才有力。
    """
    gen, cursor = await _overlap_fixture(pgdb, pgconn)
    OVERLAP = rt.CONSUMER_OVERLAP
    pgdb._retention_overrides = {"ack_slack_seq": OVERLAP}

    summary = await pgdb.run_retention_pass()
    assert [d["partition"] for d in summary["dropped"]] == ["scrape_events_p0"], \
        "p1 必须活着：裁掉它 min(seq) 就跳到 p2，重叠回拉当场掉窗"

    async with client as c:
        s = (await c.get("/api/v1/sync/status")).json()
        assert s["max_safe_overlap"] >= OVERLAP
        r = await c.get("/api/v1/sync/records",
                        params={"after_seq": max(0, cursor - OVERLAP),
                                "limit": 500})
    assert r.status_code == 200, r.text
    assert r.json()["count"] > 0
    assert r.json()["records"][0]["seq"] == cursor - OVERLAP + 1, \
        "重叠回拉必须真的拿回那 200 条，而不是被裁到只剩尾巴"
    assert gen


async def test_no_spurious_409_while_retention_runs_under_the_consumer(
        client, pgdb, pgconn):
    """让保留期与一个守规矩的消费者**并发**跑，一个 409 都不许有。

    ⚠ 前提必须写对，否则这个用例证明不了 P6-5。第一版把消费者的游标放在 0、
    且 ``sync_meta`` 里没有任何 ack —— 那模拟的**不是**守规矩的消费者，而是一个
    从零开始、却指望 60 天前的数据还在的消费者。保留期这时按纯时间下界把 p0/p1
    一起裁掉（完全正确的行为，计划 T9），于是它第一次请求就 409。
    实测（scratchpad/flake.py）复现率约 1/3 —— 取决于 pruner 有没有抢在第一个
    请求之前跑完。

    真正的前提：消费者**已经消费并确认到 p1 中段**，现在带着契约要求的重叠回拉
    继续跑。这一版还顺带把余量压到下限 200，让 `X` 正好落在 p0 被裁之后的
    ``min(seq) - 1`` 上 —— 边界值本身就是被测对象。
    """
    gen = pgdb.event_gen
    OVERLAP = rt.CONSUMER_OVERLAP
    await _seed(pgconn, gen, list(range(1, 401)), age_days=60)
    await _seed(pgconn, gen, [P1_LO + i for i in range(1, 401)], age_days=60)
    await _seed(pgconn, gen, [P2_LO + i for i in range(1, 51)], age_days=0)
    await _setval(pgconn, P2_LO + 50)

    pgdb._retention_overrides = {"ack_slack_seq": OVERLAP}
    start_cursor = P1_LO + OVERLAP          # 已消费到这里，并且确认过
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('ack_seq',$1) "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v", str(start_cursor))

    statuses = []
    stop = asyncio.Event()

    async def consumer():
        """契约 §7 的伪码，逐行照抄：每一**轮**开头回拉一次 OVERLAP，
        轮内用服务端给的 next_after_seq 往前翻页。"""
        cursor = start_cursor
        seen = set()
        async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=_srv().app),
                base_url="http://overlap") as c:
            for _ in range(40):
                x = max(0, cursor - OVERLAP)          # 轮首的重叠回拉
                while True:
                    r = await c.get("/api/v1/sync/records",
                                    params={"after_seq": x, "limit": 1000})
                    statuses.append(r.status_code)
                    if r.status_code != 200:
                        return seen
                    body = r.json()
                    for rec in body["records"]:
                        seen.add(rec["seq"])
                    x = body["next_after_seq"]
                    cursor = x
                    if cursor > 0:
                        await c.post("/api/v1/sync/ack",
                                     json={"gen": gen, "ack_seq": cursor})
                    if not body["has_more"]:
                        break
                if stop.is_set():
                    break
                await asyncio.sleep(0.01)
        return seen

    async def pruner():
        for _ in range(6):
            await pgdb.run_retention_pass()
            await asyncio.sleep(0.02)
        stop.set()

    seen, _ = await asyncio.gather(consumer(), pruner())

    assert set(statuses) == {200}, f"出现了非 200：{sorted(set(statuses))}"
    assert len(statuses) >= 6, "消费者压根没跑几轮，这个用例就没测到并发"
    # 非空虚性：保留期在这段时间里**确实**裁掉了东西。
    # 少了这一条，「一个 409 都没有」可能只是因为保留期什么都没做。
    assert "scrape_events_p0" not in await _partition_names(pgconn), \
        "保留期一个分区都没裁 —— 那这个用例什么也没证明"
    assert "scrape_events_p1" in await _partition_names(pgconn), \
        "p1 必须活着：消费者的重叠窗口还压在它上面"
    # 消费者拿到的必须覆盖库里现存的每一条
    ack = int(await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='ack_seq'"))
    assert ack >= P2_LO + 50
    live = {r["seq"] for r in await pgconn.fetch(
        "SELECT seq FROM scraper.scrape_events")}
    assert live <= seen, f"库里还留着消费者从没见过的行：{sorted(live - seen)[:5]}"


# ============================================================
# 8) P6-6：/status 暴露计划要求的一切
# ============================================================

async def test_status_exposes_every_phase6_field(client, pgdb, pgconn):
    await _three_partitions(pgdb, pgconn)
    async with client as c:
        b = (await c.get("/api/v1/sync/status")).json()

    for k in ("observed_daily_insert_rate", "free_disk_bytes", "retention_forced",
              "forced_prune_log", "lag_records", "dead_letters",
              "max_safe_overlap", "retention"):
        assert k in b, f"/status 少了 Phase 6 要求的 {k}"
    r = b["retention"]
    assert r is not None and "error" not in r
    for k in ("age_floor_seq", "ack_seq", "ack_floor_seq", "ack_slack_seq",
              "effective_floor_seq", "hard_floor_seq", "free_disk_bytes",
              "disk_floor_bytes", "disk_pressure", "retained_partitions",
              "estimated_rows", "partitions", "droppable_now", "next_seq"):
        assert k in r, f"/status.retention 少了 {k}"
    assert r["ack_seq"] is None, "从未 ack 过就是 null，不是 0"
    assert r["ack_floor_seq"] is None
    assert r["effective_floor_seq"] == r["age_floor_seq"]
    assert r["droppable_now"] == ["scrape_events_p0", "scrape_events_p1"]
    assert b["max_safe_overlap"] == r["ack_slack_seq"] >= rt.CONSUMER_OVERLAP
    assert r["disk_pressure"] is False


async def test_status_retention_is_recomputed_every_call(client, pgdb, pgconn):
    """观测值现算：裁一刀，下一次 /status 立刻不一样。"""
    await _three_partitions(pgdb, pgconn)
    async with client as c:
        a = (await c.get("/api/v1/sync/status")).json()["retention"]
        await pgdb.run_retention_pass()
        b = (await c.get("/api/v1/sync/status")).json()["retention"]
    assert a["retained_partitions"] == 3 and b["retained_partitions"] == 1
    assert a["droppable_now"] and b["droppable_now"] == []
    assert b["min_seq"] == P2_LO + 1


# ============================================================
# 9) 运行时纪律
# ============================================================

async def test_retention_yields_to_a_long_reader_instead_of_blocking_it(
        pgdb, pgconn):
    """承重约束 7：DROP 拿不到锁就放弃，绝不排队把后面的读者一起堵住。"""
    await _three_partitions(pgdb, pgconn)
    holder = await asyncpg.connect(dsn=pgdb.dsn)
    try:
        tx = holder.transaction()
        await tx.start()
        await holder.fetchval("SELECT count(*) FROM scraper.scrape_events_p0")

        pgdb._retention_overrides = {"lock_timeout": "200ms"}
        summary = await pgdb.run_retention_pass()

        assert summary["dropped_count"] == 0
        assert summary["stopped_because"] == "lock_timeout"
        assert pgdb._rt()["counters"]["lock_timeouts"] == 1
        assert "scrape_events_p0" in await _partition_names(pgconn)
        await tx.rollback()
    finally:
        await holder.close()

    # 读者走了，下一轮照常裁
    pgdb._retention_overrides = {}
    again = await pgdb.run_retention_pass()
    assert again["dropped_count"] == 2


async def test_two_instances_do_not_prune_at_the_same_time(pgdb, pgconn,
                                                           monkeypatch):
    """库级单例锁：第二个实例安静跳过，不去和第一个抢父表的 ACCESS EXCLUSIVE。"""
    await _three_partitions(pgdb, pgconn)
    other = await asyncpg.connect(dsn=pgdb.dsn)
    try:
        got = await other.fetchval("SELECT pg_try_advisory_lock($1)",
                                   rt.RETENTION_LOCK_KEY)
        assert got is True
        summary = await pgdb.run_retention_pass()
        assert summary == {"skipped": "lock_held_elsewhere"}
        assert "scrape_events_p0" in await _partition_names(pgconn)
    finally:
        await other.execute("SELECT pg_advisory_unlock($1)", rt.RETENTION_LOCK_KEY)
        await other.close()

    assert (await pgdb.run_retention_pass())["dropped_count"] == 2


async def test_maybe_run_retention_honours_the_interval_and_the_kill_switch(
        pgdb, pgconn, monkeypatch):
    await _three_partitions(pgdb, pgconn)

    monkeypatch.setenv("SYNC_RETENTION_ENABLED", "0")
    assert await pgdb.maybe_run_retention() is None
    assert "scrape_events_p0" in await _partition_names(pgconn)

    monkeypatch.setenv("SYNC_RETENTION_ENABLED", "1")
    monkeypatch.setenv("SYNC_RETENTION_INTERVAL_S", "9999")
    first = await pgdb.maybe_run_retention()
    assert first is not None and first["dropped_count"] == 2
    assert await pgdb.maybe_run_retention() is None, "间隔没到就不该再跑"


async def test_row_and_partition_caps_force_a_prune_and_latch_it(pgdb, pgconn):
    """P6-1：驱动量是「剩余磁盘 + 行/分区上限」，不是天数。

    这里所有行都是**新**的（时间下界 = 0，正常路径一个都裁不掉），
    只靠分区数上限把下界抬上去。
    """
    await _three_partitions(pgdb, pgconn, old_days=0.0)
    assert (await pgdb.run_retention_pass())["dropped_count"] == 0

    pgdb._retention_overrides = {"max_retained_partitions": 2}
    summary = await pgdb.run_retention_pass()
    assert summary["dropped_count"] == 1 and summary["forced_count"] == 1
    assert summary["dropped"][0]["reason"] == "partition_cap"
    assert (await pgdb.forced_prune_entries(pgconn))[0]["reason"] == "partition_cap"

    await pgconn.execute("ANALYZE scraper.scrape_events")
    pgdb._retention_overrides = {"max_retained_partitions": 0, "max_event_rows": 4}
    summary = await pgdb.run_retention_pass()
    assert summary["dropped_count"] == 1
    assert summary["dropped"][0]["reason"] == "row_cap"


async def test_the_pass_is_single_flight_within_one_process(pgdb, pgconn):
    await _three_partitions(pgdb, pgconn)
    a, b = await asyncio.gather(pgdb.run_retention_pass(),
                                pgdb.run_retention_pass())
    outcomes = sorted([json.dumps(x, sort_keys=True, default=str) for x in (a, b)])
    assert any("already_running" in o for o in outcomes) or \
        (a.get("dropped_count", 0) + b.get("dropped_count", 0)) == 2, (a, b)
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 3


async def test_a_corrupt_latch_is_carried_along_not_overwritten(pgdb, pgconn):
    """闩锁里的每一条都是一段永久丢失数据的唯一凭据 —— 坏了也不许擦掉。"""
    await _three_partitions(pgdb, pgconn, old_days=0.0)
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('forced_prune_log','{坏掉的') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")
    pgdb._retention_overrides = {"disk_floor_bytes": 10 ** 18,
                                 "disk_target_bytes": 10 ** 18}
    await pgdb.run_retention_pass()

    log = json.loads(await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='forced_prune_log'"))
    assert any(e.get("raw") == "{坏掉的" for e in log), "原文被擦掉了"
    assert any(e.get("reason") == "disk_floor" for e in log), "新条目也要写进去"


async def test_forced_prune_log_cap_never_drops_an_unacknowledged_entry():
    """闩锁满了先丢**已确认**的；未确认的一条都不许丢。"""
    log = []
    for i in range(1, rt.FORCED_PRUNE_LOG_CAP + 51):
        entry = {"id": i, "to_seq": i}
        if i % 2 == 0:
            entry["acknowledged_at"] = "2026-08-05T00:00:00Z"
        log.append(entry)
    trimmed = rt._trim_log(list(log))
    assert len(trimmed) <= rt.FORCED_PRUNE_LOG_CAP
    pending_before = {e["id"] for e in log if not e.get("acknowledged_at")}
    pending_after = {e["id"] for e in trimmed if not e.get("acknowledged_at")}
    assert pending_before == pending_after
    assert [e["id"] for e in trimmed] == sorted(e["id"] for e in trimmed)


async def test_sqlite_backend_503s_on_ack_prune(monkeypatch):
    """新端点必须与另外四个同口径：SQLite 上 503，绝不是 404。"""
    srv = _srv()
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    monkeypatch.setattr(srv, "db", None, raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=srv.app),
                                 base_url="http://sqlite") as c:
        r = await c.post("/api/v1/sync/ack-prune",
                         json={"gen": "x", "prune_ids": [1]})
    assert r.status_code == 503, r.text
    assert r.json()["error"] == "event_stream_unavailable"
    assert os.environ.get("DB_BACKEND") == "sqlite"
