"""tests/pgdb/test_relay.py —— Phase 2 事件流：DDL / 分区 / gen / relay。

这一组的核心是**游标保证**，而且它必须被**证明**，不是被断言：

    对固定的 gen，若消费者持久化 X = 已见过的最大 seq 且总是请求 seq > X，
    则任何在后续请求时刻已提交且 seq > X 的行，必定在该次或之后某次请求中
    被返回，且按 seq 严格递增。绝不跳过已提交行。

所以 test_cursor_guarantee_under_staggered_commits 里有两样东西缺一不可：

  1. **对照组**（test_naive_cursor_skips_committed_rows）先证明这个测试形状
     真的能观察到危险——同样的交错走裸 bigserial 表，消费者会永久跳过一行。
     没有对照组的"没跳过"是个真空断言：也许写入根本没并发。
  2. **非真空断言**：跑完必须存在一对记录 a、b，满足
     ``outbox_id[a] < outbox_id[b]`` 而 ``seq[a] > seq[b]``。
     那正是裸游标会丢掉的那种倒置；它出现了，才说明危险交错真的发生过。

黄金校验对这一整组是**结构性失明**的（4 个后台协程被 no-op、TestClient 严格
顺序执行），跟 Phase 1 看不到 F1/F3 是同一个原因。别指望它兜底。
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from common.pgdb import relay as R

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# ==================================================================
# 0) 纯函数（不碰库，SQLite-only 机器上也会跑）
# ==================================================================

def test_outcome_taxonomy_is_a_closed_set():
    assert R.normalize_outcome("ok") == ("ok", False)
    assert R.normalize_outcome("stale") == ("stale", False)
    # 越界值就地归一化，而不是靠 CHECK 约束——一个脏值不该让整条流停摆
    assert R.normalize_outcome("weird") == ("parse_failed", True)
    assert R.normalize_outcome(None) == ("parse_failed", True)


def test_error_type_mapping():
    assert R.outcome_for_error_type("blocked") == "blocked"
    assert R.outcome_for_error_type("captcha") == "blocked"
    for et in ("parse_error", "variant_offset", "network", "timeout",
               "session_not_ready", "zip_switch_failed", "zip_not_effective",
               "server_reject", "", None, "something_new"):
        assert R.outcome_for_error_type(et) == "parse_failed", et


def test_not_found_is_exact_match_never_a_prefix():
    assert R.classify_success_outcome({"title": R.NOT_FOUND_TITLE}) == "not_found"
    # 审计 §2.11 / 计划 §4.2：绝不能用 startswith("[")
    assert R.classify_success_outcome({"title": "[2-Pack] Storage Bins"}) == "ok"
    assert R.classify_success_outcome({"title": "[页面为空]"}) == "parse_failed"
    assert R.classify_success_outcome({}) == "ok"


def test_marketplace_never_passes_through_parser_site():
    # worker/parser.py:1333 永远发 "site": "US"
    assert R.normalize_marketplace("US") == ("amazon.com", True)
    assert R.normalize_marketplace("amazon.com") == ("amazon.com", False)
    assert R.normalize_marketplace(None) == ("amazon.com", True)


def test_zip_padding_only_restores_lost_leading_zeros():
    assert R.normalize_zip("1001") == ("01001", True)
    assert R.normalize_zip("10001") == ("10001", False)
    assert R.normalize_zip(None) == ("", False)
    # 不认识的形状原样透传：宁可留着，也不要把一个商品的价格序列劈成两组
    assert R.normalize_zip("SW1A 1AA") == ("SW1A 1AA", False)


def test_scrub_nul_is_recursive_and_counts():
    obj = {"a\x00": ["x\x00y", {"b": "\x00\x00"}], "c": 1}
    clean, n = R.scrub_nul(obj)
    assert n == 4
    assert clean == {"a": ["xy", {"b": ""}], "c": 1}
    assert "\x00" not in R.dumps_body(clean)


def test_collected_at_uses_the_worker_utc8_wall_clock():
    fallback = datetime(2000, 1, 1, tzinfo=timezone.utc)
    got, fell_back = R.parse_collected_at("2026-08-05 10:00:00", fallback)
    assert not fell_back
    # worker/parser.py:13 _CN_TZ = UTC+8，且 crawl_time 不带偏移标记
    assert got == datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc)
    assert R.parse_collected_at("", fallback) == (fallback, True)
    assert R.parse_collected_at("not a time", fallback) == (fallback, True)


def test_partition_bound_parsing():
    assert R.parse_partition_bound(
        "FOR VALUES FROM (MINVALUE) TO ('20000000')") == (None, 20000000)
    assert R.parse_partition_bound(
        "FOR VALUES FROM ('20000000') TO ('40000000')") == (20000000, 40000000)


def test_hash_ver_string_to_int():
    # common.slowhash.HASH_VER 是 'v1'，scrape_events.hash_ver 是 int
    assert R.parse_hash_ver("v1") == 1
    assert R.parse_hash_ver(2) == 2
    assert R.parse_hash_ver("garbage") == 1


def test_row_fault_classification_never_quarantines_a_partition_overflow():
    class _NoPart(asyncpg.CheckViolationError):
        pass

    exc = _NoPart('no partition of relation "scrape_events" found for row')
    assert R._is_row_fault(exc) is False           # 分区余量的问题，不是行的问题
    assert R._is_row_fault(asyncpg.NotNullViolationError("x")) is True
    assert R._is_row_fault(asyncpg.InterfaceError("conn gone")) is False
    assert R._is_row_fault(asyncio.TimeoutError()) is False


# ==================================================================
# 1) DDL / 分区
# ==================================================================

@pytest.mark.asyncio
async def test_ddl_shape_and_first_partitions(pgdb, pgconn):
    parts = await pgdb._list_partitions(pgconn)
    names = [p[0] for p in parts]
    assert names[0] == "scrape_events_p0"
    assert parts[0][1] is None                       # MINVALUE
    assert len(parts) >= 1 + R.EVENT_FUTURE_PARTITIONS

    # 每个分区都必须自带 source_id 唯一索引。父表上建不了（PG 16 要求唯一约束
    # 必须包含分区键 seq），LIKE 父表又会静默漏掉它 —— 所以这是硬闸门。
    for name, *_ in parts:
        defs = [r["indexdef"] for r in await pgconn.fetch(
            "SELECT indexdef FROM pg_indexes WHERE schemaname='scraper' "
            "AND tablename=$1", name)]
        assert any("UNIQUE" in d and "source_id" in d for d in defs), (name, defs)
        assert any("recorded_at" in d for d in defs), (name, defs)

    # 计划 §2.1 的 UNIQUE INDEX ON <父表> (source_id) 在 PG 16 上是不合法的
    with pytest.raises(asyncpg.FeatureNotSupportedError):
        await pgconn.execute(
            "CREATE UNIQUE INDEX x_parent_src ON scraper.scrape_events (source_id)")


@pytest.mark.asyncio
async def test_no_default_partition_overflow_fails_loudly_and_loses_nothing(pgdb, pgconn):
    """溢出必须是「吵闹的停摆」，不是「悄悄收下」。"""
    top = max(p[2] for p in await pgdb._list_partitions(pgconn))
    await pgconn.execute(
        "SELECT setval(pg_get_serial_sequence('scraper.scrape_events','seq'), $1)",
        top + 10)

    async with pgdb._write_lock("other"):
        await pgdb._db.execute("BEGIN")
        await pgdb._emit_outbox(outcome="ok", data={"asin": "B0OVERFLOW"})
        await pgdb._db.execute("COMMIT")

    rc = await pgdb._relay_open_conn()
    try:
        with pytest.raises(asyncpg.CheckViolationError) as ei:
            await pgdb._relay_drain_once(rc, 500)
        assert "no partition" in str(ei.value).lower()
        # 事务回滚 -> DELETE 也回滚 -> 行原样留在 outbox。零丢失。
        assert await pgconn.fetchval(
            "SELECT count(*) FROM scraper.scrape_outbox") == 1
        assert await pgconn.fetchval(
            "SELECT count(*) FROM scraper.scrape_events") == 0
        # 而且**不许**被当成毒丸隔离掉
        assert R._is_row_fault(ei.value) is False
    finally:
        await rc.close()


@pytest.mark.asyncio
async def test_partition_rollover_keeps_two_future_partitions(pgdb, pgconn):
    before = await pgdb._list_partitions(pgconn)
    top = max(p[2] for p in before)
    # 把序列推到最高分区的上界之下一点点：未来分区只剩 1 个
    await pgconn.execute(
        "SELECT setval(pg_get_serial_sequence('scraper.scrape_events','seq'), $1)",
        top - R.EVENT_PARTITION_SPAN - 1)

    created = await pgdb.ensure_event_partitions(pgconn)
    assert created >= 1
    after = await pgdb._list_partitions(pgconn)
    assert len(after) > len(before)

    last_value = await pgdb._seq_last_value(pgconn)
    future = [p for p in after if p[1] is not None and p[1] > last_value]
    assert len(future) >= R.EVENT_FUTURE_PARTITIONS

    # 区间必须首尾相接、无缝无叠
    bounds = sorted((p[1] or 0, p[2]) for p in after)
    for (_, hi), (lo2, _) in zip(bounds, bounds[1:]):
        assert hi == lo2, bounds

    # 新分区必须继承 source_id 唯一索引（LIKE 抄父表就会静默漏掉它）
    for name, *_ in after:
        defs = [r["indexdef"] for r in await pgconn.fetch(
            "SELECT indexdef FROM pg_indexes WHERE schemaname='scraper' "
            "AND tablename=$1", name)]
        assert any("UNIQUE" in d and "source_id" in d for d in defs), name


@pytest.mark.asyncio
async def test_attach_partition_does_not_block_an_open_writer(pgdb, pgconn):
    """新分区必须走 create+ATTACH：PARTITION OF 拿 ACCESS EXCLUSIVE 会卡死 relay。"""
    other = await asyncpg.connect(pgdb.dsn)
    try:
        await other.execute("BEGIN")
        await other.execute(
            "INSERT INTO scraper.scrape_outbox (body) VALUES ('{}'::jsonb)")
        # 写事务开着的情况下建分区：必须在 3s 内完成
        await asyncio.wait_for(pgdb.ensure_event_partitions(pgconn, floor_seq=10**7),
                               timeout=3.0)
        await other.execute("ROLLBACK")
    finally:
        await other.close()


# ==================================================================
# 2) gen / instance_id
# ==================================================================

@pytest.mark.asyncio
async def test_gen_is_reused_across_reconnects(pgdb):
    """普通发版**不得**改 gen —— 契约 §5.5 把 gen 变化定义成消费侧硬停 + 全量对账。"""
    from common.pgdb import Database

    gen = pgdb.event_gen
    assert gen and len(gen) == 12

    db2 = Database()               # pgdb 夹具已经把 PG_DSN 指到这个临时库
    await db2.connect()
    try:
        assert db2.event_gen == gen
    finally:
        await db2.close()


@pytest.mark.asyncio
async def test_rewind_mints_a_new_gen_and_pushes_the_sequence_forward(pgdb, pgconn):
    """T11(a)：只回滚 DB -> 启动检出回退 -> 铸新 gen。"""
    old_gen = pgdb.event_gen
    # 假装曾经发到过 seq=500000，然后库被恢复回了空
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('max_seq_ever','500000') "
        "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v")

    await pgdb._bootstrap_identity(pgconn)

    assert pgdb.event_gen != old_gen
    assert await pgconn.fetchval(
        "SELECT v FROM scraper.sync_meta WHERE k='gen'") == pgdb.event_gen
    # 序列被推到已知最高水位之后，实例内 seq 保持单调
    assert await pgdb._seq_last_value(pgconn) >= 500000


@pytest.mark.asyncio
async def test_gen_is_stored_on_every_row_not_just_in_meta(pgdb, pgconn):
    """只存 meta 表的话，一次从备份恢复会把全部历史重贴上恢复后的标签。"""
    async with pgdb._write_lock("other"):
        await pgdb._db.execute("BEGIN")
        await pgdb._emit_outbox(outcome="ok", data={"asin": "B0GEN"})
        await pgdb._db.execute("COMMIT")
    rc = await pgdb._relay_open_conn()
    try:
        await pgdb._relay_drain_once(rc, 500)
    finally:
        await rc.close()
    row = await pgconn.fetchrow("SELECT gen, source_id FROM scraper.scrape_events")
    assert row["gen"] == pgdb.event_gen
    assert row["source_id"].startswith(pgdb.event_gen + ":")


# ==================================================================
# 3) 写钩子的事务归属
# ==================================================================

@pytest.mark.asyncio
async def test_emit_joins_the_callers_transaction_and_rolls_back_with_it(pgdb, pgconn):
    async with pgdb._write_lock("other"):
        await pgdb._db.execute("BEGIN")
        oid = await pgdb._emit_outbox(outcome="ok", data={"asin": "B0ROLLBACK"})
        assert oid is not None
        await pgdb._db.execute("ROLLBACK")
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 0


@pytest.mark.asyncio
async def test_emit_scrubs_nul_bytes_that_jsonb_cannot_represent(pgdb, pgconn):
    """jsonb 连**正确转义过**的 \\u0000 都拒收（22P05），与 text 列的 22021 还不是同一个。"""
    async with pgdb._write_lock("other"):
        await pgdb._db.execute("BEGIN")
        await pgdb._emit_outbox(outcome="ok",
                                data={"asin": "B0NUL", "title": "bad\x00title"})
        await pgdb._db.execute("COMMIT")
    body = json.loads(await pgconn.fetchval("SELECT body::text FROM scraper.scrape_outbox"))
    assert body["result"]["title"] == "badtitle"
    assert pgdb.event_relay_metrics()["counters"]["emit_nul_stripped"] == 1


# ==================================================================
# 4) 单例（T5）
# ==================================================================

@pytest.mark.asyncio
async def test_second_relay_refuses_to_start(pgdb):
    from common.pgdb import Database

    assert await pgdb.start_event_relay() is True
    db2 = Database()
    await db2.connect()
    try:
        assert await db2.start_event_relay() is False
        assert db2.event_relay_metrics()["relay_state"] == "refused"
        # 第二个进程不许留下任何后台任务
        assert db2._ev()["relay_task"] is None

        # 第一个让位之后，第二个就能接管——滚动部署要靠这条
        await pgdb.stop_event_relay()
        assert await db2.start_event_relay() is True
        await db2.stop_event_relay()
    finally:
        await db2.close()
        await pgdb.stop_event_relay()


@pytest.mark.asyncio
async def test_relay_bootstrap_does_not_ride_on_the_shared_write_transaction(pgdb):
    """relay 的引导 DDL 必须跑在**它自己的连接**上。

    relay 是 lifespan 里 ``create_task`` 起来的，它第一次真正执行的时刻，服务
    很可能已经在处理请求了 —— 也就是 ``_write_conn`` 上正开着别人的事务。
    在那条**裸** asyncpg 连接上发 DDL 有两种死法：``InterfaceError: another
    operation is in progress``，或者更坏 —— DDL 悄悄进了别人的事务，跟着别人
    一起回滚，于是"表建过了"这件事在下一次写钩子触发时才被发现。

    这里把那个前置状态造出来：外人开着事务，此时启动 relay，然后让外人**回滚**。
    事件流的表必须活下来。
    """
    verify = await asyncpg.connect(pgdb.dsn)
    try:
        await verify.execute("DROP SCHEMA scraper CASCADE")
        pgdb._ev()["ready"] = False

        holding = asyncio.Event()
        release = asyncio.Event()

        async def hold_a_foreign_transaction():
            async with pgdb._write_lock("other"):
                await pgdb._db.execute("BEGIN")
                await pgdb._db.execute("CREATE TABLE tmp_holder_marker (x int)")
                holding.set()
                await release.wait()
                await pgdb._db.execute("ROLLBACK")

        holder = asyncio.create_task(hold_a_foreign_transaction())
        await asyncio.wait_for(holding.wait(), timeout=5.0)
        try:
            assert await asyncio.wait_for(pgdb.start_event_relay(), timeout=10.0) is True
        finally:
            release.set()
            await holder
            await pgdb.stop_event_relay()

        # 外人的事务整个回滚了：他的表没了，事件流的表还在 = DDL 独立提交过
        assert await verify.fetchval("SELECT to_regclass('tmp_holder_marker')") is None
        for table in R.EVENT_EXPECTED_COLUMNS:
            assert await verify.fetchval(
                "SELECT to_regclass($1)", f"scraper.{table}") is not None, table
        assert await verify.fetchval(
            "SELECT v FROM scraper.sync_meta WHERE k = 'gen'") == pgdb.event_gen
    finally:
        await verify.close()


# ==================================================================
# 5) 游标保证（本阶段的核心）
# ==================================================================

async def _emit_direct(conn, source_id, gen, asin, marketplace="amazon.com"):
    """绕过 _emit_outbox 直接插 outbox：测试要自己控制事务的开与提交时机。"""
    body = R.dumps_body({
        "v": 1, "source_id": source_id, "gen": gen, "outcome": "ok",
        "asin": asin, "marketplace": marketplace, "zip_requested": "10001",
        "result": {"asin": asin, "crawl_time": "2026-08-05 10:00:00"},
    })
    return await conn.fetchval(
        "INSERT INTO scraper.scrape_outbox (body) VALUES ($1::jsonb) RETURNING id",
        body)


@pytest.mark.asyncio
async def test_naive_cursor_skips_committed_rows(pgdb, pgconn):
    """**对照组**：证明这个测试形状真的能观察到危险。

    没有它，下面那个 "relay 没跳过" 就是个真空断言。
    """
    await pgconn.execute("CREATE TABLE naive (seq bigserial PRIMARY KEY, tag text)")
    a = await asyncpg.connect(pgdb.dsn)
    b = await asyncpg.connect(pgdb.dsn)
    try:
        await a.execute("BEGIN")
        await b.execute("BEGIN")
        sa = await a.fetchval("INSERT INTO naive (tag) VALUES ('A') RETURNING seq")
        sb = await b.fetchval("INSERT INTO naive (tag) VALUES ('B') RETURNING seq")
        assert sa < sb
        await b.execute("COMMIT")                     # B 后拿到序列号，却先提交

        seen = await pgconn.fetch("SELECT seq, tag FROM naive WHERE seq > 0 ORDER BY seq")
        cursor = max(r["seq"] for r in seen)
        assert [r["tag"] for r in seen] == ["B"]
        assert cursor == sb

        await a.execute("COMMIT")                     # A 现在也提交了
        after = await pgconn.fetch(
            "SELECT seq, tag FROM naive WHERE seq > $1 ORDER BY seq", cursor)

        assert await pgconn.fetchval("SELECT count(*) FROM naive WHERE tag='A'") == 1
        assert after == []          # A 已提交，却被游标永久跳过 —— 这就是要杀掉的东西
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_cursor_guarantee_under_staggered_commits(pgdb, pgconn, monkeypatch):
    """N 个并发写入者 + 刻意反转的提交顺序 + 一个 seq > X 的消费者。

    断言（三条缺一不可）：
      1. 消费者每次拿到的 seq 都严格递增且 > 上次游标；
      2. 抽干之后消费者见过的 source_id 集合 == 写入的全集（一条不漏）；
      3. **非真空**：存在 outbox_id[a] < outbox_id[b] 而 seq[a] > seq[b]，
         也就是危险交错真的发生了，只不过被 outbox 接住了。
    """
    monkeypatch.setattr(R, "RELAY_TICK_SECONDS", 0.02)
    monkeypatch.setattr(R, "RELAY_BATCH", 50)

    n_writers, per_writer = 8, 12
    gen = pgdb.event_gen
    conns = [await asyncpg.connect(pgdb.dsn) for _ in range(n_writers)]
    gates = [asyncio.Event() for _ in range(n_writers + 1)]
    inserted_all = asyncio.Event()
    outbox_id = {}
    n_done = 0
    lock = asyncio.Lock()

    async def writer(k: int):
        nonlocal n_done
        conn = conns[k]
        await gates[k].wait()                 # 串行化「插入」阶段 -> id 顺序 = k 顺序
        await conn.execute("BEGIN")
        for j in range(per_writer):
            sid = f"{gen}:w{k}-{j}"
            outbox_id[sid] = await _emit_direct(conn, sid, gen, f"B0W{k:02d}{j:02d}")
        gates[k + 1].set()
        async with lock:
            n_done += 1
            if n_done == n_writers:
                inserted_all.set()
        await inserted_all.wait()
        # 提交顺序与 id 顺序**完全相反**：writer 0 的 id 最小、提交最晚
        await asyncio.sleep(0.04 * (n_writers - k))
        await conn.execute("COMMIT")

    seen_seq = []
    seen_src = {}
    stop = asyncio.Event()

    # 消费者**只记录，不断言**。断言全部挪到 teardown 之后：一个在后台任务里
    # 抛出的 AssertionError 会让 finally 里的 `await cons` 直接炸掉，于是
    # relay 任务泄漏、事件循环关不掉，一个本该是"红"的用例变成"挂住"。
    # 实测踩过：这个文件曾经 1/6 概率卡死，根因就是这里。
    async def consumer():
        c = await asyncpg.connect(pgdb.dsn)
        cursor = 0
        try:
            while not stop.is_set():
                rows = await c.fetch(
                    "SELECT seq, source_id FROM scraper.scrape_events "
                    "WHERE seq > $1 ORDER BY seq LIMIT 50", cursor)
                for r in rows:
                    polls.append((cursor, r["seq"], r["source_id"]))
                    seen_seq.append(r["seq"])
                    seen_src[r["source_id"]] = r["seq"]
                    cursor = r["seq"]
                await asyncio.sleep(0.01)
        finally:
            await c.close()

    polls = []
    assert await pgdb.start_event_relay() is True
    cons = asyncio.create_task(consumer())
    try:
        gates[0].set()
        await asyncio.gather(*(writer(k) for k in range(n_writers)))
        # 等 outbox 抽干 + 消费者追上
        for _ in range(400):
            depth = await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox")
            if depth == 0 and len(seen_src) == n_writers * per_writer:
                break
            await asyncio.sleep(0.05)
    finally:
        stop.set()
        # 每一步都必须跑到，哪怕上一步炸了——漏掉 stop_event_relay 就是泄漏一个
        # 后台任务 + 一条连接，下一个用例会以"莫名其妙的挂起"形式收账。
        for coro in (cons, pgdb.stop_event_relay(), *(c.close() for c in conns)):
            try:
                await coro
            except Exception:                                  # noqa: BLE001, PERF203
                pass

    total = n_writers * per_writer
    assert len(outbox_id) == total
    # (2) 一条不漏
    assert set(seen_src) == set(outbox_id), (
        f"漏了 {set(outbox_id) - set(seen_src)}，多了 {set(seen_src) - set(outbox_id)}")
    # (1) 每一次返回都严格递增、且都 > 请求时的游标
    bad = [p for p in polls if not p[1] > p[0]]
    assert not bad, f"返回了 <= after_seq 的行: {bad[:5]}"
    assert seen_seq == sorted(seen_seq) and len(set(seen_seq)) == len(seen_seq), (
        f"seq 不是严格递增：{seen_seq[:20]} ...")

    # (3) 非真空：确实发生了「先拿到号、后提交」的倒置
    inversions = [
        (a, b) for a in outbox_id for b in outbox_id
        if outbox_id[a] < outbox_id[b] and seen_src[a] > seen_src[b]
    ]
    assert inversions, (
        f"没有观察到任何 outbox_id/seq 倒置（共 {total} 行，seq "
        f"{min(seen_seq)}..{max(seen_seq)}）——说明危险的并发交错根本没发生，"
        f"这个用例是真空的，别信它的绿灯。实测正常应有数千对倒置。")


@pytest.mark.asyncio
async def test_two_relays_would_break_the_guarantee(pgdb, pgconn):
    """单例锁是**承重**的，不是装饰。

    上面那个用例证明了「一个 relay 时不跳行」。这个用例证明反面：只要有第二个
    写入者，同一个游标就会永久跳过已提交行 —— 也就是 pg_try_advisory_lock 拦下
    的到底是什么。两条 relay 事务手工驱动，完全确定，不靠时序运气。
    """
    gen = pgdb.event_gen
    await _emit_direct(pgconn, f"{gen}:A", gen, "B0AAA")      # outbox id 小
    await _emit_direct(pgconn, f"{gen}:B", gen, "B0BBB")      # outbox id 大

    r1 = await pgdb._relay_open_conn()
    r2 = await pgdb._relay_open_conn()
    try:
        await r1.execute("BEGIN")
        rows1 = await r1.fetch(R.SQL_CLAIM, 1)                # 认领 A
        await r2.execute("BEGIN")
        rows2 = await r2.fetch(R.SQL_CLAIM, 1)                # 认领 B（SKIP LOCKED 跳过 A）
        assert rows1[0]["id"] < rows2[0]["id"]

        ev1 = [pgdb._build_event_row(r) for r in rows1]
        ev2 = [pgdb._build_event_row(r) for r in rows2]
        seq_a = (await pgdb._relay_write_events(r1, ev1))[0]["seq"]
        seq_b = (await pgdb._relay_write_events(r2, ev2))[0]["seq"]
        assert seq_a < seq_b                                  # A 先拿到号

        await r2.execute("COMMIT")                            # 却是 B 先提交
        seen = await pgconn.fetch(
            "SELECT seq, source_id FROM scraper.scrape_events WHERE seq > 0 ORDER BY seq")
        cursor = max(r["seq"] for r in seen)
        assert [r["source_id"] for r in seen] == [f"{gen}:B"]

        await r1.execute("COMMIT")                            # A 现在也提交了
        after = await pgconn.fetch(
            "SELECT seq, source_id FROM scraper.scrape_events WHERE seq > $1 ORDER BY seq",
            cursor)
        assert after == [], "两个写入者下居然没跳行？那这个用例失去意义了"
        assert await pgconn.fetchval(
            "SELECT count(*) FROM scraper.scrape_events WHERE source_id = $1",
            f"{gen}:A") == 1                                  # A 在库里，却永远拉不到
    finally:
        await r1.close()
        await r2.close()


# ==================================================================
# 6) relay 崩在事务中途（T4 / T7）
# ==================================================================

@pytest.mark.asyncio
async def test_relay_failure_mid_transaction_loses_nothing(pgdb, pgconn):
    gen = pgdb.event_gen
    for i in range(5):
        await _emit_direct(pgconn, f"{gen}:crash-{i}", gen, f"B0CRASH{i}")

    async def boom(self, conn, rows):
        raise RuntimeError("boom inside the relay transaction")

    rc = await pgdb._relay_open_conn()
    try:
        orig = R.EventStreamMixin._relay_write_events
        R.EventStreamMixin._relay_write_events = boom
        try:
            with pytest.raises(RuntimeError):
                await pgdb._relay_drain_once(rc, 500)
        finally:
            R.EventStreamMixin._relay_write_events = orig

        # DELETE 与 INSERT 同事务 -> 一起回滚 -> 认领的 5 行原样留在 outbox
        assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 5
        assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 0
        # 连接没有被留在事务里
        assert not rc.is_in_transaction()

        # 恢复之后一次抽干，恰好 5 条、零重复
        assert await pgdb._relay_drain_once(rc, 500) == 5
    finally:
        await rc.close()

    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 5
    assert await pgconn.fetchval(
        "SELECT count(DISTINCT source_id) FROM scraper.scrape_events") == 5
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 0


@pytest.mark.asyncio
async def test_relay_cancelled_mid_transaction_then_restarted(pgdb, pgconn, monkeypatch):
    """真·中途杀死：取消正卡在事务里的 relay 任务，再拉一个新的起来。"""
    monkeypatch.setattr(R, "RELAY_TICK_SECONDS", 0.02)
    gen = pgdb.event_gen
    for i in range(7):
        await _emit_direct(pgconn, f"{gen}:kill-{i}", gen, f"B0KILL{i}")

    entered = asyncio.Event()

    async def hang(self, conn, rows):
        entered.set()
        await asyncio.sleep(30)               # 抱着事务不放，等着被取消

    orig = R.EventStreamMixin._relay_write_events
    R.EventStreamMixin._relay_write_events = hang
    try:
        assert await pgdb.start_event_relay() is True
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        await pgdb.stop_event_relay()          # 在事务中途取消
    finally:
        R.EventStreamMixin._relay_write_events = orig

    # 零丢失：7 行全部还在 outbox，事件表还是空的
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 7
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 0
    # 也没有留下 idle-in-transaction 的僵尸会话
    assert await pgconn.fetchval(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND state = 'idle in transaction'") == 0

    assert await pgdb.start_event_relay() is True
    try:
        for _ in range(200):
            if await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 0:
                break
            await asyncio.sleep(0.02)
    finally:
        await pgdb.stop_event_relay()

    rows = await pgconn.fetch("SELECT seq, source_id FROM scraper.scrape_events ORDER BY seq")
    assert len(rows) == 7                                   # 零丢失
    assert len({r["source_id"] for r in rows}) == 7         # 零重复
    assert [r["seq"] for r in rows] == sorted(r["seq"] for r in rows)


@pytest.mark.asyncio
async def test_duplicate_source_id_is_swallowed_not_raised(pgdb, pgconn):
    """幂等锚点：同一条记录被写第二遍时，无目标 ON CONFLICT DO NOTHING 吃掉它。

    ``ON CONFLICT (source_id)`` 推断不出来（唯一索引在分区上），会直接
    InvalidColumnReferenceError —— 所以只能用无目标形式。
    """
    gen = pgdb.event_gen
    await _emit_direct(pgconn, f"{gen}:dup", gen, "B0DUP")
    rc = await pgdb._relay_open_conn()
    try:
        assert await pgdb._relay_drain_once(rc, 500) == 1
        await _emit_direct(pgconn, f"{gen}:dup", gen, "B0DUP")      # 同一个 source_id
        assert await pgdb._relay_drain_once(rc, 500) == 1           # 认领了 1 行
    finally:
        await rc.close()
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 1
    assert pgdb.event_relay_metrics()["counters"]["conflicts"] == 1

    with pytest.raises(asyncpg.InvalidColumnReferenceError):
        await pgconn.execute(
            "INSERT INTO scraper.scrape_events "
            "(source_id,gen,asin,marketplace,zip_requested,zip_verify,"
            " collected_at,recorded_at,outcome,payload) "
            "VALUES ('x','g','a','amazon.com','1','unverified',now(),now(),'ok','{}') "
            "ON CONFLICT (source_id) DO NOTHING")


# ==================================================================
# 7) 毒丸不许把整条流焊死
# ==================================================================

@pytest.mark.asyncio
async def test_a_poison_row_is_quarantined_and_the_stream_resumes(pgdb, pgconn,
                                                                  monkeypatch):
    monkeypatch.setattr(R, "RELAY_TICK_SECONDS", 0.01)
    monkeypatch.setattr(R, "RELAY_ERROR_BACKOFF", 0.01)
    monkeypatch.setattr(R, "RELAY_QUARANTINE_AFTER", 2)

    gen = pgdb.event_gen
    poison_id = await _emit_direct(pgconn, f"{gen}:poison", gen, "B0POISON")
    good = [await _emit_direct(pgconn, f"{gen}:good-{i}", gen, f"B0GOOD{i}")
            for i in range(3)]
    assert poison_id < min(good)                 # 毒丸排在队头

    orig = R.EventStreamMixin._build_event_row

    def build(self, claimed):
        row = orig(self, claimed)
        if row["source_id"].endswith(":poison"):
            raise ValueError("deliberately unbuildable row")
        return row

    R.EventStreamMixin._build_event_row = build
    try:
        assert await pgdb.start_event_relay() is True
        for _ in range(600):
            if await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 0:
                break
            await asyncio.sleep(0.02)
        await pgdb.stop_event_relay()
    finally:
        R.EventStreamMixin._build_event_row = orig

    # 毒丸被原封不动搬进死信表（不是丢弃），三条好行照常进流
    dead = await pgconn.fetch("SELECT id, body FROM scraper.scrape_outbox_dead")
    assert [r["id"] for r in dead] == [poison_id]
    assert json.loads(dead[0]["body"])["source_id"] == f"{gen}:poison"
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_events") == 3
    assert await pgconn.fetchval("SELECT count(*) FROM scraper.scrape_outbox") == 0
    assert pgdb.event_relay_metrics()["counters"]["quarantined"] == 1


# ==================================================================
# 8) 指标
# ==================================================================

@pytest.mark.asyncio
async def test_metrics_expose_depth_lag_and_rate(pgdb, pgconn):
    gen = pgdb.event_gen
    old = datetime.now(timezone.utc) - timedelta(seconds=30)
    for i in range(4):
        await _emit_direct(pgconn, f"{gen}:m-{i}", gen, f"B0M{i}")
    await pgconn.execute("UPDATE scraper.scrape_outbox SET enqueued_at = $1", old)

    stats = await pgdb.event_stream_stats()
    assert stats["outbox_depth"] == 4
    assert stats["relay_lag_s"] >= 29
    assert stats["max_seq"] == 0 and stats["min_available_seq"] == 0
    assert stats["future_partitions"] >= R.EVENT_FUTURE_PARTITIONS
    assert stats["contract_version"] == R.EVENT_CONTRACT_VERSION
    assert stats["sync_meta"]["gen"] == gen
    assert stats["dead_letters"] == 0

    rc = await pgdb._relay_open_conn()
    try:
        await pgdb._relay_drain_once(rc, 500)
    finally:
        await rc.close()

    m = pgdb.event_relay_metrics()
    assert m["counters"]["relayed"] == 4
    assert m["events_per_minute"] > 0
    assert m["hash_backend"] in (None, "common.slowhash")

    stats = await pgdb.event_stream_stats()
    assert stats["outbox_depth"] == 0
    assert stats["max_seq"] == 4 and stats["min_available_seq"] == 1
    assert stats["sync_meta"]["max_seq_ever"] == "4"
