"""tests/pgdb/test_phase4_fields.py —— Phase 4 的采集质量信号接进事件流。

**为什么这个文件必须存在（读之前先读这一段）**

黄金样本对本文件的每一条断言都是**结构性失明**的，三个独立的原因：

  1. `crawl_time` 和一切时间戳键都在 `tests/golden/harness.py:34` 的
     `_VOLATILE_KEYS` 里，值被换成 `<VOLATILE>` —— 裸 UTC+8 改成带时区 UTC
     在黄金侧**逐字节不可见**。
  2. 黄金夹具喂给 server 的是 `tests/golden/scenario.py` 里合成的 result dict，
     它从不 import `worker.parser`（Phase 2 用 MetaPathFinder 实测过），
     更不会走 404 分支 —— 本文件测的那些 `_` 键在黄金语料里根本不存在。
  3. 事件流的四张表**一个字节都不在基线里**：基线比的是 HTTP 响应体，
     而 `scraper.scrape_events` 只有 `/api/v1/sync/*` 读得到，那几个端点
     黄金一步都没走。

所以「64/64 仍然绿」对下面任何一条断言都是零信息。证据只能自己带：真库、
真 relay、真 worker 提交体。

覆盖：
  A. 四个信号的归一化纯函数（值域 + 只降不升）
  B. crawl_time 的两种线格式（P4-7 的**混合机队**：同一批里新旧各一半）
  C. 端到端：emit -> relay -> scrape_events 的列值
  D. 404 分支：outcome=not_found、两个哈希为 NULL、目录层不被覆盖
     （新老两种提交体各一组）
  E. zip_requested 的三级仲裁
  F. 跨模块值域对表（schema 常量 ↔ worker.parser ↔ 契约文档）
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone

import pytest

from common.pgdb import relay as R
from common.pgdb import schema as S
from common.pgdb.results_write import NOT_FOUND_PRESERVED_FIELDS

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PARSER_PATH = os.path.join(_REPO_ROOT, "worker", "parser.py")
_parser_cache = []


def real_parser_module():
    """真的 `worker/parser.py`，**绕开 sys.modules**。

    ⚠ 不能直接 `import worker.parser`：`tests/test_session_slot.py` 在**模块级**
    往 sys.modules 里塞了 `worker.parser = <stub AmazonParser=object>`，而 pytest
    在**收集期**就 import 每个测试文件 —— 那个桩在本文件跑的时候是装着的
    （它的 `tearDownModule` 要等它自己的用例跑完才还原，而 tests/pgdb/ 排在
    tests/test_session_slot.py 前面）。实测症状：
        AttributeError: 'object' object has no attribute '_default_result'
    按文件路径独立加载一份，跑单文件与跑全量得到的是同一个对象，
    收集顺序不再能改变断言结果。**不注册进 sys.modules**，免得反过来踩到别人。
    """
    if not _parser_cache:
        spec = importlib.util.spec_from_file_location(
            "_phase4_real_worker_parser", _PARSER_PATH)
        module = importlib.util.module_from_spec(spec)
        if _REPO_ROOT not in sys.path:            # 它自己 import worker.ziputil
            sys.path.insert(0, _REPO_ROOT)
        spec.loader.exec_module(module)
        _parser_cache.append(module)
    return _parser_cache[0]


# ==================================================================
# A) 归一化纯函数
# ==================================================================

def test_zip_verify_domain_and_default():
    for v in S.EVENT_ZIP_VERIFY_VALUES:
        assert R.normalize_zip_verify(v) == (v, False)
    # 老 worker 根本没有这个键：那是"没测过"，不是脏值 —— 不计 coerced，
    # 否则灰度期计数器会被老 worker 刷满，真正的脏值反而看不见
    assert R.normalize_zip_verify(None) == ("unverified", False)
    for junk in ("", " ", "yes", 7, True, [], {}, "CONFIRMED"):
        value, coerced = R.normalize_zip_verify(junk)
        assert (value, coerced) == ("unverified", True), junk


def test_zip_verify_is_never_upgraded_only_downgraded():
    # 自相矛盾的 confirmed（观测值不是请求值）-> mismatch
    assert R.reconcile_zip_verify("confirmed", "10001", "90210") == ("mismatch", True)
    # 自洽的 confirmed 原样
    assert R.reconcile_zip_verify("confirmed", "10001", "10001") == ("confirmed", False)
    # 没有观测值时不动它（worker 可能是靠带外证据判的）
    assert R.reconcile_zip_verify("confirmed", "10001", None) == ("confirmed", False)
    # **绝不**反向加强：observed == requested 却写着 mismatch，也留着
    assert R.reconcile_zip_verify("mismatch", "10001", "10001") == ("mismatch", False)
    assert R.reconcile_zip_verify("unverified", "10001", "10001") == ("unverified", False)


def test_zip_observed_pads_exactly_like_zip_requested():
    """两个值会被直接比较，补零规则漂了就会凭空造出 mismatch。"""
    assert R.normalize_zip_observed("1001") == ("01001", True)
    assert R.normalize_zip_observed("1001")[0] == R.normalize_zip("1001")[0]
    assert R.normalize_zip_observed("10001") == ("10001", False)
    # None / 空 = 页面没挂 glow 挂件，合法且常见，不算改写
    assert R.normalize_zip_observed(None) == (None, False)
    assert R.normalize_zip_observed("   ") == (None, False)
    for junk in (True, ["10001"], {"z": 1}):
        assert R.normalize_zip_observed(junk) == (None, True), junk


def test_completeness_is_a_four_bit_map_and_bool_is_rejected():
    for v in range(16):
        assert R.normalize_completeness(v) == (v, False)
    assert R.normalize_completeness(None) == (0, False)
    # True 是 int 的子类；放行会变成 bit0=1 = "面包屑区块存在"
    assert R.normalize_completeness(True) == (0, True)
    assert R.normalize_completeness(False) == (0, True)
    for junk in (-1, 16, 999, "abc", [], {}):
        assert R.normalize_completeness(junk) == (0, True), junk
    # 数字字符串（jsonb 里出现过）照收
    assert R.normalize_completeness("15") == (15, False)


def test_parse_engine_is_a_closed_set_and_unknown_becomes_null():
    assert R.normalize_parse_engine("selectolax") == ("selectolax", False)
    assert R.normalize_parse_engine("lxml") == ("lxml", False)
    assert R.normalize_parse_engine(None) == (None, False)
    for junk in ("Selectolax", "beautifulsoup", "", 3, True):
        assert R.normalize_parse_engine(junk) == (None, True), junk


def test_outcome_reconcile_only_goes_ok_to_not_found():
    nf = {"_outcome": "not_found"}
    assert R.reconcile_outcome("ok", nf) == ("not_found", True)
    assert R.reconcile_outcome("ok", {"title": R.NOT_FOUND_TITLE}) == ("not_found", True)
    assert R.reconcile_outcome("ok", {"title": "[2-Pack] Bins"}) == ("ok", False)
    # 服务端事实不许被 payload 推翻：stale 说的是"提交到得太晚"，
    # blocked / parse_failed 是服务端自己的判定
    for server_side in ("stale", "blocked", "parse_failed", "not_found"):
        assert R.reconcile_outcome(server_side, nf) == (server_side, False)


def test_classify_reads_the_declared_outcome_but_the_sentinel_title_wins():
    assert R.classify_success_outcome({"_outcome": "not_found"}) == "not_found"
    assert R.classify_success_outcome({"_outcome": "ok"}) == "ok"
    # worker 只被授权判这两个；集外值忽略，交回服务端判定
    for junk in ("stale", "blocked", "", None, 3, True):
        assert R.classify_success_outcome({"_outcome": junk}) == "ok", junk
    # 页面级证据（哨兵标题）比自述强
    assert R.classify_success_outcome(
        {"title": R.NOT_FOUND_TITLE, "_outcome": "ok"}) == "not_found"
    assert R.classify_success_outcome(
        {"title": "[页面为空]", "_outcome": "ok"}) == "parse_failed"
    # 老 worker 的提交体（既没有 `_outcome`，也不是哨兵）照旧是 ok
    assert R.classify_success_outcome({"title": "Anker PowerCore"}) == "ok"
    assert R.classify_success_outcome({}) == "ok"


# ==================================================================
# B) crawl_time：两种线格式（P4-7 混合机队）
# ==================================================================

def test_both_crawl_time_wire_formats_land_on_the_same_instant():
    """老 worker 的裸 UTC+8 与新 worker 的 RFC3339 UTC 指的是同一时刻。

    这是混合机队的核心断言：一次 worker 灰度期间两种格式同时在线，
    两边算出来的 collected_at 必须**逐微秒相等**，否则同一个商品的时间序列
    会在 8 小时的台阶上来回跳。
    """
    same = datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc)
    assert R.parse_crawl_time("2026-08-05 10:00:00") == (same, R.CRAWL_TIME_LEGACY_CST)
    assert R.parse_crawl_time("2026-08-05T02:00:00Z") == (same, R.CRAWL_TIME_TZ_AWARE)
    assert R.parse_crawl_time("2026-08-05T02:00:00+00:00") == (same, R.CRAWL_TIME_TZ_AWARE)
    # 别的偏移也照收（worker 换机房/换镜像时可能出现）
    assert R.parse_crawl_time("2026-08-05T10:00:00+08:00") == (same, R.CRAWL_TIME_TZ_AWARE)


def test_the_dangerous_shape_is_the_one_p4_7_did_not_choose():
    """`'2026-08-05 02:00:00+00:00'` 这种写法为什么被 P4-7 否决。

    老口径的解析是 `strptime(s[:19], '%Y-%m-%d %H:%M:%S')`，而这个字符串的前 19
    个字符恰好是合法的老格式 —— 没改造的消费者会**静默**解析成功再贴上 +08:00，
    错 8 小时且无声。选 'T' + 'Z' 之后同样的切片必然 ValueError，
    消费者走自己的兜底分支并留下计数。
    """
    dangerous = "2026-08-05 02:00:00+00:00"
    naive = datetime.strptime(dangerous[:19], "%Y-%m-%d %H:%M:%S")
    assert naive == datetime(2026, 8, 5, 2, 0), "老式切片对危险形状会成功"
    with pytest.raises(ValueError):
        datetime.strptime("2026-08-05T02:00:00Z"[:19], "%Y-%m-%d %H:%M:%S")
    # 而 relay 两种都认得，且都落在同一个正确的时刻上
    assert R.parse_crawl_time(dangerous)[0] == datetime(
        2026, 8, 5, 2, 0, tzinfo=timezone.utc)


def test_crawl_time_edge_shapes():
    # 带小数秒的老格式（旧实现靠 s[:19] 前缀切法救回来的那一类）
    got, form = R.parse_crawl_time("2026-08-05 10:00:00.123456")
    assert form == R.CRAWL_TIME_LEGACY_CST
    assert got == datetime(2026, 8, 5, 2, 0, 0, 123456, tzinfo=timezone.utc)
    # 裸 ISO（丢了标记的新一代形状）按 UTC，不按 +08:00
    assert R.parse_crawl_time("2026-08-05T02:00:00") == (
        datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc), R.CRAWL_TIME_NAIVE_UTC)
    assert R.parse_crawl_time("") == (None, R.CRAWL_TIME_MISSING)
    assert R.parse_crawl_time(None) == (None, R.CRAWL_TIME_MISSING)
    assert R.parse_crawl_time("not a time") == (None, R.CRAWL_TIME_UNPARSABLE)
    assert R.parse_crawl_time(12345) == (None, R.CRAWL_TIME_MISSING)


def test_parse_collected_at_keeps_its_phase2_signature():
    """两元组签名是 Phase 2 就有的公开面，别在 P4-7 顺手改掉。"""
    fallback = datetime(2000, 1, 1, tzinfo=timezone.utc)
    assert R.parse_collected_at("2026-08-05 10:00:00", fallback) == (
        datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc), False)
    assert R.parse_collected_at("2026-08-05T02:00:00Z", fallback) == (
        datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc), False)
    assert R.parse_collected_at("", fallback) == (fallback, True)
    assert R.parse_collected_at("not a time", fallback) == (fallback, True)
    # 裸 datetime 沿用旧口径（+08:00）：JSON 路径产生不了它，改口径没有收益
    assert R.parse_collected_at(datetime(2026, 8, 5, 10, 0), fallback) == (
        datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc), False)


# ==================================================================
# C/D/E) 端到端：真库 + 真 relay
# ==================================================================

GOOD_PAYLOAD = {
    "asin": "B0PHASE401",
    "title": "Anker PowerCore 10000 Portable Charger",
    "brand": "Anker",
    "product_type": "Portable Power Bank",
    "manufacturer": "Anker Innovations",
    "model_number": "A1263",
    "bullet_points": "one\ntwo",
    "long_description": "desc",
    "image_urls": "https://m.media-amazon.com/images/I/71X._AC_SL1500_.jpg",
    "upc_list": "012345678905",
    "category_tree": "Home > Test > Sub",
    "category_ids": "c1,c2",
    "root_category_id": "root123",
    "variant_attributes": "Color:Red",
    "parent_asin": "B0PARENT01",
    "current_price": "29.99",
    "buybox_price": "29.99",
    "stock_count": "12",
    "stock_status": "In Stock",
    "site": "US",
    "zip_code": "10001",
    "crawl_time": "2026-08-05T02:00:00Z",
    # worker 的 `_` 前缀采集元数据
    "_outcome": "ok",
    "_zip_requested": "10001",
    "_zip_observed": "10001",
    "_zip_verify": "confirmed",
    "_completeness": 15,
    "_parse_engine": "selectolax",
}


async def _drain(pgdb, pgconn):
    """跑一轮 relay 的认领 + 落库（不起后台任务，测试自己控制节奏）。"""
    n = 1
    while n:
        n = await pgdb._relay_drain_once(pgconn, 500)
    return await pgconn.fetch(
        "SELECT * FROM scraper.scrape_events ORDER BY seq")


async def _submit(pgdb, data, *, batch_name="p4", zip_code="10001", is_auto=False):
    """建批次 + 建任务 + 拉任务 + 走真正的 accept_success_result。"""
    bid = await pgdb.create_batch(batch_name)
    if is_auto:
        await pgdb._db.execute("UPDATE batches SET is_auto = 1 WHERE id = ?", (bid,))
    await pgdb.create_tasks(bid, [data["asin"]], zip_code=zip_code)
    tasks = await pgdb.pull_tasks("w-p4", 50)
    task = [t for t in tasks if t["asin"] == data["asin"]][0]
    res = await pgdb.accept_success_result(
        task["id"], "w-p4", task["lease_epoch"], data, bid)
    return bid, res


@pytest.mark.asyncio
async def test_quality_signals_reach_the_event_columns(pgdb, pgconn):
    """C：四个信号从 payload 的 `_` 键落到 scrape_events 的同名列。

    Phase 2 时这四列是写死的占位值（None / 'unverified' / 0 / None），
    relay.py 里并排躺着四个 `# Phase 4` 注释。这是那四行的验收。
    """
    await _submit(pgdb, dict(GOOD_PAYLOAD))
    rows = await _drain(pgdb, pgconn)
    assert len(rows) == 1
    r = rows[0]
    assert r["outcome"] == "ok"
    assert r["zip_requested"] == "10001"
    assert r["zip_observed"] == "10001"
    assert r["zip_verify"] == "confirmed"
    assert r["completeness"] == 15
    assert r["parse_engine"] == "selectolax"
    # collected_at 来自新格式的 crawl_time，且**不是** recorded_at 兜底
    assert r["collected_at"] == datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc)
    assert r["collected_at"] != r["recorded_at"]
    # marketplace 仍然是封闭集，绝不透传 parser 的 site="US"
    assert r["marketplace"] == "amazon.com"
    assert r["review_hash"] and r["slow_hash"]
    # `_` 键原样留在 payload 里（消费侧要能自己复核）
    payload = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
    assert payload["_zip_verify"] == "confirmed"
    assert payload["_completeness"] == 15
    counters = pgdb.event_relay_metrics()["counters"]
    for k in ("zip_verify_coerced", "completeness_coerced", "parse_engine_coerced",
              "zip_observed_coerced", "collected_at_fallback"):
        assert counters[k] == 0, (k, counters)


@pytest.mark.asyncio
async def test_old_worker_payload_still_lands_with_honest_placeholders(pgdb, pgconn):
    """灰度期的另一半：老 worker 没有任何 `_` 键。

    结果必须是**诚实的缺省**（unverified / 0 / NULL），而不是错误，
    也不该把归一化计数器刷起来——那几个计数器是漂移探针，不是流量计。
    """
    old = {k: v for k, v in GOOD_PAYLOAD.items() if not k.startswith("_")}
    old["crawl_time"] = "2026-08-05 10:00:00"     # 老格式：裸 UTC+8
    await _submit(pgdb, old)
    rows = await _drain(pgdb, pgconn)
    r = rows[0]
    assert r["outcome"] == "ok"
    assert r["zip_observed"] is None
    assert r["zip_verify"] == "unverified"
    assert r["completeness"] == 0
    assert r["parse_engine"] is None
    assert r["collected_at"] == datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc)
    counters = pgdb.event_relay_metrics()["counters"]
    assert counters["zip_verify_coerced"] == 0
    assert counters["completeness_coerced"] == 0
    assert counters["parse_engine_coerced"] == 0
    assert counters["collected_at_legacy_cst"] == 1
    assert counters["collected_at_fallback"] == 0


@pytest.mark.asyncio
async def test_mixed_fleet_two_wire_formats_in_one_relay_batch(pgdb, pgconn):
    """B（端到端）：新老两个 worker 的记录进同一批 relay，时间戳必须一致。

    这不是假想场景：worker 与 server 独立部署，灰度期同一秒内两种提交体都在。
    """
    new = dict(GOOD_PAYLOAD, asin="B0MIXNEW01", crawl_time="2026-08-05T02:00:00Z")
    old = {k: v for k, v in GOOD_PAYLOAD.items() if not k.startswith("_")}
    old = dict(old, asin="B0MIXOLD01", crawl_time="2026-08-05 10:00:00")
    await _submit(pgdb, new, batch_name="mix-new")
    await _submit(pgdb, old, batch_name="mix-old")
    rows = await _drain(pgdb, pgconn)
    by_asin = {r["asin"]: r for r in rows}
    assert len(by_asin) == 2
    want = datetime(2026, 8, 5, 2, 0, tzinfo=timezone.utc)
    assert by_asin["B0MIXNEW01"]["collected_at"] == want
    assert by_asin["B0MIXOLD01"]["collected_at"] == want
    assert (by_asin["B0MIXNEW01"]["collected_at"]
            == by_asin["B0MIXOLD01"]["collected_at"])
    counters = pgdb.event_relay_metrics()["counters"]
    assert counters["collected_at_legacy_cst"] == 1     # 老的那条被记了一笔
    assert counters["collected_at_fallback"] == 0       # 谁都没退到兜底
    # 新老可分辨（parse_engine / completeness），但时间轴是同一条
    assert by_asin["B0MIXNEW01"]["parse_engine"] == "selectolax"
    assert by_asin["B0MIXOLD01"]["parse_engine"] is None


@pytest.mark.asyncio
async def test_relay_downgrades_a_self_contradictory_confirmed(pgdb, pgconn):
    """E：zip_verify=confirmed 却带着别的 zip_observed -> mismatch + 计数。"""
    bad = dict(GOOD_PAYLOAD, asin="B0ZIPBAD01",
               _zip_observed="90210", _zip_verify="confirmed")
    await _submit(pgdb, bad, zip_code="10001")
    rows = await _drain(pgdb, pgconn)
    r = rows[0]
    assert r["zip_requested"] == "10001"
    assert r["zip_observed"] == "90210"
    assert r["zip_verify"] == "mismatch"
    assert pgdb.event_relay_metrics()["counters"]["zip_verify_downgraded"] == 1


@pytest.mark.asyncio
async def test_zip_requested_prefers_the_task_row_then_the_worker_meta(pgdb, pgconn):
    """E：三级仲裁。tasks.zip_code 为空串时，真相在 payload 的 `_zip_requested`。

    worker 的 target_zip 是 `(task.zip_code or "").strip() or worker.zip_code`，
    所以 tasks 行上的空串既非 NULL 又不是真相。照收它 = 整批记录的分组键
    (asin, marketplace, zip_requested) 变成 ''，消费侧再也分不出这组价格
    是哪个邮编采的（契约 §6.1 硬规则）。
    """
    data = dict(GOOD_PAYLOAD, asin="B0ZIPMETA1", _zip_requested="90210",
                zip_code="90210")
    bid = await pgdb.create_batch("zipmeta")
    await pgdb.create_tasks(bid, [data["asin"]], zip_code="")
    tasks = await pgdb.pull_tasks("w-p4", 50)
    t = [x for x in tasks if x["asin"] == data["asin"]][0]
    await pgdb.accept_success_result(t["id"], "w-p4", t["lease_epoch"], data, bid)
    rows = await _drain(pgdb, pgconn)
    assert rows[0]["zip_requested"] == "90210"
    counters = pgdb.event_relay_metrics()["counters"]
    assert counters["zip_requested_from_payload_meta"] == 1
    assert counters["zip_requested_mismatch"] == 0


@pytest.mark.asyncio
async def test_blank_task_zip_consults_the_meta_before_the_payload_zip(
        pgdb, pgconn):
    """D-54：`tasks.zip_code=''` 且 payload 两个邮编**不一致**时，第 2 级必须赢。

    这一条盯的是仲裁被**写成两份**的后果，而不是仲裁本身。
    `outbox.result_context` 原来在「tasks 行不权威」这一格里自己退到了
    `data['zip_code']`（第 3 级），等于把第 2 级 `_zip_requested` 跳过去了。
    上面那条用例发现不了：它的两个邮编都是 '90210'，值一样，只有
    `zip_requested_source` 的标签是错的。

    这里让两级**分开**：worker 自述请求的是 90210（真相 —— target_zip 是
    `(task.zip_code or "").strip() or worker.zip_code`），而 payload 的
    `zip_code` 是老 worker 留下的 10001。抢答的写法会保留 10001 并把它当成
    「服务端事实 vs worker 自述冲突」记一次 mismatch —— 值错了，还记成了
    另一种故障。正确结果是 90210 / from_payload_meta / mismatch 计数为 0。
    """
    data = dict(GOOD_PAYLOAD, asin="B0ZIPORD01", _zip_requested="90210",
                zip_code="10001")
    bid = await pgdb.create_batch("ziporder")
    await pgdb.create_tasks(bid, [data["asin"]], zip_code="")
    tasks = await pgdb.pull_tasks("w-p4", 50)
    t = [x for x in tasks if x["asin"] == data["asin"]][0]
    await pgdb.accept_success_result(t["id"], "w-p4", t["lease_epoch"], data, bid)
    # 出处（zip_requested_source）只在 outbox 的 body 里，不是 scrape_events 的列，
    # 所以趁 relay 还没认领先读一眼——落库值对但出处标错，正是抢答的症状之一。
    body = json.loads(await pgconn.fetchval(
        "SELECT body FROM scraper.scrape_outbox ORDER BY id DESC LIMIT 1"))
    assert body["zip_requested"] == "90210"
    assert body["zip_requested_source"] == "payload_meta"

    rows = await _drain(pgdb, pgconn)
    assert rows[0]["zip_requested"] == "90210", (
        "第 2 级被跳过了：落库的是 payload.zip_code 而不是 worker 自述的请求邮编")
    counters = pgdb.event_relay_metrics()["counters"]
    assert counters["zip_requested_from_payload_meta"] == 1
    assert counters["zip_requested_mismatch"] == 0, (
        "tasks 行是空串，它根本不是「服务端事实」，不该记成事实与自述冲突")


@pytest.mark.asyncio
async def test_task_row_wins_over_the_worker_meta_but_the_disagreement_rings(
        pgdb, pgconn):
    """服务端事实优先（消费侧要能按批次对上账），但不一致必须可见。"""
    data = dict(GOOD_PAYLOAD, asin="B0ZIPWAR01", _zip_requested="99999")
    await _submit(pgdb, data, zip_code="10001")
    rows = await _drain(pgdb, pgconn)
    assert rows[0]["zip_requested"] == "10001"
    assert pgdb.event_relay_metrics()["counters"]["zip_requested_mismatch"] == 1


# ---------------- D) 404 ----------------

def _new_not_found_payload(asin="B0NF40401", zip_code="10001"):
    """新 worker 的 404 提交体：慢变字段一个都不提交，只留 `_outcome`。

    形状与 worker/engine.py 的 `_build_not_found_result` 一致；这里手写而不是
    import worker.engine，因为那个模块在模块级 import worker.session ->
    curl_cffi（本环境没有）。形状的一致性由
    tests/test_engine_not_found.py 那一侧的用例保证。
    """
    return {
        "asin": asin,
        "crawl_time": "2026-08-06T02:00:00Z",
        "site": "US",
        "zip_code": zip_code,
        "product_url": f"https://www.amazon.com/dp/{asin}",
        "current_price": "N/A",
        "buybox_price": "N/A",
        "stock_count": "0",            # ← 这个 "0" 让它过得了 _is_parse_failure
        "stock_status": "N/A",
        "rating": "N/A",
        "review_count": "N/A",
        "seller_id": "N/A",
        "seller_name": "N/A",
        "_outcome": "not_found",
        "_zip_requested": zip_code,
        "_zip_observed": None,
        "_zip_verify": "unverified",
        "_completeness": 0,
        "_parse_engine": None,
    }


def _legacy_not_found_payload(pgdb_unused=None, asin="B0NF40402", zip_code="10001"):
    """老 worker 的 404 提交体：`_default_result()` 全套 "N/A" + 哨兵标题。"""
    data = real_parser_module().AmazonParser()._default_result(asin, zip_code)
    data["title"] = R.NOT_FOUND_TITLE
    data["crawl_time"] = "2026-08-06 10:00:00"          # 老格式
    for k in list(data):
        if k.startswith("_"):
            del data[k]                                  # 老 worker 没有这些键
    return data


@pytest.mark.asyncio
@pytest.mark.parametrize("builder,label", [
    (_new_not_found_payload, "new"),
    (_legacy_not_found_payload, "legacy"),
])
async def test_404_is_not_found_and_keeps_the_catalog_layer(
        pgdb, pgconn, builder, label):
    """D：新老两种 404 提交体，两边都必须

      1. 在事件流里是 outcome='not_found'（而不是 ok）；
      2. review_hash / slow_hash 都是 NULL（契约 §6.3 的第一道）；
      3. **不覆盖** asin_data 的目录层，也不污染两个哈希列与 baseline；
      4. 不产出假的 asin_changes 记录。

    第 1 条对新提交体尤其要紧：它**没有 title**（title 是慢变字段，P4-3 之后
    404 一个慢变字段都不提交），所以哨兵标题那条老信号在新 worker 上不存在，
    唯一的信号就是 `_outcome`。判错的后果不是标签难看：relay 只对 outcome=='ok'
    算哈希，于是 404 会得到一个"慢变字段全 null"的 review_hash，
    好页 -> 404 -> 好页 = 两次翻转 = 两次误复审。
    """
    asin = f"B0NF{label.upper()[:3]}001"
    good = dict(GOOD_PAYLOAD, asin=asin)
    await _submit(pgdb, good, batch_name=f"good-{label}", is_auto=True)

    async with pgdb._db.execute(
            "SELECT * FROM asin_data WHERE asin = ?", (asin,)) as c:
        before = dict(await c.fetchone())

    nf = builder(asin=asin) if label == "new" else builder(asin=asin)
    await _submit(pgdb, nf, batch_name=f"nf-{label}", is_auto=True)

    async with pgdb._db.execute(
            "SELECT * FROM asin_data WHERE asin = ?", (asin,)) as c:
        after = dict(await c.fetchone())

    # 3) 目录层 + 两个哈希 + baseline 全部原封不动
    for col in sorted(NOT_FOUND_PRESERVED_FIELDS):
        if col in before:
            assert after[col] == before[col], f"{label}: {col} 被 404 覆盖了"
    for col in ("baseline_price", "baseline_buybox_price", "baseline_stock_count",
                "baseline_stock_status", "baseline_title_bullets_hash"):
        assert after[col] == before[col], f"{label}: {col} 被 404 污染了"
    # 快变层如实写成"不可得"，采集参数是这一次的
    assert after["current_price"] == "N/A"
    assert after["stock_count"] == "0"
    assert after["crawl_time"] != before["crawl_time"]

    # 4) 没有假变动（只剩首次入库那条 'new'）
    async with pgdb._db.execute(
            "SELECT change_type FROM asin_changes WHERE asin = ? ORDER BY id",
            (asin,)) as c:
        kinds = [r[0] for r in await c.fetchall()]
    assert kinds == ["new"], f"{label}: 多出假变动 {kinds}"

    # 1) + 2) 事件流侧
    rows = await _drain(pgdb, pgconn)
    assert [r["outcome"] for r in rows] == ["ok", "not_found"], label
    nf_row = rows[1]
    assert nf_row["review_hash"] is None
    assert nf_row["slow_hash"] is None
    assert nf_row["zip_verify"] == "unverified"
    assert nf_row["completeness"] == 0


@pytest.mark.asyncio
async def test_an_outbox_row_written_before_the_upgrade_is_still_classified(
        pgdb, pgconn):
    """升级窗口：老 server 写的 body（outcome='ok'）+ 新 worker 的 404 payload。

    outbox 是**库表**不是内存队列：升级前入队、升级后才被认领的行带的是老代码
    写的 body。窗口宽度 = 重启时的 outbox 深度，不是零。
    """
    gen = pgdb.event_gen
    body = R.dumps_body({
        "v": 1, "source_id": f"{gen}:preupgrade", "gen": gen,
        "outcome": "ok",                       # ← 老 server 的分类
        "asin": "B0PREUPG01", "marketplace": "amazon.com",
        "zip_requested": "10001",
        "result": _new_not_found_payload(asin="B0PREUPG01"),
    })
    await pgconn.execute(
        "INSERT INTO scraper.scrape_outbox (body) VALUES ($1::jsonb)", body)
    rows = await _drain(pgdb, pgconn)
    assert rows[0]["outcome"] == "not_found"
    assert rows[0]["review_hash"] is None and rows[0]["slow_hash"] is None
    assert pgdb.event_relay_metrics()["counters"]["outcome_recovered_not_found"] == 1


# ==================================================================
# F) 跨模块值域对表
# ==================================================================

def test_zip_verify_domain_matches_the_parser():
    """三份定义（schema / parser / engine）不许漂。engine 侧的那一份由
    tests/test_engine_not_found.py 对着 parser 比，这里比 schema 与 parser。"""
    P = real_parser_module()
    assert set(S.EVENT_ZIP_VERIFY_VALUES) == {
        P.ZIP_VERIFY_CONFIRMED, P.ZIP_VERIFY_ASSUMED,
        P.ZIP_VERIFY_MISMATCH, P.ZIP_VERIFY_UNVERIFIED}
    assert S.EVENT_ZIP_VERIFY_DEFAULT == P.ZIP_VERIFY_UNVERIFIED


def test_parse_engine_domain_matches_the_parser():
    P = real_parser_module()
    assert set(S.EVENT_PARSE_ENGINES) == {P.ENGINE_SELECTOLAX, P.ENGINE_LXML}


def test_completeness_bits_match_the_parser_and_the_contract():
    P = real_parser_module()
    assert S.EVENT_COMPLETENESS_BREADCRUMB == P.COMPLETENESS_BREADCRUMB == 1
    assert S.EVENT_COMPLETENESS_DETAIL == P.COMPLETENESS_DETAIL_TABLE == 2
    assert S.EVENT_COMPLETENESS_IMAGE == P.COMPLETENESS_MAIN_IMAGE == 4
    assert S.EVENT_COMPLETENESS_MEASURED == P.COMPLETENESS_MEASURED == 8
    assert S.EVENT_COMPLETENESS_MAX == 15
    # 契约 §6.4 的合取式（Phase 3 把它算在服务端）
    from server.api.sync import COMPLETENESS_MEASURED_BIT, COMPLETENESS_REQUIRED_MASK
    assert COMPLETENESS_MEASURED_BIT == S.EVENT_COMPLETENESS_MEASURED
    assert COMPLETENESS_REQUIRED_MASK == (
        S.EVENT_COMPLETENESS_BREADCRUMB | S.EVENT_COMPLETENESS_DETAIL
        | S.EVENT_COMPLETENESS_IMAGE)


def test_meta_key_names_match_the_parser_default_result():
    """relay 读的 `_` 键名必须与 parser 产出的完全一致。

    engine 的 META_* 常量由 tests/test_engine_not_found.py 对着 parser 比；
    这里比的是**服务端读的那一份**——名字漂一个字母，四列就静默回到占位值，
    而 relay 那边什么都不会响（缺键的语义是"老 worker"）。
    """
    default = real_parser_module().AmazonParser()._default_result("B0IFACE001", "10001")
    for key in (S.EVENT_META_ZIP_OBSERVED, S.EVENT_META_ZIP_VERIFY,
                S.EVENT_META_COMPLETENESS, S.EVENT_META_PARSE_ENGINE):
        assert key in default, f"parser 的 _default_result 少了 {key}"
    assert default[S.EVENT_META_ZIP_VERIFY] in S.EVENT_ZIP_VERIFY_VALUES


def test_not_found_preserved_fields_cover_the_slow_hash_field_set():
    """P4-3 的保留集必须是慢变字段的超集，否则复审门还是会被 404 翻。"""
    from common.slowhash import SLOW_HASH_FIELDS
    missing = (set(SLOW_HASH_FIELDS) - {"image_ids"}) - NOT_FOUND_PRESERVED_FIELDS
    assert not missing, missing
    assert "image_urls" in NOT_FOUND_PRESERVED_FIELDS      # image_ids 的原始列
    assert {"content_hash", "title_bullets_hash"} <= NOT_FOUND_PRESERVED_FIELDS
    # 快变字段绝不能进保留集：404 时它们必须被写成"不可得"
    for fast in ("current_price", "buybox_price", "stock_count", "stock_status",
                 "delivery_date", "best_sellers_rank", "rating", "review_count",
                 "seller_id", "seller_name", "crawl_time", "zip_code", "site"):
        assert fast not in NOT_FOUND_PRESERVED_FIELDS, fast
