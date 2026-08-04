"""common/pgdb/batches.py 的验收用例（agent C）。

黄金 64 步只覆盖 create_batch / get_batches / get_batch_by_name 的一部分，
**整套回调机制、mark_batch_completed、get_batch_completion_status 都没覆盖**，
只能靠这里兜。断言的是"返回形状 + Python 类型"，不只是"值差不多"。

对照实验（SQLite 原实现 vs 本模块，40 步逐字节 diff）见提交说明；
本文件只保留能在 CI 里跑的那部分。
"""
from __future__ import annotations

import re
from datetime import datetime

import pytest

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


async def _seed_tasks(db, batch_id, statuses):
    for i, st in enumerate(statuses):
        await db._db.execute(
            "INSERT INTO tasks (batch_id, asin, zip_code, status) VALUES (?,?,?,?)",
            (batch_id, "B0T%06d" % i, "10001", st))


# ============================ create_batch ============================

@pytest.mark.asyncio
async def test_create_batch_returns_id_and_duplicate_is_noop(pgdb):
    first = await pgdb.create_batch("b1")
    assert first == 1
    again = await pgdb.create_batch("b1", needs_screenshot=True)
    # 同名重传：静默 no-op，返回**已有** id（不是 0、不是新 id）
    assert again == first
    row = await pgdb.get_batch_by_name("b1")
    assert row["needs_screenshot"] == 0          # 第二次的参数没有生效


@pytest.mark.asyncio
async def test_create_batch_burns_identity_like_sqlite(pgdb):
    """自增烧号被基线钉死：golden_batch_b 的 id 是 3 不是 2。"""
    a = await pgdb.create_batch("a")
    dup = await pgdb.create_batch("a")     # 冲突，但序列照样 +1
    b = await pgdb.create_batch("b")
    assert (a, dup, b) == (1, 1, 3)


@pytest.mark.asyncio
async def test_create_batch_flags_are_integers_not_booleans(pgdb):
    """/api/batches 的 needs_screenshot 在基线里是 int 0/1，不是 true/false。"""
    await pgdb.create_batch("b1", needs_screenshot=True, is_auto=True,
                            expand_variants=True)
    row = await pgdb.get_batch_by_name("b1")
    for k in ("needs_screenshot", "is_auto", "expand_variants"):
        assert type(row[k]) is int and row[k] == 1, k


@pytest.mark.asyncio
async def test_create_batch_coerces_non_string_name(pgdb):
    """SQLite 的 TEXT affinity 会把 int 名字存成 '123'；asyncpg 会直接 raise。"""
    bid = await pgdb.create_batch(123)
    assert bid > 0
    row = await pgdb.get_batch_by_name("123")
    assert row is not None and row["name"] == "123"


@pytest.mark.asyncio
async def test_create_batch_callback_status_pending_only_with_url(pgdb):
    await pgdb.create_batch("with_cb", callback_url="http://x/y", external_id="E1")
    await pgdb.create_batch("no_cb")
    assert (await pgdb.get_batch_by_name("with_cb"))["callback_status"] == "pending"
    assert (await pgdb.get_batch_by_name("no_cb"))["callback_status"] is None


# ============================ get_batch_by_name ============================

@pytest.mark.asyncio
async def test_get_batch_by_name_leaks_all_18_columns(pgdb):
    """SELECT * —— 整张表的列都会进 erpAPI 的响应，列集是契约的一部分。"""
    await pgdb.create_batch("b1")
    row = await pgdb.get_batch_by_name("b1")
    assert set(row) == {
        "id", "name", "needs_screenshot", "is_auto", "expand_variants", "status",
        "completed_at", "external_id", "callback_url", "callback_status",
        "callback_attempts", "callback_next_retry_at", "callback_last_error",
        "callback_sent_at", "created_at", "updated_at", "batch_type",
        "discover_mode",
    }
    assert await pgdb.get_batch_by_name("nope") is None


@pytest.mark.asyncio
async def test_timestamps_are_plain_strings(pgdb):
    """app.py:1168/487 对 created_at 做 strptime(x[:19], ...)，datetime 会 TypeError。"""
    bid = await pgdb.create_batch("b1")
    await pgdb.mark_batch_completed(bid)
    row = await pgdb.get_batch_by_name("b1")
    for k in ("created_at", "updated_at", "completed_at"):
        assert type(row[k]) is str, (k, type(row[k]))
        assert TS_RE.match(row[k]), (k, row[k])
        # app.py 的真实用法必须能跑通
        datetime.strptime(row[k][:19], '%Y-%m-%d %H:%M:%S')


# ============================ get_batches ============================

@pytest.mark.asyncio
async def test_get_batches_shape_and_order(pgdb):
    a = await pgdb.create_batch("a")
    b = await pgdb.create_batch("b")
    await _seed_tasks(pgdb, a, ["done", "done", "failed", "pending", "processing"])
    rows = await pgdb.get_batches()
    assert [r["id"] for r in rows] == [b, a]        # ORDER BY b.id DESC
    assert set(rows[0]) == {"id", "name", "needs_screenshot", "created_at",
                            "total_tasks", "completed", "failed", "pending",
                            "processing"}
    ra = next(r for r in rows if r["id"] == a)
    assert (ra["total_tasks"], ra["completed"], ra["failed"],
            ra["pending"], ra["processing"]) == (5, 2, 1, 1, 1)
    # SUM(CASE ...) 的分支是整数字面量 -> bigint -> int。
    # 若有人加了 ::bigint 之类的转换就会变 Decimal，FastAPI 会序列化成字符串。
    for k in ("total_tasks", "completed", "failed", "pending", "processing"):
        assert type(ra[k]) is int, (k, type(ra[k]))


@pytest.mark.asyncio
async def test_get_batches_zero_task_batch(pgdb):
    """LEFT JOIN 出一行全 NULL 的 t -> COUNT=0、SUM(CASE)=0（两个后端一致）。"""
    await pgdb.create_batch("empty")
    row = (await pgdb.get_batches())[0]
    assert row["total_tasks"] == 0
    assert (row["completed"], row["failed"], row["pending"],
            row["processing"]) == (0, 0, 0, 0)


# ==================== get_batch_completion_status ====================

@pytest.mark.asyncio
async def test_completion_status_key_is_open_not_open_underscore(pgdb):
    a = await pgdb.create_batch("a")
    await _seed_tasks(pgdb, a, ["done", "failed", "pending"])
    await pgdb._db.execute(
        "INSERT INTO screenshots (asin, batch_id, status) VALUES (?,?,?)",
        ("B0T000000", a, "done"))
    snap = await pgdb.get_batch_completion_status(a)
    assert set(snap) == {"tasks", "screenshots", "all_terminal"}
    assert set(snap["tasks"]) == {"total", "done", "failed", "open"}  # 不是 open_
    assert snap["tasks"] == {"total": 3, "done": 1, "failed": 1, "open": 1}
    assert snap["screenshots"] == {"total": 1, "done": 1, "failed": 0, "open": 0}
    assert snap["all_terminal"] is False


@pytest.mark.asyncio
async def test_completion_status_all_terminal(pgdb):
    a = await pgdb.create_batch("a")
    await _seed_tasks(pgdb, a, ["done", "failed"])
    assert (await pgdb.get_batch_completion_status(a))["all_terminal"] is True


@pytest.mark.asyncio
async def test_completion_status_missing_batch_is_all_zero(pgdb):
    """空聚合：SUM 给 NULL、COUNT 给 0，`or 0` 兜住；total=0 -> all_terminal=False。"""
    snap = await pgdb.get_batch_completion_status(999999)
    assert snap["tasks"] == {"total": 0, "done": 0, "failed": 0, "open": 0}
    assert snap["all_terminal"] is False


# ======================== mark_batch_completed ========================

@pytest.mark.asyncio
async def test_mark_batch_completed_is_an_idempotent_cas(pgdb):
    a = await pgdb.create_batch("a")
    assert await pgdb.mark_batch_completed(a) is True    # running -> completed
    assert await pgdb.mark_batch_completed(a) is False   # 第二次不再转移
    assert await pgdb.mark_batch_completed(999999) is False
    row = await pgdb.get_batch_by_name("a")
    assert row["status"] == "completed" and row["completed_at"] is not None


# ============================ 回调机制 ============================

@pytest.mark.asyncio
async def test_list_callback_due_filters(pgdb):
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    done = await pgdb.create_batch("cb_done", callback_url="http://x/1")
    await pgdb.create_batch("cb_running", callback_url="http://x/2")   # 未 completed
    nourl = await pgdb.create_batch("cb_nourl")                        # 无 url
    await pgdb.mark_batch_completed(done)
    await pgdb.mark_batch_completed(nourl)

    rows = await pgdb.list_callback_due(now, 50)
    assert [r["id"] for r in rows] == [done]
    assert set(rows[0]) == {"id", "name", "external_id", "callback_url",
                            "callback_attempts", "completed_at", "status"}
    assert rows[0]["status"] == "completed"

    # next_retry_at 在未来 -> 不到期
    await pgdb.mark_callback_attempt(done, False, error="x",
                                     next_retry_at="2099-01-01 00:00:00")
    assert await pgdb.list_callback_due(now, 50) == []
    # 到期后又回来
    assert [r["id"] for r in await pgdb.list_callback_due("2099-06-01 00:00:00", 50)] \
        == [done]


@pytest.mark.asyncio
async def test_mark_callback_attempt_success_shape(pgdb):
    bid = await pgdb.create_batch("b", callback_url="http://x/1")
    res = await pgdb.mark_callback_attempt(bid, True)
    assert res == {"final_status": "sent"}          # 成功分支**没有** attempts 键
    row = await pgdb.get_batch_by_name("b")
    assert row["callback_status"] == "sent"
    assert TS_RE.match(row["callback_sent_at"])
    assert row["callback_last_error"] is None


@pytest.mark.asyncio
async def test_mark_callback_attempt_counts_up_to_cap(pgdb):
    bid = await pgdb.create_batch("b", callback_url="http://x/1")
    seen = []
    for i in range(5):
        seen.append(await pgdb.mark_callback_attempt(
            bid, False, error="e%d" % i, next_retry_at="2099-01-01 00:00:00"))
    assert seen == [
        {"final_status": "pending", "attempts": 1},
        {"final_status": "pending", "attempts": 2},
        {"final_status": "pending", "attempts": 3},
        {"final_status": "pending", "attempts": 4},
        {"final_status": "failed", "attempts": 5},
    ]
    row = await pgdb.get_batch_by_name("b")
    assert row["callback_status"] == "failed" and row["callback_attempts"] == 5
    # 达上限的分支**不写** next_retry_at，所以停在第 4 次写的值
    assert row["callback_next_retry_at"] == "2099-01-01 00:00:00"


@pytest.mark.asyncio
async def test_mark_callback_attempt_missing_batch_still_returns_attempts_1(pgdb):
    """`if row else 1` 的兜底：批次不存在时 attempts=1 且 UPDATE 匹配 0 行。"""
    assert await pgdb.mark_callback_attempt(999999, False, error="x") == \
        {"final_status": "pending", "attempts": 1}


@pytest.mark.asyncio
async def test_mark_callback_attempt_truncates_error_to_500(pgdb):
    bid = await pgdb.create_batch("b", callback_url="http://x/1")
    await pgdb.mark_callback_attempt(bid, False, error="Z" * 800, max_attempts=99)
    assert len((await pgdb.get_batch_by_name("b"))["callback_last_error"]) == 500
    # error=None -> (None or "")[:500] -> ''，不是 NULL
    await pgdb.mark_callback_attempt(bid, False, error=None, max_attempts=99)
    assert (await pgdb.get_batch_by_name("b"))["callback_last_error"] == ""


@pytest.mark.asyncio
async def test_reset_callback_for_retry(pgdb):
    bid = await pgdb.create_batch("b", callback_url="http://x/1")
    nourl = await pgdb.create_batch("nourl")
    for i in range(5):
        await pgdb.mark_callback_attempt(bid, False, error="e")
    assert await pgdb.reset_callback_for_retry(bid) is True
    row = await pgdb.get_batch_by_name("b")
    assert row["callback_status"] == "pending"
    assert row["callback_attempts"] == 0
    assert row["callback_next_retry_at"] is None
    assert row["callback_last_error"] is None
    # 没有 callback_url 的批次 / 不存在的批次 -> False
    assert await pgdb.reset_callback_for_retry(nourl) is False
    assert await pgdb.reset_callback_for_retry(999999) is False


# ============================ 类型强转 ============================

@pytest.mark.asyncio
async def test_string_batch_id_is_accepted_like_sqlite_affinity(pgdb):
    """SQLite 的 `id IN ('3')` 会命中；asyncpg 不做隐式转换，必须显式强转。"""
    bid = await pgdb.create_batch("b", callback_url="http://x/1")
    assert (await pgdb.get_batch_completion_status(str(bid)))["tasks"]["total"] == 0
    assert await pgdb.mark_batch_completed(str(bid)) is True
    assert await pgdb.reset_callback_for_retry(str(bid)) is True
    assert await pgdb.mark_callback_attempt(str(bid), True) == {"final_status": "sent"}
