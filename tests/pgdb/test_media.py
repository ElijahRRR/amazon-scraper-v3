"""tests/pgdb/test_media.py —— common/pgdb/media.py 的验收门。

黄金 64 步**一条也没覆盖**本文件的 10 个方法（没有任何一步碰截图落地、
seller 发现或变体展开），所以返回形状只能靠这里逐键钉死。

比对基准是 common/database.py 的 SQLite 实现：
``scratchpad/media_cmp.py`` 用同一个 scenario 双跑两个后端、逐字段 diff
（69 步 0 差异，含参数类型强转）。本文件把其中的关键断言固化下来，
这样 CI 里不需要 SQLite 也能守住形状。
"""
from __future__ import annotations

import pytest

pytest_plugins = []


# ============================================================
# 截图
# ============================================================

@pytest.mark.asyncio
async def test_get_pending_screenshots_shape(pgdb):
    bid = await pgdb.create_batch("b1", needs_screenshot=True)
    await pgdb.create_tasks(bid, ["B01", "B02"], "10001", True)

    rows = await pgdb.get_pending_screenshots(bid, 50)
    assert len(rows) == 2
    # SELECT * —— 9 列全部泄给调用方，列集是契约的一部分
    assert sorted(rows[0].keys()) == [
        "asin", "batch_id", "created_at", "error_detail", "file_path",
        "id", "retry_count", "status", "updated_at"]
    r = sorted(rows, key=lambda d: d["asin"])[0]
    assert r["asin"] == "B01"
    assert r["status"] == "pending"
    assert r["file_path"] is None
    # 时间戳必须是 TEXT（datetime 对象会让 app.py 的 strptime(x[:19]) 炸）
    assert isinstance(r["created_at"], str) and len(r["created_at"]) == 19
    assert isinstance(r["retry_count"], int)

    assert await pgdb.get_pending_screenshots(bid, 1) != []
    assert await pgdb.get_pending_screenshots(999999, 50) == []


@pytest.mark.asyncio
async def test_update_screenshot_status_branches(pgdb):
    bid = await pgdb.create_batch("b1", needs_screenshot=True)
    await pgdb.create_tasks(bid, ["B01", "B02", "B03"], "10001", True)

    # done + file_path -> 同时回写 asin_data.screenshot_path
    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title) VALUES (?, ?)", ("B01", "t"))
    assert await pgdb.update_screenshot_status("B01", bid, "done", "/p/B01.png") is True
    async with pgdb.read() as rc, rc.execute(
            "SELECT screenshot_path FROM asin_data WHERE asin=?", ("B01",)) as c:
        assert (await c.fetchone())[0] == "/p/B01.png"

    # failed -> retry_count+1
    assert await pgdb.update_screenshot_status("B02", bid, "failed", error="boom") is True
    assert await pgdb.update_screenshot_status("B02", bid, "failed", error="boom2") is True
    async with pgdb.read() as rc, rc.execute(
            "SELECT retry_count, error_detail, status FROM screenshots "
            "WHERE asin=? AND batch_id=?", ("B02", bid)) as c:
        row = await c.fetchone()
    assert (row["retry_count"], row["error_detail"], row["status"]) == (2, "boom2", "failed")

    # 其他分支（含 status='done' 但 file_path 为空 —— 原版就是落 else）
    assert await pgdb.update_screenshot_status("B03", bid, "processing") is True
    assert await pgdb.update_screenshot_status("B03", bid, "done") is True
    async with pgdb.read() as rc, rc.execute(
            "SELECT status, file_path FROM screenshots WHERE asin=? AND batch_id=?",
            ("B03", bid)) as c:
        row = await c.fetchone()
    assert row["status"] == "done" and row["file_path"] is None

    # 不存在的行 -> False，且返回值必须是真 bool
    got = await pgdb.update_screenshot_status("NOPE", bid, "done", "/p/x.png")
    assert got is False


@pytest.mark.asyncio
async def test_get_screenshot_progress(pgdb):
    bid = await pgdb.create_batch("b1", needs_screenshot=True)
    await pgdb.create_tasks(bid, ["B01", "B02", "B03"], "10001", True)
    await pgdb.update_screenshot_status("B01", bid, "done", "/p/B01.png")
    await pgdb.update_screenshot_status("B02", bid, "failed", error="x")

    prog = await pgdb.get_screenshot_progress(bid)
    assert prog == {"pending": 1, "processing": 0, "done": 1, "failed": 1, "total": 3}
    # total 是四者之和（在 total 自身入 dict 之前算的）
    assert prog["total"] == sum(prog[k] for k in ("pending", "processing", "done", "failed"))
    assert all(isinstance(v, int) for v in prog.values())

    assert await pgdb.get_screenshot_progress(999999) == {
        "pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0}


@pytest.mark.asyncio
async def test_get_done_screenshot_path_batch_preference(pgdb):
    b1 = await pgdb.create_batch("b1", needs_screenshot=True)
    b2 = await pgdb.create_batch("b2", needs_screenshot=True)
    await pgdb.create_tasks(b1, ["B01"], "10001", True)
    await pgdb.create_tasks(b2, ["B01"], "10001", True)
    await pgdb.update_screenshot_status("B01", b1, "done", "/p/one.png")
    await pgdb.update_screenshot_status("B01", b2, "done", "/p/two.png")

    assert await pgdb._get_done_screenshot_path("B01", b1) == "/p/one.png"
    assert await pgdb._get_done_screenshot_path("B01", b2) == "/p/two.png"
    # 指定了不存在的批次 -> 回落到无批次查询（原版的两段式 queries）
    assert await pgdb._get_done_screenshot_path("B01", 424242) in ("/p/one.png", "/p/two.png")
    assert await pgdb._get_done_screenshot_path("NOPE") is None
    # 非 done 的行不算数
    await pgdb.create_tasks(b1, ["B77"], "10001", True)
    await pgdb.update_screenshot_status("B77", b1, "failed", error="x")
    assert await pgdb._get_done_screenshot_path("B77", b1) is None


@pytest.mark.asyncio
async def test_done_screenshot_path_null_updated_at_sorts_last(pgdb):
    """PG 的 DESC 默认 NULLS FIRST，SQLite 是 NULLS LAST。

    没有显式 NULLS LAST 的话，一行 updated_at 为 NULL 的截图会抢到第一名，
    改掉每一条 /api/results 与导出里的 screenshot_path。
    """
    await pgdb._db.execute(
        "INSERT INTO screenshots (asin, batch_id, status, file_path, updated_at) "
        "VALUES (?, ?, 'done', ?, NULL)", ("B09", 777, "/p/NULL.png"))
    await pgdb._db.execute(
        "INSERT INTO screenshots (asin, batch_id, status, file_path, updated_at) "
        "VALUES (?, ?, 'done', ?, ?)", ("B09", 778, "/p/TS.png", "2020-01-01 00:00:00"))

    assert await pgdb._get_done_screenshot_path("B09") == "/p/TS.png"
    assert await pgdb._get_done_screenshot_paths(["B09"]) == {"B09": "/p/TS.png"}


@pytest.mark.asyncio
async def test_get_done_screenshot_paths(pgdb):
    b1 = await pgdb.create_batch("b1", needs_screenshot=True)
    await pgdb.create_tasks(b1, ["B01", "B02", "B03"], "10001", True)
    await pgdb.update_screenshot_status("B01", b1, "done", "/p/B01.png")
    # 占位值必须被 _normalize_screenshot_path 归一成 None（= 不进 mapping）
    await pgdb.update_screenshot_status("B02", b1, "done", "none")

    assert await pgdb._get_done_screenshot_paths([]) == {}
    assert await pgdb._get_done_screenshot_paths(["B01", "B02", "B03"], b1) == {
        "B01": "/p/B01.png"}
    assert await pgdb._get_done_screenshot_paths(["B01", "B01"]) == {"B01": "/p/B01.png"}
    assert await pgdb._get_done_screenshot_paths(["NOPE"]) == {}


@pytest.mark.asyncio
async def test_hydrate_screenshot_paths_mutates_in_place(pgdb):
    b1 = await pgdb.create_batch("b1", needs_screenshot=True)
    await pgdb.create_tasks(b1, ["B01", "B02"], "10001", True)
    await pgdb.update_screenshot_status("B02", b1, "done", "/p/B02.png")

    items = [
        {"asin": "B01", "screenshot_path": "/orig/B01.png"},   # 已有 -> 原样保留
        {"asin": "B02", "screenshot_path": None},              # 缺 -> 回填
        {"asin": "B01", "screenshot_path": "none"},            # 占位值 -> None，且无 done 行
        {"asin": "NOPE", "screenshot_path": ""},               # 空串 -> None
    ]
    ret = await pgdb._hydrate_screenshot_paths(items, b1)
    assert ret is None                                          # 原地改，无返回值
    assert [i["screenshot_path"] for i in items] == [
        "/orig/B01.png", "/p/B02.png", None, None]


# ============================================================
# 卖家发现（F-009）
# ============================================================

@pytest.mark.asyncio
async def test_create_seller_batch(pgdb):
    with pytest.raises(ValueError):
        await pgdb.create_seller_batch("bad", ["A"], "bogus_mode")

    assert await pgdb.create_seller_batch("empty", []) == (0, 0)
    assert await pgdb.create_seller_batch("blank", ["  ", ""]) == (0, 0)

    got = await pgdb.create_seller_batch("s1", ["  a1 ", "A1", "a2"],
                                         "with_detail", "60601", True)
    assert isinstance(got, tuple) and len(got) == 2       # app.py:1067 直接解包
    bid, inserted = got
    assert bid > 0 and inserted == 2                      # a1/A1 去重 + 大写归一

    # 同名重传：batches 静默 no-op（返回已有 id），只补新增的那个 seller 任务
    assert await pgdb.create_seller_batch("s1", ["A1", "A3"], "with_detail",
                                          "60601", True) == (bid, 1)

    async with pgdb.read() as rc, rc.execute(
            "SELECT batch_type, discover_mode, needs_screenshot, is_auto "
            "FROM batches WHERE id=?", (bid,)) as c:
        row = await c.fetchone()
    assert row["batch_type"] == "seller_discovery"
    assert row["discover_mode"] == "with_detail"
    assert row["needs_screenshot"] == 1 and isinstance(row["needs_screenshot"], int)
    assert row["is_auto"] == 0

    async with pgdb.read() as rc, rc.execute(
            "SELECT asin, zip_code, task_type, needs_screenshot FROM tasks "
            "WHERE batch_id=? ORDER BY asin", (bid,)) as c:
        tasks = [dict(r) for r in await c.fetchall()]
    assert [t["asin"] for t in tasks] == ["A1", "A2", "A3"]
    # discover 任务自身不截图（截图开关只作用于衍生的 detail 任务）
    assert all(t["task_type"] == "discover_seller" and t["needs_screenshot"] == 0
               for t in tasks)
    assert all(t["zip_code"] == "60601" for t in tasks)


@pytest.mark.asyncio
async def test_accept_seller_discovery_result(pgdb):
    bid, _ = await pgdb.create_seller_batch("s1", ["a1"], "with_detail", "60601", True)
    task = (await pgdb.pull_tasks("wk1", 10))[0]

    items = [
        {"asin": "d001", "title": "T1", "price": "$1", "image": "i1"},
        {"asin": " d002 ", "title": "T2", "price": "$2", "image": "i2"},
        {"asin": "D001", "title": "dupe", "price": "x", "image": "y"},   # 去重
        {"asin": "", "title": "empty", "price": "", "image": ""},        # 丢弃
    ]

    stale_shape = {"accepted": False, "stale": True, "discovered": 0,
                   "new_asins": 0, "detail_tasks_created": 0}
    assert await pgdb.accept_seller_discovery_result(
        task["id"], "wk1", task["lease_epoch"] + 99, bid, "A1", items) == stale_shape
    assert await pgdb.accept_seller_discovery_result(
        task["id"], "other", task["lease_epoch"], bid, "A1", items) == stale_shape

    got = await pgdb.accept_seller_discovery_result(
        task["id"], "wk1", task["lease_epoch"], bid, "A1", items,
        {"pages_scanned": 3, "truncated": False})
    # 这个 dict 原样就是 HTTP 响应体（app.py:1626 return result）
    assert got == {"accepted": True, "stale": False, "discovered": 2,
                   "new_asins": 2, "detail_tasks_created": 2}
    assert set(got) == {"accepted", "stale", "discovered", "new_asins",
                        "detail_tasks_created"}

    # 重复提交 -> stale（lease 门必须是 int rowcount，字符串标签会静默放行）
    assert await pgdb.accept_seller_discovery_result(
        task["id"], "wk1", task["lease_epoch"], bid, "A1", items) == stale_shape

    async with pgdb.read() as rc:
        async with rc.execute(
                "SELECT * FROM seller_discoveries WHERE batch_id=? ORDER BY asin",
                (bid,)) as c:
            disc = [dict(r) for r in await c.fetchall()]
        async with rc.execute(
                "SELECT asin, zip_code, task_type, needs_screenshot FROM tasks "
                "WHERE batch_id=? AND task_type='asin' ORDER BY asin", (bid,)) as c:
            detail = [dict(r) for r in await c.fetchall()]
        async with rc.execute(
                "SELECT asin, is_new FROM batch_asins WHERE batch_id=? ORDER BY asin",
                (bid,)) as c:
            ba = [dict(r) for r in await c.fetchall()]
        async with rc.execute(
                "SELECT asin FROM screenshots WHERE batch_id=? ORDER BY asin", (bid,)) as c:
            shots = [r["asin"] for r in await c.fetchall()]
        async with rc.execute("SELECT task_meta FROM tasks WHERE id=?", (task["id"],)) as c:
            meta = (await c.fetchone())[0]

    assert [d["asin"] for d in disc] == ["D001", "D002"]     # 统一大写
    assert disc[0]["seller_id"] == "A1"
    assert disc[0]["list_title"] == "T1" and disc[0]["list_price"] == "$1"
    # discovered_at 依赖列默认值，直接出现在 /api/seller-batches/{id}/discoveries
    assert isinstance(disc[0]["discovered_at"], str) and len(disc[0]["discovered_at"]) == 19
    assert [d["asin"] for d in detail] == ["D001", "D002"]
    assert all(d["zip_code"] == "60601" and d["needs_screenshot"] == 1 for d in detail)
    assert ba == [{"asin": "D001", "is_new": 1}, {"asin": "D002", "is_new": 1}]
    assert shots == ["D001", "D002"]
    assert meta == '{"pages_scanned": 3, "truncated": false}'


@pytest.mark.asyncio
async def test_accept_seller_discovery_result_counts_and_modes(pgdb):
    # discover_only：不产生 detail 任务、不产生 batch_asins/screenshots
    bid, _ = await pgdb.create_seller_batch("s2", ["z9"], "discover_only", "10001", False)
    task = (await pgdb.pull_tasks("wk1", 10))[0]
    got = await pgdb.accept_seller_discovery_result(
        task["id"], "wk1", task["lease_epoch"], bid, "Z9",
        [{"asin": "e001", "title": "E", "price": "p", "image": "i"}])
    assert got == {"accepted": True, "stale": False, "discovered": 1,
                   "new_asins": 1, "detail_tasks_created": 0}
    async with pgdb.read() as rc, rc.execute(
            "SELECT COUNT(*) FROM tasks WHERE batch_id=? AND task_type='asin'",
            (bid,)) as c:
        assert (await c.fetchone())[0] == 0

    # with_detail 且 ASIN 已有 detail 任务 -> detail_tasks_created 只数**真正落库**的行
    bid2, _ = await pgdb.create_seller_batch("s3", ["a1", "a2"], "with_detail",
                                             "10001", False)
    pulled = {t["asin"]: t for t in await pgdb.pull_tasks("wk1", 10)}
    t1, t2 = pulled["A1"], pulled["A2"]
    assert (await pgdb.accept_seller_discovery_result(
        t1["id"], "wk1", t1["lease_epoch"], bid2, "A1",
        [{"asin": "x1", "title": "", "price": "", "image": ""},
         {"asin": "x2", "title": "", "price": "", "image": ""}]
    ))["detail_tasks_created"] == 2
    assert (await pgdb.accept_seller_discovery_result(
        t2["id"], "wk1", t2["lease_epoch"], bid2, "A2",
        [{"asin": "x1", "title": "", "price": "", "image": ""},
         {"asin": "x9", "title": "", "price": "", "image": ""}]
    )) == {"accepted": True, "stale": False, "discovered": 2, "new_asins": 2,
           "detail_tasks_created": 1}

    # is_new=0 的分支：asin_data 里已有该 ASIN
    await pgdb._db.execute("INSERT INTO asin_data (asin, title) VALUES (?, ?)",
                           ("X5", "t"))
    bid3, _ = await pgdb.create_seller_batch("s4", ["b1"], "with_detail", "10001", False)
    t3 = {t["asin"]: t for t in await pgdb.pull_tasks("wk1", 10)}["B1"]
    await pgdb.accept_seller_discovery_result(
        t3["id"], "wk1", t3["lease_epoch"], bid3, "B1",
        [{"asin": "x5", "title": "", "price": "", "image": ""}])
    async with pgdb.read() as rc, rc.execute(
            "SELECT is_new FROM batch_asins WHERE batch_id=? AND asin=?",
            (bid3, "X5")) as c:
        assert (await c.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_get_seller_batch_progress(pgdb):
    empty = await pgdb.get_seller_batch_progress(999999)
    assert empty == {
        "batch_id": 999999,
        "discover": {"pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0},
        "detail": {"pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0},
        "discovered_asins": 0,
        "discover_mode": None,
    }

    bid, _ = await pgdb.create_seller_batch("s1", ["a1", "a2"], "with_detail",
                                            "10001", False)
    pulled = {t["asin"]: t for t in await pgdb.pull_tasks("wk1", 10)}
    t1 = pulled["A1"]
    await pgdb.accept_seller_discovery_result(
        t1["id"], "wk1", t1["lease_epoch"], bid, "A1",
        [{"asin": "d1", "title": "", "price": "", "image": ""},
         {"asin": "d2", "title": "", "price": "", "image": ""}])

    prog = await pgdb.get_seller_batch_progress(bid)
    assert prog["batch_id"] == bid
    assert prog["discover_mode"] == "with_detail"
    assert prog["discover"] == {"pending": 0, "processing": 1, "done": 1,
                                "failed": 0, "total": 2}
    assert prog["detail"] == {"pending": 2, "processing": 0, "done": 0,
                              "failed": 0, "total": 2}
    assert prog["discovered_asins"] == 2
    assert isinstance(prog["discovered_asins"], int)


# ============================================================
# 变体展开
# ============================================================

@pytest.mark.asyncio
async def test_expand_batch_variants(pgdb):
    assert await pgdb.expand_batch_variants(999999) == 0

    off = await pgdb.create_batch("off")
    await pgdb.create_tasks(off, ["V01"], "10001", False)
    assert await pgdb.expand_batch_variants(off) == 0          # 开关没开

    bid = await pgdb.create_batch("on", expand_variants=True)
    await pgdb.create_tasks(bid, ["V01", "V02"], "90210", False)
    assert await pgdb.expand_batch_variants(bid) == 0          # 还没有 asin_data

    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title, variation_asins) VALUES (?, ?, ?)",
        ("V01", "t", "V02, V03 ,V04,"))                        # 含空白与空项
    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title, variation_asins) VALUES (?, ?, ?)",
        ("V02", "t", ""))                                      # 空串不参与
    assert await pgdb.expand_batch_variants(bid) == 2          # V03/V04（V02 已在本批）
    # 收敛：第二轮必须是 0，否则 completion watcher 会死循环
    assert await pgdb.expand_batch_variants(bid) == 0

    async with pgdb.read() as rc, rc.execute(
            "SELECT asin, zip_code FROM tasks WHERE batch_id=? ORDER BY asin",
            (bid,)) as c:
        rows = [dict(r) for r in await c.fetchall()]
    assert [r["asin"] for r in rows] == ["V01", "V02", "V03", "V04"]
    assert all(r["zip_code"] == "90210" for r in rows)         # 继承本批已有 zip


@pytest.mark.asyncio
async def test_expand_batch_variants_family_cap(pgdb):
    """单族候选数 > VARIANT_EXPAND_FAMILY_CAP 的家族整族跳过。"""
    from common import config

    bid = await pgdb.create_batch("big", expand_variants=True)
    await pgdb.create_tasks(bid, ["W01", "W02"], "10001", False)
    big = ",".join("BIG%03d" % i for i in range(config.VARIANT_EXPAND_FAMILY_CAP + 2))
    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title, variation_asins) VALUES (?, ?, ?)",
        ("W01", "t", big))
    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title, variation_asins) VALUES (?, ?, ?)",
        ("W02", "t", "SMALL1,SMALL2"))
    # 超大族被跳过，正常小族照常展开
    assert await pgdb.expand_batch_variants(bid) == 2


@pytest.mark.asyncio
async def test_expand_batch_variants_inherits_screenshot_flag(pgdb):
    bid = await pgdb.create_batch("ss", needs_screenshot=True, expand_variants=True)
    await pgdb.create_tasks(bid, ["X01"], "77002", True)
    await pgdb._db.execute(
        "INSERT INTO asin_data (asin, title, variation_asins) VALUES (?, ?, ?)",
        ("X01", "t", "X02,X03"))
    assert await pgdb.expand_batch_variants(bid) == 2

    async with pgdb.read() as rc:
        async with rc.execute(
                "SELECT asin, needs_screenshot FROM tasks WHERE batch_id=? ORDER BY asin",
                (bid,)) as c:
            assert all(r["needs_screenshot"] == 1 for r in await c.fetchall())
        async with rc.execute(
                "SELECT asin FROM screenshots WHERE batch_id=? ORDER BY asin", (bid,)) as c:
            assert [r["asin"] for r in await c.fetchall()] == ["X01", "X02", "X03"]


# ============================================================
# 参数类型强转（asyncpg 严格，SQLite 靠列 affinity 静默转换）
# ============================================================

@pytest.mark.asyncio
async def test_json_parameter_types_are_coerced(pgdb):
    """HTTP 边界收的是任意 JSON —— str 的 id、int 的 zip 都到得了这里。

    黄金夹具全是字符串，抓不到这一类；SQLite 会静默按列 affinity 转换，
    asyncpg 直接 DataError（→ 500，任务卡在 processing 直到回收）。
    """
    bid, _ = await pgdb.create_seller_batch("s1", ["q1"], "with_detail", 60601, False)
    task = (await pgdb.pull_tasks("wk1", 10))[0]

    got = await pgdb.accept_seller_discovery_result(
        str(task["id"]), "wk1", str(task["lease_epoch"]), str(bid), "Q1",
        [{"asin": "f001", "title": "F", "price": "9", "image": "i"}], {"pages_scanned": 1})
    assert got["accepted"] is True and got["detail_tasks_created"] == 1

    async with pgdb.read() as rc, rc.execute(
            "SELECT zip_code FROM tasks WHERE batch_id=? ORDER BY id LIMIT 1", (bid,)) as c:
        assert (await c.fetchone())[0] == "60601"      # int 60601 -> TEXT '60601'

    assert (await pgdb.get_screenshot_progress(str(bid)))["total"] == 0
    assert len(await pgdb.get_pending_screenshots(str(bid), "50")) == 0
    assert await pgdb._get_done_screenshot_path("F001", str(bid)) is None
    assert await pgdb.expand_batch_variants(str(bid)) == 0
    assert (await pgdb.get_seller_batch_progress(str(bid)))["discover_mode"] == "with_detail"
    assert await pgdb.update_screenshot_status("F001", str(bid), "processing") is False
