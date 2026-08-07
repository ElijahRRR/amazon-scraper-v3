"""结果查询与删除（5 个端点）—— Phase 3.6 从 `server/app.py` 拆出。

    GET    /api/results
    GET    /api/results/{asin}
    GET    /api/changes/stats
    POST   /api/results/delete-by-file
    DELETE /api/results

------------------------------------------------------------------------
承重约束
------------------------------------------------------------------------

1. **模块级可变全局一个都不搬。** `db` 留在 `server/app.py`，这里一律
   `_srv().db`，**禁止 from-import** —— 三个 PG 夹具
   （`tests/pgdb/test_sync_api.py:38`、`test_retention.py`、
   `test_export_retention_window.py`）用 `monkeypatch.setattr(srv, "db", pgdb)`
   整体换掉这个属性，快照下来就补丁打空。
   `MAX_UPLOAD_BYTES` / `_remove_screenshot_files` 同理留在 `app.py`，
   这里走 `_srv().xxx`。

2. **本模块一条 SQL 都没有了**（Phase 3.8 批 (3)）。三段裸 SQL 收进了
   `db.find_asins_by_search(terms)` / `db.get_batch_asin_set(batch_id)` /
   `db.delete_asins(asins)`，两个后端各一份实现，名字都在
   `common/pgdb/__init__.py` 的 `PUBLIC_API` 里（导入期守卫）。

   收口之前这里是这么脆的：`api_delete_results` 里那段
   `"(d.asin LIKE ? OR d.title LIKE ? OR d.brand LIKE ?)"` + `f"%{term}%"`
   在 PG 侧的语义**不是** psycopg 原生的 —— 它靠 `common/pgdb/pool.py` 的
   `_LIKE_QMARK_RE` **按字面文本**把 `LIKE ?` 改写成带 `ESCAPE ''` 的形式
   （`LIKE_NO_ESCAPE`），才让反斜杠在两个后端表现一致（D-16）。
   把 `LIKE ?` 换个写法、把三段 OR 拆开、甚至只是多一个空格，正则就不再命中，
   PG 侧当场换语义而两边都不报错。现在删除路径与读路径引用**同一个常量**
   （SQLite 侧 `common/database.py:SEARCH_TERM_OR`、PG 侧
   `common/pgdb/results_read.py:_TERM_OR`，模式函数共用
   `_shared.search_like_pattern`），那条正则不再是删除路径的承重件。

   ⚠ **这条不一致今天是死的，别把它弄活**：
   `tests/test_search_like_escape_parity.py` 逐条比对「同一个 `search` 在
   GET /api/results 读出来的行集」与「DELETE /api/results 删掉的行集」，
   跟着 `DB_BACKEND` 走两列。实测把 `LIKE_NO_ESCAPE` 置空会让其中 3 条当场红。

3. **router 光秃**：`APIRouter()`，不带 `tags=` / `prefix=` /
   `include_in_schema`。`/openapi.json` 是黄金基线的一步、逐字节钉死。

4. **函数名 / docstring / 路径一个字不改** —— 它们被编码进 `operationId` /
   `summary` / `description` / `Body_api_delete_by_file_*` schema 名。

5. **路由匹配不受影响**：`GET /api/results/{asin}` 与
   `POST /api/results/delete-by-file` 方法不同（Starlette 对方法不匹配的
   路径记 PARTIAL 后继续找 FULL），`GET /api/results` 与
   `DELETE /api/results` 同理；两者在本文件里的先后顺序仍与拆分前一致。

6. **本模块的两个删除端点今天没有黄金网**（78 步一步没有，2.4 的错误路径扩容
   也没覆盖它们 —— 它们不是错误路径）。替代网是两份 `unittest.TestCase`
   （写成 TestCase 而非裸 `def test_*`，否则门禁里 `unittest discover`
   那两列会静默跳过），两个后端都跑：
     * `tests/test_results_delete_api.py` —— 端点行为（三个上传分派分支、
       三条筛选路径、两条 4xx、`deleted` 计的是请求里的 ASIN 数）。
     * `tests/test_search_like_escape_parity.py` —— 读路径与删除路径的
       LIKE 语义必须选中同一批行（批 (3) 的前置条件）。
"""

import csv
import io
import re

import openpyxl
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile


def _srv():
    from server import app as _s
    return _s


router = APIRouter()


# ==================== API: 结果查询 ====================

@router.get("/api/results")
async def api_results(batch_id: int = None,
                      cursor: int = None,
                      limit: int = Query(50, le=200),
                      search: str = None,
                      change_filter: str = "all",
                      direction: str = "next"):
    result = await _srv().db.get_results(
        batch_id=batch_id,
        cursor_id=cursor,
        limit=limit,
        search=search,
        change_filter=change_filter,
        direction=direction,
    )
    return result


@router.get("/api/results/{asin}")
async def api_result_detail(asin: str):
    db = _srv().db
    data = await db.get_result_by_asin(asin)
    if not data:
        raise HTTPException(404, f"ASIN {asin} 不存在")
    changes = await db.get_asin_changes(asin)
    return {"data": data, "changes": changes}


@router.get("/api/changes/stats")
async def api_change_stats(batch_id: int = None):
    return await _srv().db.get_change_stats(batch_id)


# ==================== API: 结果删除 ====================
#
# 这两条原本待在 app.py 的「诊断 / 侦查」节头之后 —— 节头骗人，按域它们属于
# 结果面。路径与上面三条同族（/api/results*），一起搬走。

@router.post("/api/results/delete-by-file")
async def api_delete_by_file(file: UploadFile = File(...)):
    """上传文件识别 ASIN 后删除对应数据"""
    _s = _srv()
    db = _s.db
    content = await file.read()
    if len(content) > _s.MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"文件过大：{len(content)//1024//1024}MB，上限 {_s.MAX_UPLOAD_BYTES//1024//1024}MB")
    filename = file.filename or ""

    asins = []
    if filename.endswith(".xlsx"):
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        try:
            ws = wb.active
            for row in ws.iter_rows(min_row=1, values_only=True):
                for cell in row:
                    if cell:
                        val = str(cell).strip().upper()
                        if re.match(r'^B[0-9A-Z]{9}$', val):
                            asins.append(val)
        finally:
            wb.close()
    elif filename.endswith(".csv"):
        text = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            for cell in row:
                val = cell.strip().upper()
                if re.match(r'^B[0-9A-Z]{9}$', val):
                    asins.append(val)
    else:
        text = content.decode("utf-8", errors="ignore")
        for line in text.splitlines():
            val = line.strip().upper()
            if re.match(r'^B[0-9A-Z]{9}$', val):
                asins.append(val)

    # 去重
    asin_list = list(dict.fromkeys(asins))
    if not asin_list:
        raise HTTPException(400, "文件中未找到有效 ASIN")

    screenshot_files = await db.delete_asins(asin_list)

    _s._remove_screenshot_files(screenshot_files)
    return {"ok": True, "deleted": len(asin_list), "asin_count": len(asin_list)}


@router.delete("/api/results")
async def api_delete_results(request: Request):
    """按条件删除采集结果"""
    _s = _srv()
    db = _s.db
    body = await request.json()
    batch_id = body.get("batch_id")
    asins = body.get("asins")  # list of ASIN strings
    search = body.get("search")  # fuzzy search string

    # 构建 ASIN 列表
    target_asins = set()
    has_explicit_filter = bool(asins or search)

    if asins:
        # asins 列表也做上限保护，避免一次投递百万级 ASIN 触发巨量 SELECT
        if len(asins) > 100000:
            raise HTTPException(400, f"asins 列表过长（{len(asins)}），单次上限 100000")
        target_asins.update(asins)

    if search:
        # 限长防 DoS：500 字符 / 10 关键词 / 单词 100 字符
        search = str(search)[:500]
        terms = [t.strip()[:100] for t in search.split(",") if t.strip()][:10]
        if terms:
            target_asins.update(await db.find_asins_by_search(terms))

    # 纯 batch_id（无 asins/search）→ 删除该批次所有 ASIN
    if batch_id and not has_explicit_filter:
        target_asins = set(await db.get_batch_asin_set(batch_id))
    # batch_id + 其他条件 → 取交集
    elif batch_id and target_asins:
        target_asins &= set(await db.get_batch_asin_set(batch_id))

    if not target_asins:
        return {"ok": True, "deleted": 0}

    asin_list = list(target_asins)
    screenshot_files = await db.delete_asins(asin_list)

    # 删除物理截图文件
    _s._remove_screenshot_files(screenshot_files)
    return {"ok": True, "deleted": len(asin_list)}
