"""两个删除端点的替代网（Phase 3.6）。

**为什么要单独有这份文件**：``POST /api/results/delete-by-file`` 与
``DELETE /api/results`` 在黄金 78 步里一步都没有，Phase 2.4 的错误路径扩容
也没覆盖它们 —— 它们不是错误路径，是**破坏性**的正常路径。于是把它们从
``server/app.py`` 拆到 ``server/api/results.py`` 这一步没有黄金网可依赖，
这份用例就是那张网。

写成 ``unittest.TestCase`` 而不是裸 ``def test_*``，是因为门禁里
``python -m unittest discover -s tests`` 是两列之一，它的加载器只认
``TestCase`` 子类 —— 裸函数用例在那一列会被静默跳过。

覆盖面按「拆分会怎么坏」选，不是按行覆盖率：

  * **三个上传分派分支**（txt / csv / xlsx）都要走到 —— handler 里是
    ``filename.endswith`` 分派，搬模块时最容易漏掉 ``csv`` / ``io`` /
    ``openpyxl`` 的 import，而漏了只有走到那个分支才会炸。
  * **`search` 的 LIKE 通配符与转义**（``%`` / ``_`` / 反斜杠）——
    那段 WHERE 是 handler 自己拼的 f-string，PG 侧的语义靠
    ``common/pgdb/pool.py`` 的 ``_LIKE_QMARK_RE`` **按字面文本**改写成
    ``ESCAPE ''`` 撑着（D-16）。把 ``LIKE ?`` 写法动一下、或者三段 OR 拆开，
    正则就不再命中，PG 当场换语义而两边都不报错。本文件跟着 ``DB_BACKEND``
    走，两列各验一遍，就是钉这一条。
  * **三条筛选路径的分叉**：纯 ``batch_id``（覆盖 target_asins）、
    ``batch_id`` + 其他条件（取交集）、无 ``batch_id``。三条在 handler 里
    是 if/elif，搬运时最容易把 ``has_explicit_filter`` 的顺序弄反。
  * **两条 4xx**：``HTTPException`` 现在是从新模块抛的，状态码与 detail
    原文都得在。
  * **已知的坑也钉住**：``deleted`` 计的是 ``len(asin_list)``（请求里的
    ASIN 数），**不是**真删掉的行数 —— 删一个库里没有的 ASIN 也回
    ``deleted: 1``。这是既有行为，不是本步引入的；钉住它是为了让将来
    3.8 批 (3) 收口时，如果顺手"修"了它，这里会红。
"""
from __future__ import annotations

import io
import unittest

import openpyxl

from tests.golden.harness import isolated_server

_BATCH_A = "rdel_a"
_BATCH_B = "rdel_b"
_A = ["B00RDEL001", "B00RDEL002", "B00RDEL003", "B00RDEL004"]
_B = ["B00RDEL005", "B00RDEL006"]

# 标题里故意埋 LIKE 元字符：% / _ / 反斜杠。
_TITLES = {
    "B00RDEL001": "Alpha 100% pure",
    "B00RDEL002": "Beta_under score",
    "B00RDEL003": "Gamma back\\slash",
    "B00RDEL004": "Delta plain",
    "B00RDEL005": "Eps 100% pure",
    "B00RDEL006": "Zeta plain",
}


def _seed(client):
    """建两个批次并把结果写进 asin_data，返回 {batch_name: batch_id}。"""
    ids = {}
    for name, asins, zc in ((_BATCH_A, _A, "10001"), (_BATCH_B, _B, "60601")):
        r = client.post(
            "/api/upload",
            files={"file": (f"{name}.txt", "\n".join(asins).encode(), "text/plain")},
            data={"batch_name": name, "zip_code": zc, "needs_screenshot": "false"},
        )
        assert r.status_code == 200, r.text
        ids[name] = r.json()["batch_id"]
    r = client.get("/api/tasks/pull", params={"worker_id": "rdel-w", "count": 50,
                                              "enable_screenshot": "false"})
    assert r.status_code == 200, r.text
    tasks = r.json()["tasks"]
    assert len(tasks) == len(_A) + len(_B), tasks
    r = client.post("/api/tasks/result/batch", json={"results": [
        {"task_id": t["id"], "batch_id": t["batch_id"], "worker_id": "rdel-w",
         "lease_epoch": t["lease_epoch"], "success": True,
         "asin": t["asin"], "title": _TITLES[t["asin"]], "brand": "AcmeBrand",
         "price": "9.99", "stock": "5"}
        for t in tasks]})
    assert r.status_code == 200, r.text
    return ids


def _remaining(client):
    r = client.get("/api/results", params={"limit": 200})
    assert r.status_code == 200, r.text
    body = r.json()
    return sorted(i["asin"] for i in body.get("items", body.get("results", [])))


def _delete(client, body):
    return client.request("DELETE", "/api/results", json=body)


def _delete_file(client, filename, payload, ctype="text/plain"):
    return client.post("/api/results/delete-by-file",
                       files={"file": (filename, payload, ctype)})


def _xlsx(rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class DeleteResultsTests(unittest.TestCase):
    """DELETE /api/results —— 三条筛选路径 + LIKE 元字符 + 上限。"""

    def test_delete_by_asins(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"asins": ["B00RDEL001", "B00RDEL005"]})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL002", "B00RDEL003", "B00RDEL004", "B00RDEL006"])

    def test_delete_by_batch_id_only(self):
        """纯 batch_id：target_asins 被整批覆盖，另一个批次一行不掉。"""
        with isolated_server() as (client, _ctx):
            ids = _seed(client)
            r = _delete(client, {"batch_id": ids[_BATCH_A]})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 4})
            self.assertEqual(_remaining(client), ["B00RDEL005", "B00RDEL006"])

    def test_delete_batch_id_intersects_asins(self):
        """batch_id + asins：取交集，不在该批次里的 ASIN 不受影响。"""
        with isolated_server() as (client, _ctx):
            ids = _seed(client)
            r = _delete(client, {"batch_id": ids[_BATCH_B],
                                 "asins": ["B00RDEL001", "B00RDEL005"]})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 1})
            self.assertEqual(_remaining(client),
                             ["B00RDEL001", "B00RDEL002", "B00RDEL003",
                              "B00RDEL004", "B00RDEL006"])

    def test_delete_by_search_matches_title(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"search": "Alpha"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 1})
            self.assertNotIn("B00RDEL001", _remaining(client))

    def test_delete_by_search_percent_is_literal(self):
        """``%`` 必须当字面量：只命中标题里真含 '100%' 的两条，不是全表。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"search": "100%"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL002", "B00RDEL003", "B00RDEL004", "B00RDEL006"])

    def test_delete_by_search_underscore_is_literal(self):
        """``_`` 是 LIKE 的单字符通配符；这里必须只命中真含下划线的那条。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"search": "Beta_under"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 1})
            self.assertNotIn("B00RDEL002", _remaining(client))

    def test_delete_by_search_backslash(self):
        """反斜杠：两个后端必须删同一条（D-16 记的那处不一致的守卫）。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"search": "back\\slash"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 1})
            self.assertEqual(_remaining(client),
                             ["B00RDEL001", "B00RDEL002", "B00RDEL004",
                              "B00RDEL005", "B00RDEL006"])

    def test_delete_by_search_comma_is_or(self):
        """逗号分词是 OR，不是 AND。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"search": "Alpha,Zeta"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL002", "B00RDEL003", "B00RDEL004", "B00RDEL005"])

    def test_delete_empty_body_deletes_nothing(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 0})
            self.assertEqual(len(_remaining(client)), 6)

    def test_delete_unknown_asin_still_reports_one(self):
        """既有行为：deleted 计的是请求里的 ASIN 数，不是真删掉的行数。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"asins": ["B00NOTHERE1"]})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(), {"ok": True, "deleted": 1})
            self.assertEqual(len(_remaining(client)), 6)

    def test_delete_asins_over_limit_is_400(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete(client, {"asins": ["B00X%09d" % i for i in range(100001)]})
            self.assertEqual(r.status_code, 400, r.text)
            self.assertEqual(r.json()["detail"],
                             "asins 列表过长（100001），单次上限 100000")
            self.assertEqual(len(_remaining(client)), 6)


class DeleteByFileTests(unittest.TestCase):
    """POST /api/results/delete-by-file —— 三个分派分支 + 去重 + 4xx。"""

    def test_txt_branch_dedupes(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete_file(client, "x.txt",
                             b"B00RDEL001\nB00RDEL002\nB00RDEL001\ngarbage\n")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(),
                             {"ok": True, "deleted": 2, "asin_count": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL003", "B00RDEL004", "B00RDEL005", "B00RDEL006"])

    def test_csv_branch(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete_file(client, "x.csv",
                             b"asin,note\nB00RDEL003,x\nB00RDEL005,y\n", "text/csv")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(),
                             {"ok": True, "deleted": 2, "asin_count": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL001", "B00RDEL002", "B00RDEL004", "B00RDEL006"])

    def test_xlsx_branch(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete_file(
                client, "x.xlsx", _xlsx([["asin"], ["B00RDEL004"], ["B00RDEL006"]]),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(),
                             {"ok": True, "deleted": 2, "asin_count": 2})
            self.assertEqual(_remaining(client),
                             ["B00RDEL001", "B00RDEL002", "B00RDEL003", "B00RDEL005"])

    def test_no_valid_asin_is_400(self):
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete_file(client, "x.txt", b"nothing here\n")
            self.assertEqual(r.status_code, 400, r.text)
            self.assertEqual(r.json()["detail"], "文件中未找到有效 ASIN")
            self.assertEqual(len(_remaining(client)), 6)

    def test_unknown_asin_still_reports_one(self):
        """同上：asin_count / deleted 都只是请求里的 ASIN 数。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            r = _delete_file(client, "x.txt", b"B00ZZZZZZZ\n")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json(),
                             {"ok": True, "deleted": 1, "asin_count": 1})
            self.assertEqual(len(_remaining(client)), 6)


if __name__ == "__main__":
    unittest.main()
