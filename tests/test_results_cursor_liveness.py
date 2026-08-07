"""``GET /api/results`` 的翻页**活性**：游标必须严格前进，循环必须终止。

------------------------------------------------------------------------
为什么需要这一条
------------------------------------------------------------------------
erpAPI 那边在旧系统上踩过「``next_cursor`` 不前进 → 翻页卡死」：游标停在同一个
值上，消费者要么无限循环，要么反复拿到同一页还以为在前进。

我们这边的实现是**严格**不等号（``common/database.py`` 与
``common/pgdb/results_read.py`` 的 keyset 谓词：``direction=next`` 是
``d.id < ?``、``direction=prev`` 是 ``d.id > ?``，游标取自本页边界行的 id），
所以这个缺陷在结构上不成立 —— **但在这一步之前没有任何用例钉住它**。
黄金基线也钉不住：它只翻了一页（``results_page2`` / ``results_page_prev``
各一次），一个「游标不前进」的实现在只翻一页时表现完全正常。

所以这里翻**满整个结果集**，并把两条性质写成断言：

  1. **游标严格单调**：``direction=next`` 走的是 ``ORDER BY d.id DESC``，
     所以「严格前进」的形式是 ``next_cursor`` **严格递减**；
     ``direction=prev`` 是 ``ASC``，``prev_cursor`` **严格递增**。
     把不等号从 ``<`` 放宽成 ``<=`` 会让边界行重复出现，这一条当场红。
  2. **循环一定终止**：给一个硬轮次上限（页数 + 余量），超了就判定卡死并失败，
     而不是让用例挂在那里等超时 —— 「卡死」必须以一条读得懂的断言出现。

外加两条完整性断言（它们才是让上面两条不至于空转的东西）：
每一页的 id 集合两两不相交、并集恰好等于全集。游标只要有一次不前进，
要么翻不完（并集缺行），要么重复（交集非空）。

------------------------------------------------------------------------
覆盖面上的顺带收益
------------------------------------------------------------------------
``tests/golden/run.py`` 自己列出的基线盲区第一条就是「六次 ``/api/results``
一次都没传 ``batch_id``」。这里第二个用例带 ``batch_id`` 翻满全程，
把 keyset 谓词与 ``batch_asins`` JOIN 的组合也盖上了。

写成 ``unittest.TestCase``：门禁里 ``unittest discover`` 只认 TestCase 子类，
裸 ``test_`` 函数在那两列会被静默跳过。跟着 ``DB_BACKEND`` 走 ``isolated_server``，
四道 runner 门（pytest × 2 后端、unittest × 2 后端）全都盖得到。
"""
from __future__ import annotations

import unittest

from tests.golden.harness import isolated_server

_BATCH = "cursor_live_a"
#: 7 行 / 每页 2 行 = 4 页（最后一页只有 1 行）。奇数是刻意的：
#: 整除时「多取一条判 has_more」的边界不会被走到。
_ASINS = [f"B00PAGE{i:03d}" for i in range(1, 8)]
_PAGE = 2

#: 另一个批次，只是为了让「不带 batch_id 的全集」严格大于「带 batch_id 的子集」。
_BATCH_OTHER = "cursor_live_b"
_ASINS_OTHER = ["B00PAGE900", "B00PAGE901"]


def _seed(client, batch_name, asins):
    r = client.post(
        "/api/upload",
        files={"file": (f"{batch_name}.txt", "\n".join(asins).encode(), "text/plain")},
        data={"batch_name": batch_name, "zip_code": "10001",
              "needs_screenshot": "false"},
    )
    assert r.status_code == 200, r.text
    batch_id = r.json()["batch_id"]
    r = client.get("/api/tasks/pull", params={"worker_id": "cursor-w", "count": 100,
                                              "enable_screenshot": "false"})
    assert r.status_code == 200, r.text
    tasks = [t for t in r.json()["tasks"] if t["batch_id"] == batch_id]
    assert len(tasks) == len(asins), tasks
    r = client.post("/api/tasks/result/batch", json={"results": [
        {"task_id": t["id"], "batch_id": t["batch_id"], "worker_id": "cursor-w",
         "lease_epoch": t["lease_epoch"], "success": True,
         "asin": t["asin"], "title": f"Cursor {t['asin']}", "brand": "CursorBrand"}
        for t in tasks]})
    assert r.status_code == 200, r.text
    return batch_id


class ResultsCursorLivenessTests(unittest.TestCase):

    #: 硬轮次上限的余量。页数算得出来，多给 3 轮，超了就是没在前进。
    _SLACK = 3

    def _walk(self, client, *, direction, start_cursor=None, batch_id=None):
        """翻满全程，返回 ``(每页 id 列表, 用过的游标序列, 轮次数)``。

        终止条件只认 ``has_more``；轮次上限是**看门狗**，它响了就等于卡死。
        """
        cursor = start_cursor
        cursor_key = "next_cursor" if direction == "next" else "prev_cursor"
        pages, cursors, rounds = [], [], 0
        max_rounds = -(-self._expected_total // _PAGE) + self._SLACK

        while True:
            rounds += 1
            self.assertLessEqual(
                rounds, max_rounds,
                f"direction={direction} 翻了 {rounds} 轮还没到头（上限 {max_rounds}，"
                f"总共 {self._expected_total} 行 / 每页 {_PAGE}）——游标没有前进，"
                f"翻页卡死。已用过的游标: {cursors}")

            params = {"limit": _PAGE, "direction": direction}
            if cursor is not None:
                params["cursor"] = cursor
            if batch_id is not None:
                params["batch_id"] = batch_id
            resp = client.get("/api/results", params=params)
            self.assertEqual(resp.status_code, 200, resp.text)
            body = resp.json()

            pages.append([item["id"] for item in body["items"]])
            self.assertEqual(body["total"], self._expected_total,
                             "total 是不含游标谓词的全集计数，翻页途中不该变")

            if not body["has_more"]:
                break

            nxt = body[cursor_key]
            self.assertIsNotNone(
                nxt, f"has_more=True 却没有 {cursor_key}，调用方无从继续")
            if cursors:
                if direction == "next":
                    self.assertLess(
                        nxt, cursors[-1],
                        f"next_cursor 必须严格递减（id DESC 序下的『前进』）："
                        f"{cursors[-1]} -> {nxt}")
                else:
                    self.assertGreater(
                        nxt, cursors[-1],
                        f"prev_cursor 必须严格递增：{cursors[-1]} -> {nxt}")
            cursors.append(nxt)
            cursor = nxt

        return pages, cursors, rounds

    def _assert_partition(self, pages, expected_ids, *, where):
        flat = [i for page in pages for i in page]
        self.assertEqual(
            len(flat), len(set(flat)),
            f"{where}: 有行被翻到两次 —— 游标少前进了一格（`<` 写成了 `<=`）。"
            f"逐页: {pages}")
        self.assertEqual(
            set(flat), set(expected_ids),
            f"{where}: 翻完全程的并集不等于全集，说明中途漏页或提前终止。"
            f"逐页: {pages}")

    # ---------------- 全集，direction=next ----------------

    def test_next_cursor_strictly_advances_and_terminates(self):
        self._expected_total = len(_ASINS) + len(_ASINS_OTHER)
        with isolated_server() as (client, _ctx):
            _seed(client, _BATCH, _ASINS)
            _seed(client, _BATCH_OTHER, _ASINS_OTHER)
            all_ids = [i["id"] for i in
                       client.get("/api/results", params={"limit": 200}).json()["items"]]
            self.assertEqual(len(all_ids), self._expected_total)

            pages, cursors, rounds = self._walk(client, direction="next")

        self.assertEqual(rounds, -(-self._expected_total // _PAGE),
                         f"页数不对，逐页: {pages}")
        self.assertEqual(cursors, sorted(cursors, reverse=True),
                         f"游标序列必须严格递减: {cursors}")
        self.assertEqual(len(cursors), len(set(cursors)), f"游标重复了: {cursors}")
        self._assert_partition(pages, all_ids, where="direction=next")
        # 全集是 id DESC，所以第一页的第一行就是最大 id
        self.assertEqual(pages[0][0], max(all_ids))

    # ---------------- direction=prev（反向也必须严格前进） ----------------

    def test_prev_cursor_strictly_advances_and_terminates(self):
        """从最后一页往回翻。反向的谓词是 ``d.id > ?``，同样必须是严格的。"""
        self._expected_total = len(_ASINS) + len(_ASINS_OTHER)
        with isolated_server() as (client, _ctx):
            _seed(client, _BATCH, _ASINS)
            _seed(client, _BATCH_OTHER, _ASINS_OTHER)
            all_ids = [i["id"] for i in
                       client.get("/api/results", params={"limit": 200}).json()["items"]]

            # 先用 next 翻到最后一页，拿它的 prev_cursor 当反向起点
            forward, _c, _r = self._walk(client, direction="next")
            last_page_min = min(forward[-1])

            pages, cursors, rounds = self._walk(
                client, direction="prev", start_cursor=last_page_min)

        self.assertGreater(rounds, 1, "反向只翻了一轮，用例没测到东西")
        self.assertEqual(cursors, sorted(cursors), f"游标序列必须严格递增: {cursors}")
        self.assertEqual(len(cursors), len(set(cursors)), f"游标重复了: {cursors}")
        # 反向起点是最后一页的最小 id，谓词是**严格** `>`，所以那一行不会再出现
        self._assert_partition(
            pages, [i for i in all_ids if i > last_page_min], where="direction=prev")

    # ---------------- 带 batch_id 的子集（黄金基线的盲区） ----------------

    def test_next_cursor_advances_with_a_batch_filter(self):
        """keyset 谓词 + ``batch_asins`` JOIN 的组合。

        ``tests/golden/run.py`` 自己写明「六次 /api/results 一次都没传 batch_id」，
        这一条把那个盲区补上：筛选之后仍然必须翻得完、且不重复。
        """
        self._expected_total = len(_ASINS)
        with isolated_server() as (client, _ctx):
            batch_id = _seed(client, _BATCH, _ASINS)
            _seed(client, _BATCH_OTHER, _ASINS_OTHER)
            scoped = client.get("/api/results",
                                params={"limit": 200, "batch_id": batch_id}).json()
            scoped_ids = [i["id"] for i in scoped["items"]]
            self.assertEqual(len(scoped_ids), len(_ASINS),
                             "batch_id 筛选没起作用，后面的断言就没意义了")

            pages, cursors, rounds = self._walk(
                client, direction="next", batch_id=batch_id)

        self.assertEqual(rounds, -(-len(_ASINS) // _PAGE), f"逐页: {pages}")
        self.assertEqual(cursors, sorted(cursors, reverse=True), f"{cursors}")
        self._assert_partition(pages, scoped_ids, where="batch_id 子集")

    # ---------------- 单页上限 ----------------

    def test_page_size_ceiling_rejects_not_truncates(self):
        """单页上限是**拒绝**（422）而不是静默截断 —— 这是与旧 erpAPI 的语义差别。

        旧系统那条是「单页上限 200 是服务端硬约束，调大无效」，"无效"听起来像
        静默截断；我们这边是 ``Query(50, le=MAX_PAGE_LIMIT)``，超限当场 422。
        静默截断才是危险的：消费者会把「只回了 N 条」读成「只有 N 条」。
        代价是调用方必须**自己分页**，不能"传个大数看它给多少"。

        ⚠ 这条钉的是**语义**（拒绝 / 不截断），不是那个具体数字。上限本身
        直接从 ``MAX_PAGE_LIMIT`` 读 —— 数字的出处、以及什么时候该重估，
        写在 ``server/api/results.py`` 里该常量上方。上限改了这条不该红；
        改成静默截断（比如换 ``min(limit, MAX_PAGE_LIMIT)``）必须红。
        """
        from server.api.results import MAX_PAGE_LIMIT

        with isolated_server() as (client, _ctx):
            ok = client.get("/api/results", params={"limit": MAX_PAGE_LIMIT})
            self.assertEqual(ok.status_code, 200, ok.text)
            over = client.get("/api/results", params={"limit": MAX_PAGE_LIMIT + 1})
            self.assertEqual(over.status_code, 422, over.text)
            # 「拒绝」而不是「截断」：422 的响应体是校验错误、不是一页结果
            self.assertNotIn("items", over.json())

    def test_page_size_ceiling_is_1000(self):
        """上限值本身钉一条，改动它必须是**有意**的（并连带重录 openapi 基线）。

        ``le=`` 会渲染进 ``/openapi.json`` 的 ``maximum``，而那份 schema 是
        黄金基线逐字节钉死的一步 —— 所以这个数字是对外契约，不是实现细节。
        """
        from server.api.results import MAX_PAGE_LIMIT

        self.assertEqual(MAX_PAGE_LIMIT, 1000)
        with isolated_server() as (client, _ctx):
            schema = client.get("/openapi.json").json()
            params = schema["paths"]["/api/results"]["get"]["parameters"]
            limit_param = next(p for p in params if p["name"] == "limit")
            self.assertEqual(limit_param["schema"]["maximum"], MAX_PAGE_LIMIT)


if __name__ == "__main__":
    unittest.main()
