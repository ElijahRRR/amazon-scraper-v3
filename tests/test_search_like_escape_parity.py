"""``search`` 的 LIKE 语义：**读路径与删除路径必须选中同一批行**。

------------------------------------------------------------------------
这份文件是 Phase 3.8 批 (3) 的前置条件（计划 §A7）
------------------------------------------------------------------------

同一个 ``search`` 字符串今天走两条完全不同的 SQL：

  * **读**：``GET /api/results?search=X`` -> ``db.get_results(search=X)``。
    SQLite 侧在 term ≥ 3 字符时走 **FTS5 trigram** 子查询
    （``common/database.py`` 的快路径），< 3 字符才退回 LIKE；
    PG 侧走 ``common/pgdb/results_read.py`` 的 ``_TERM_OR``
    （``ascii_lower(col) LIKE ascii_lower(?) ESCAPE ''``）。
  * **删**：``DELETE /api/results {"search": X}`` -> handler 自己拼的 f-string
    ``(d.asin LIKE ? OR d.title LIKE ? OR d.brand LIKE ?)``，PG 侧靠
    ``common/pgdb/pool.py`` 的 ``_LIKE_QMARK_RE`` 按**字面文本**改写成
    带 ``ESCAPE ''`` 的形式（D-16）。

``tests/test_results_delete_api.py`` 已经钉住了删除路径**自己**的行为，
但没有任何一条用例问过「读出来的和删掉的是不是同一批」。而这正是 3.8 批 (3)
要合并的两条路径 —— 合并之前必须先知道它们今天是否一致，否则「收进同一个
db 方法」就只是把一个已知不一致搬了个家。

所以每条用例的形状都是：

    1. GET  /api/results?search=X&limit=200  -> 读出来的 ASIN 集合
    2. DELETE /api/results {"search": X}     -> 删除前后的差集
    3. 断言 1 == 2

跟着 ``DB_BACKEND`` 走，门禁两列各跑一遍，所以它同时也是双后端用例：
两个后端各自内部一致 **且** 两边的绝对结果都写在断言里（不是互相比较，
而是各自与同一份期望比较），任一侧改语义都会红。

写成 ``unittest.TestCase``：门禁里 ``python -m unittest discover -s tests``
的加载器只认 TestCase 子类。
"""
from __future__ import annotations

import unittest

from tests.golden.harness import isolated_server

_BATCH = "esc_a"

# 标题里埋满 LIKE 元字符。注意 Python 源码里 "\\" 是**一个**反斜杠字符。
_TITLES = {
    "B00ESC0001": "Alpha 100% pure",
    "B00ESC0002": "Beta_under score",
    "B00ESC0003": "Gamma back\\slash",
    "B00ESC0004": "Delta backslash",       # 没有反斜杠，用来抓「转义吃掉了它」
    "B00ESC0005": "Eps 100 percent",       # 没有 %，用来抓「% 当通配符了」
    "B00ESC0006": "Zeta BetaXunder tail",  # 用来抓「_ 当通配符了」
    "B00ESC0007": "Eta plain",
}
_ASINS = sorted(_TITLES)


def _seed(client):
    r = client.post(
        "/api/upload",
        files={"file": (f"{_BATCH}.txt", "\n".join(_ASINS).encode(), "text/plain")},
        data={"batch_name": _BATCH, "zip_code": "10001", "needs_screenshot": "false"},
    )
    assert r.status_code == 200, r.text
    r = client.get("/api/tasks/pull", params={"worker_id": "esc-w", "count": 50,
                                              "enable_screenshot": "false"})
    assert r.status_code == 200, r.text
    tasks = r.json()["tasks"]
    assert len(tasks) == len(_ASINS), tasks
    r = client.post("/api/tasks/result/batch", json={"results": [
        {"task_id": t["id"], "batch_id": t["batch_id"], "worker_id": "esc-w",
         "lease_epoch": t["lease_epoch"], "success": True,
         "asin": t["asin"], "title": _TITLES[t["asin"]], "brand": "AcmeBrand",
         "price": "9.99", "stock": "5"}
        for t in tasks]})
    assert r.status_code == 200, r.text


def _all_asins(client):
    r = client.get("/api/results", params={"limit": 200})
    assert r.status_code == 200, r.text
    return {i["asin"] for i in r.json()["items"]}


def _read_hits(client, term):
    r = client.get("/api/results", params={"search": term, "limit": 200})
    assert r.status_code == 200, r.text
    return {i["asin"] for i in r.json()["items"]}


def _delete_hits(client, term):
    before = _all_asins(client)
    r = client.request("DELETE", "/api/results", json={"search": term})
    assert r.status_code == 200, r.text
    return before - _all_asins(client)


class SearchLikeEscapeParityTests(unittest.TestCase):
    """读路径 ∩ 删除路径：同一个 search 必须选中同一批行。"""

    def _check(self, term, expected):
        """读命中 == 删命中 == expected。两次独立起服务，互不污染。"""
        with isolated_server() as (client, _ctx):
            _seed(client)
            read = _read_hits(client, term)
        with isolated_server() as (client, _ctx):
            _seed(client)
            deleted = _delete_hits(client, term)
        self.assertEqual(
            read, deleted,
            f"search={term!r}: 读路径选中 {sorted(read)}，删除路径删掉 "
            f"{sorted(deleted)} —— 同一个字符串在两条路径上语义不同")
        self.assertEqual(read, set(expected), f"search={term!r} 读路径")
        self.assertEqual(deleted, set(expected), f"search={term!r} 删除路径")

    # ---------- 反斜杠（D-16 那条）----------

    def test_backslash_term(self):
        """``back\\slash``：只命中标题里真有反斜杠的那条。

        若 PG 把反斜杠当转义字符，它会去匹配 ``backslash``（B00ESC0004）。
        """
        self._check("back\\slash", ["B00ESC0003"])

    def test_lone_backslash(self):
        """孤立反斜杠：模式是 ``%\\%``。

        SQLite 的 LIKE 没有转义字符，所以它是字面量、命中一条。
        PG 若开着默认转义，``%\\%`` 是「以 % 结尾的转义序列」——要么 0 行，
        要么直接 InvalidEscapeSequenceError。
        """
        self._check("\\", ["B00ESC0003"])

    # ---------- 百分号 ----------

    def test_percent_is_a_wildcard(self):
        """``%`` **不是**字面量：模式 ``%100%%`` 里那个用户输入的 ``%`` 照样
        是通配符，所以 ``100%`` 等价于「含 100」，``Eps 100 percent`` 也命中。

        这是既有行为（``_like_pattern`` 的 docstring 明说了「``%`` 和 ``_``
        故意不转义」），本轮不改它 —— 这里钉的是**两条路径一样地**这么干。
        （``tests/test_results_delete_api.py`` 那条 ``..._percent_is_literal``
        名字起错了：它的两条数据都含 "100"，两种语义都能过。）
        """
        self._check("100%", ["B00ESC0001", "B00ESC0005"])

    def test_lone_percent_matches_everything(self):
        """孤立 ``%``：模式 ``%%%``，是通配符，命中全部 —— 这是既有行为，
        不是本轮要改的（两条路径一起这样就行）。"""
        self._check("%", _ASINS)

    # ---------- 下划线 ----------

    def test_underscore_is_wildcard(self):
        """``_`` 是 LIKE 的单字符通配符，两条路径都必须**一样地**通配。

        ``Beta_under`` 因此同时命中 ``Beta_under score`` 与 ``BetaXunder tail``。
        钉住的是「两条路径行为相同」，不是「下划线该不该转义」。
        """
        self._check("Beta_under", ["B00ESC0002", "B00ESC0006"])

    def test_lone_underscore(self):
        """孤立 ``_``：模式 ``%_%`` = 任意非空串，命中全部。"""
        self._check("_", _ASINS)

    # ---------- 短 term（SQLite 侧走 LIKE 慢路径，不走 FTS5）----------

    def test_short_term_takes_the_like_slow_path(self):
        """< 3 字符的 term 在 SQLite 侧绕过 FTS5 —— 两条路径仍须一致。"""
        self._check("Al", ["B00ESC0001"])

    # ---------- 多 term ----------

    def test_comma_terms_mix_metachars(self):
        """逗号分词是 OR；一个带元字符、一个不带，混在一起也要一致。"""
        self._check("100%,back\\slash",
                    ["B00ESC0001", "B00ESC0005", "B00ESC0003"])


if __name__ == "__main__":
    unittest.main()
