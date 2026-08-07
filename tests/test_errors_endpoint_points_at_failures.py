"""``/api/batches/{batch_name}/errors`` 必须指得出 ``/failures``（且**只**在文档里指）。

------------------------------------------------------------------------
背景
------------------------------------------------------------------------
``/errors`` 有两条设计如此、调参数也没用的限制：``failed_tasks`` 硬截断到 200 条，
排序是 ``updated_at DESC, id DESC``（同一次批量提交共享同一个秒级 ``updated_at``，
所以「最近 200 条」在同一批内部不是一个有业务含义的切片）。完整明细在
``GET /api/batches/{batch_id}/failures``：按 id 取、``limit`` 上限 100000、不截断。

端点**不删**（对外清单尚未最终确认，删了就是把在跑的调用方打断），
但「该用哪个」必须写在调用方看得见的地方。

------------------------------------------------------------------------
为什么指向只写进 docstring，不写进响应体
------------------------------------------------------------------------
两条，第二条才是决定性的：

  * ``errors_batch_a`` 是黄金 78 步里的一步，body 逐字节钉着
    ``{"error_summary": [], "failed_tasks": []}``。往响应体加 deprecation 字段
    会改这一步的基线，而它与本轮要改的撞名行为毫无关系。
  * 更要紧的是那是**线上格式变更**。今天在跑的调用方拿到一个没见过的键，
    严格反序列化的那种会直接崩。为了一句提示冒这个险，不划算。

docstring 走 ``/openapi.json``（那**也**是黄金的一步，所以这次改动同样落在基线里），
但它只动 ``description`` 字符串、不动任何响应形状，对在跑的调用方是零风险。

写成 ``unittest.TestCase``：``unittest discover`` 只认 TestCase 子类。
本文件的断言与后端无关（纯静态 + 一次空批次的 GET），两个后端跑出来一样。
"""
from __future__ import annotations

import unittest

from tests.golden.harness import isolated_server

_ERRORS_PATH = "/api/batches/{batch_name}/errors"
_FAILURES_PATH = "/api/batches/{batch_id}/failures"


def _route(path):
    import server.app as srv

    for r in srv.app.routes:
        if getattr(r, "path", None) == path:
            return r
    raise AssertionError(f"路由不存在: {path}")


class ErrorsEndpointDeprecationPointerTests(unittest.TestCase):

    def test_docstring_points_at_the_failures_endpoint(self):
        """人读的那一半：docstring 里必须出现 ``/failures`` 的完整路径与 200 这个限制。"""
        from server.app import api_batch_errors

        doc = api_batch_errors.__doc__ or ""
        self.assertIn(_FAILURES_PATH, doc,
                      "/errors 的 docstring 必须给出 /failures 的完整路径")
        self.assertIn("200", doc, "必须写明 200 条这个硬截断，否则调用方不知道自己在丢数据")

    def test_the_pointer_actually_reaches_openapi(self):
        """机器读的那一半：FastAPI 把 docstring 放进 ``route.description``，
        也就是 ``/openapi.json`` 里那个 ``description`` —— 对外文档的真正落点。

        直接查 ``route.description`` 而不是调 ``app.openapi()``：后者会把生成的
        schema 缓存到 ``app.openapi_schema`` 上，而黄金夹具复用的正是同一个
        ``app`` 对象，测试期给它留下缓存是没必要的跨用例状态。
        """
        self.assertIn(_FAILURES_PATH, _route(_ERRORS_PATH).description or "")

    def test_failures_really_is_the_better_endpoint(self):
        """指过去之前先确认那边真的更好，否则这条指引就是骗人。"""
        import inspect

        from server.app import api_batch_failures

        params = inspect.signature(api_batch_failures).parameters
        # pydantic v2 下 `Query(..., le=N)` 的上限落在 `.metadata` 里的 `Le(le=N)`
        ceilings = [m.le for m in params["limit"].default.metadata
                    if hasattr(m, "le")]
        self.assertEqual(ceilings, [100000],
                         "/failures 的 limit 上限必须远大于 /errors 那个 200")
        self.assertIn("error_type", params)

    def test_the_response_shape_is_deliberately_unchanged(self):
        """响应体**没有**多出 deprecation 字段 —— 这是有意的选择，不是漏做。

        这条用例的作用是让「将来有人顺手往这里加一个提示字段」在一个
        写着理由的断言上失败，而不是只在黄金基线里体现为一处莫名其妙的 diff。
        """
        with isolated_server() as (client, _ctx):
            r = client.post(
                "/api/upload",
                files={"file": ("dep.txt", b"B00DEPREC1\n", "text/plain")},
                data={"batch_name": "dep_batch", "zip_code": "10001",
                      "needs_screenshot": "false"})
            self.assertEqual(r.status_code, 200, r.text)
            r = client.get("/api/batches/dep_batch/errors")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(sorted(r.json()), ["error_summary", "failed_tasks"])


if __name__ == "__main__":
    unittest.main()
