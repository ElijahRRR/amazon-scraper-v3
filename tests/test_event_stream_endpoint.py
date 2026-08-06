"""``GET /api/_debug/event-stream`` —— Phase 2 的观测面。

放在 ``tests/`` 根而不是 ``tests/pgdb/``：那个目录的 conftest 会
``importorskip("asyncpg")``，而本文件最重要的一条断言恰恰是
**SQLite 后端上这个端点也得能回**（运维用同一个 URL 探两种部署）。
它跟着 ``DB_BACKEND`` 走，两种 CI 模式下各验各的那一半。
"""
from __future__ import annotations

import unittest

from common.dbfactory import get_backend, is_postgres
from tests.golden.harness import isolated_server


class EventStreamEndpointTest(unittest.TestCase):

    def test_endpoint_answers_on_both_backends(self):
        """两个后端都必须 200。SQLite 上如实回 enabled=false，不是 404 / 500。"""
        with isolated_server() as (client, _ctx):
            r = client.get("/api/_debug/event-stream")
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertEqual(body["backend"], get_backend())
            self.assertEqual(body["enabled"], is_postgres())
            if not is_postgres():
                # 事件流是 PG 专属，SQLite 上"没有"是正确状态，不是故障。
                self.assertIn("reason", body)
                self.assertNotIn("counters", body)

    def test_postgres_exposes_what_phase3_and_ops_need(self):
        if not is_postgres():
            self.skipTest("事件流是 PostgreSQL 专属")
        with isolated_server() as (client, _ctx):
            body = client.get("/api/_debug/event-stream").json()
            self.assertNotIn("error", body, f"指标端点自己报错了: {body.get('error')}")
            for key in ("gen", "instance_id", "outbox_depth", "relay_lag_s",
                        "events_per_minute", "max_seq", "min_available_seq",
                        "dead_letters", "partitions", "future_partitions",
                        "counters", "relay_state", "contract_version"):
                self.assertIn(key, body, f"少了运维要的 {key}")
            # 计划 §2.1：永远保持至少 2 个未来分区
            self.assertGreaterEqual(body["future_partitions"], 2)

    def test_not_in_openapi_because_the_baseline_pins_it(self):
        """``/openapi.json`` 是黄金基线的一步，逐字节钉死。

        新端点只要进了 schema，64 步当场挂——所以它必须
        ``include_in_schema=False``。这条断言是那个约束的守卫，不是风格检查。
        """
        with isolated_server() as (client, _ctx):
            paths = client.get("/openapi.json").json()["paths"]
        self.assertNotIn("/api/_debug/event-stream", paths)
        self.assertEqual([p for p in paths if "event-stream" in p], [])


if __name__ == "__main__":
    unittest.main()
