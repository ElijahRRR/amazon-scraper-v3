"""`fetch_results` 必须按**指定的 ASIN**去捞，不能只取前 N 行。

真机第一次跑 Phase 5 比对：旧系统 600 个 ASIN、新系统 44 个、**交集 0**，
可两边采的确实是同一批。原因不在采集，在这个函数：

``/api/results`` 的排序是 ``ORDER BY d.id DESC``，而 ``asin_data`` 是
**一 ASIN 一行、重采走 UPDATE** —— 行 id 在**第一次**见到该 ASIN 时分配，
之后永不变。于是「取前 N 行」= 「最近**首次收录**的 N 个 ASIN」。

旧系统是跑了很久的生产库（11920 行），测试用的 ASIN 早就在里面、id 很小，
重采只把行 UPDATE 了一遍，**永远排不进前 600**；新系统是全新库所以全在。

这条用例用一个假分页服务器把那个形状复现出来：目标 ASIN 全在**最后一页**。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.phase5_compare import (  # noqa: E402
    fetch_results, read_asin_file, resolve_batch)

# 5 页 × 200 行。前 4 页是「最近首次收录」的噪声，目标在最后一页。
TOTAL_PAGES = 5
TARGETS = [f"B0TARGET{i:02d}" for i in range(10)]


def _row(asin):
    return {"asin": asin, "id": 1, "title": f"T-{asin}", "current_price": "1.00"}


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):        # 静音
        pass

    def do_GET(self):
        q = parse_qs(urlparse(self.path).query)
        page = int(q.get("cursor", ["0"])[0])
        if page < TOTAL_PAGES - 1:
            items = [_row(f"B0NOISE{page:02d}{i:03d}") for i in range(200)]
        else:
            items = [_row(a) for a in TARGETS]
        body = json.dumps({"items": items,
                           "has_more": page < TOTAL_PAGES - 1,
                           "next_cursor": page + 1}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _BatchHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        # 真实形状：/api/batches 用的键是 ``name``（common/database.py:831）。
        # 第三个用 batch_name，钉住「两个键都要认」—— 写死一个就会像真机那样
        # 查出来全是 None。
        body = json.dumps({"batches": [
            {"id": 7, "name": "batch_20260806_145034"},
            {"id": 8, "name": "batch_20260806_145100"},
            {"id": 9, "batch_name": "batch_alt_key"},
        ]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class FetchTargetingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), _Handler)
        cls.base = f"http://127.0.0.1:{cls.srv.server_port}"
        cls.t = threading.Thread(target=cls.srv.serve_forever, daemon=True)
        cls.t.start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def test_without_wanted_it_stops_early_and_misses_the_targets(self):
        """这就是真机看到的行为 —— 保留它，好让「为什么必须给 --asins」有据可查。"""
        got = fetch_results(self.base, limit=500)
        self.assertGreaterEqual(len(got), 500)
        self.assertFalse(set(TARGETS) & set(got),
                         "前 500 行里不该有目标 ASIN（它们在最后一页）")

    def test_with_wanted_it_pages_until_it_finds_them(self):
        got = fetch_results(self.base, limit=500, wanted=set(TARGETS))
        self.assertEqual(sorted(got), sorted(TARGETS),
                         "指定了 ASIN 就该一直翻到找齐，且只收这些")

    def test_wanted_stops_at_max_scan_instead_of_looping_forever(self):
        got = fetch_results(self.base, limit=500, wanted={"B0NEVEREXIST"},
                            max_scan=400)
        self.assertEqual(got, {})

    def test_partial_hit_returns_what_it_found(self):
        wanted = set(TARGETS[:3]) | {"B0MISSING01"}
        got = fetch_results(self.base, limit=500, wanted=wanted, max_scan=10000)
        self.assertEqual(sorted(got), sorted(TARGETS[:3]))


class AsinFileTests(unittest.TestCase):

    def _write(self, text):
        import tempfile
        f = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                        encoding="utf-8")
        f.write(text)
        f.close()
        return f.name

    def test_reads_one_per_line_ignoring_blanks_and_comments(self):
        p = self._write("# 注释\nB0AAAAAAA1\n\n  B0AAAAAAA2  \nB0AAAAAAA3 备注\n")
        self.assertEqual(read_asin_file(p),
                         {"B0AAAAAAA1", "B0AAAAAAA2", "B0AAAAAAA3"})
        os.unlink(p)

    def test_empty_file_is_an_error_not_an_empty_comparison(self):
        """空列表静默通过 = 一次「全绿」的空转比对，比报错危险得多。"""
        p = self._write("# 只有注释\n\n")
        with self.assertRaises(SystemExit):
            read_asin_file(p)
        os.unlink(p)


if __name__ == "__main__":
    unittest.main()


class ResolveBatchTests(unittest.TestCase):
    """`--old-batch/--new-batch` 既认 id 也认批次名。

    真机上第一次给的就是名字（`batch_20260806_145034`）—— 界面上看得见的是名字，
    只认 id 等于把内部标识泄漏给使用者。而且找不到时必须**报错并列出候选**，
    静默按「不过滤」跑下去会退化成翻遍全表，正是它要避免的那件事。
    """

    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), _BatchHandler)
        cls.base = f"http://127.0.0.1:{cls.srv.server_port}"
        cls.t = threading.Thread(target=cls.srv.serve_forever, daemon=True)
        cls.t.start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def test_none_stays_none(self):
        self.assertIsNone(resolve_batch(self.base, None))

    def test_digits_pass_through_without_a_lookup(self):
        self.assertEqual(resolve_batch(self.base, "42"), 42)

    def test_a_name_is_looked_up(self):
        self.assertEqual(resolve_batch(self.base, "batch_20260806_145034"), 7)

    def test_the_batch_name_key_is_name_not_batch_name(self):
        """真机就栽在这儿：写死 batch_name，候选列表打出来全是 ['None','None']。"""
        self.assertEqual(resolve_batch(self.base, "batch_20260806_145100"), 8)

    def test_the_other_key_spelling_is_accepted_too(self):
        self.assertEqual(resolve_batch(self.base, "batch_alt_key"), 9)

    def test_unknown_name_fails_loudly_with_candidates(self):
        with self.assertRaises(SystemExit) as cm:
            resolve_batch(self.base, "batch_does_not_exist")
        self.assertIn("batch_20260806_145034", str(cm.exception))
