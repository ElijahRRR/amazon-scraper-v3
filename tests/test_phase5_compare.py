"""Phase 5 比对工具的分类逻辑用例。

这个工具的**全部价值**在于把差异分成三类：有意变更 / 天然快变 / 意外。
分类错了它就变成噪声发生器——把真问题淹死，或者把有意变更报成故障让人
习惯性忽略报告。所以每一类都要有用例，尤其是「有意变更但**方向反了**」
必须被抓成 UNEXPECTED，那是最容易漏的一种。
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.phase5_compare import classify, compare  # noqa: E402


class ClassifyTests(unittest.TestCase):

    def test_volatile_fields_are_not_failures(self):
        for f in ("current_price", "stock_count", "buybox_price",
                  "updated_at", "best_sellers_rank"):
            cat, _ = classify(f, "1", "2")
            self.assertEqual(cat, "VOLATILE", f)

    def test_slow_field_change_is_unexpected(self):
        """慢变字段短时间内变了 —— 这正是 Phase 5 要找的东西。"""
        cat, why = classify("title", "Acme Wrench", "Acme Wrench Pro")
        self.assertEqual(cat, "UNEXPECTED")
        self.assertIn("慢变", why)

    def test_crawl_time_format_change_is_expected(self):
        cat, why = classify("crawl_time",
                            "2026-08-05 18:34:13", "2026-08-05T10:34:13Z")
        self.assertEqual(cat, "EXPECTED")
        self.assertIn("D-61", why)

    def test_crawl_time_still_naive_is_unexpected(self):
        """新值还是裸格式 = D-61 没生效。必须报出来，不能算「预期内」。"""
        cat, _ = classify("crawl_time",
                          "2026-08-05 18:34:13", "2026-08-05 18:34:14")
        self.assertEqual(cat, "UNEXPECTED")

    def test_long_description_getting_shorter_is_expected(self):
        cat, why = classify(
            "long_description",
            "好描述。 $549.99 in stock 2431 ratings", "好描述。")
        self.assertEqual(cat, "EXPECTED")
        self.assertIn("D-60", why)

    def test_long_description_getting_LONGER_is_unexpected(self):
        """方向反了。修复的是「不再吸进容器外文本」，新值变长几乎一定是错的。

        这条是本文件最重要的用例：它守的是「有意变更也可能改错方向」。
        只按字段名放行，就会把一个真回归盖章成预期内。
        """
        cat, why = classify("long_description", "好描述。", "好描述。 $549.99 in stock")
        self.assertEqual(cat, "UNEXPECTED")
        self.assertIn("方向不对", why)

    def test_list_reordering_is_expected_but_membership_change_is_not(self):
        cat, _ = classify("upc_list", "b,a,c", "a,b,c")
        self.assertEqual(cat, "EXPECTED", "只是排序，应算预期内")

        cat, why = classify("upc_list", "a,b,c", "a,b")
        self.assertEqual(cat, "UNEXPECTED", "集合变了 = 真的丢了一个 UPC")
        self.assertIn("方向不对", why)

    def test_zip_must_become_a_five_digit_requested_zip(self):
        cat, _ = classify("zip_code", "10001", "90210")
        self.assertEqual(cat, "EXPECTED")
        cat, _ = classify("zip_code", "10001", "")
        self.assertEqual(cat, "UNEXPECTED", "变空不是 D-55 的预期形态")

    def test_unknown_field_defaults_to_unexpected(self):
        """新冒出来的字段默认按意外处理——宁可多报，不可漏报。"""
        cat, why = classify("some_new_column", "a", "b")
        self.assertEqual(cat, "UNEXPECTED")
        self.assertIn("未分类", why)


class CompareTests(unittest.TestCase):

    def _rows(self, **over):
        base = {"asin": "B0TEST0001", "title": "T", "brand": "B",
                "current_price": "19.99", "crawl_time": "2026-08-05 18:00:00",
                "upc_list": "b,a", "long_description": "d $9.99 in stock"}
        base.update(over)
        return {"B0TEST0001": base}

    def test_clean_migration_reports_no_unexpected(self):
        old = self._rows()
        new = self._rows(crawl_time="2026-08-05T10:00:00Z",
                         upc_list="a,b", long_description="d",
                         current_price="21.99")     # 价格变了，天然快变
        res = compare(old, new)
        self.assertEqual(res["unexpected"], [], res["unexpected"])
        self.assertEqual(res["compared"], 1)

    def test_a_real_regression_surfaces(self):
        old = self._rows()
        new = self._rows(crawl_time="2026-08-05T10:00:00Z",
                         upc_list="a,b", long_description="d",
                         title="")                  # 标题没了 —— 真问题
        res = compare(old, new)
        fields = {d["field"] for d in res["unexpected"]}
        self.assertIn("title", fields)

    def test_missing_asins_are_reported(self):
        res = compare({"A": {"asin": "A"}}, {"B": {"asin": "B"}})
        self.assertEqual(res["old_only"], ["A"])
        self.assertEqual(res["new_only"], ["B"])
        self.assertEqual(res["compared"], 0)

    def test_intentional_change_that_did_not_happen_is_tracked(self):
        """两边 crawl_time 完全一样 = D-61 没生效（或两边是同一个版本）。

        这不算 UNEXPECTED 差异（因为压根没差异），但报告必须单独点出来，
        否则「跑完全绿」会被误读成「修复都生效了」。
        """
        same = "2026-08-05 18:00:00"
        res = compare(self._rows(crawl_time=same), self._rows(crawl_time=same))
        self.assertEqual(res["per_field"]["crawl_time"]["unchanged"], 1)


if __name__ == "__main__":
    unittest.main()
