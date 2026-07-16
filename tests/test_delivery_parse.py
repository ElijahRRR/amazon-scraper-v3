"""
配送日期/时长解析测试（worker/parser.py 的 _slx_parse_delivery / _pick_delivery）。

覆盖两类修复：
  1) 会员专享（"Prime members get..."/"Join Prime"）更快配送日期不再被误报为
     标准配送——采集会话非 Prime，非会员实际拿不到该日期。优先报**非会员**里最快的。
  2) 鲁棒性：日期直接读 data-csa-c-delivery-time 属性值，覆盖"日期是属性节点的
     直接文本节点、无子元素"这种旧 '[attr] *' 选择器抓不到 → 返回 N/A 的模板。

核心偏好逻辑（_pick_delivery）只用 Tomorrow/Today，不依赖 dateparser，处处可跑；
需要 "Month Day" 换算天数与 HTML 解析的用例按依赖存在性 skip。
"""
import os
import sys
import unittest

_REPO_ROOT = os.environ.get("REPO_ROOT") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
if not os.path.isdir(os.path.join(_REPO_ROOT, "worker")):
    _REPO_ROOT = os.getcwd()
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from worker.parser import AmazonParser  # noqa: E402

try:
    from selectolax.parser import HTMLParser as _Slx
    _HAS_SLX = True
except ImportError:
    _HAS_SLX = False

try:
    import dateparser  # noqa: F401
    _HAS_DP = True
except ImportError:
    _HAS_DP = False


class PickDeliveryTests(unittest.TestCase):
    """_pick_delivery 偏好逻辑（Tomorrow/Today，不依赖 dateparser）。"""

    def setUp(self):
        self.p = AmazonParser()

    def test_prefers_nonprime_even_if_slower(self):
        # 非会员 Tomorrow(1) vs 会员专享 Today(0) → 取非会员 Tomorrow
        d, t = self.p._pick_delivery([("Tomorrow", False), ("Today", True)])
        self.assertEqual((d, t), ("Tomorrow", "1"))

    def test_fastest_among_nonprime(self):
        d, t = self.p._pick_delivery([("Tomorrow", False), ("Today", False)])
        self.assertEqual((d, t), ("Today", "0"))

    def test_all_prime_gated_falls_back(self):
        # 全为会员专享 → 回退用会员日期，好过 N/A
        d, t = self.p._pick_delivery([("Today", True)])
        self.assertEqual((d, t), ("Today", "0"))

    def test_empty_returns_na(self):
        self.assertEqual(self.p._pick_delivery([]), ("N/A", "N/A"))

    def test_unparseable_returns_na(self):
        self.assertEqual(self.p._pick_delivery([("no date here", False)]), ("N/A", "N/A"))


@unittest.skipUnless(_HAS_SLX, "selectolax 未安装")
class SlxDeliveryTests(unittest.TestCase):
    """_slx_parse_delivery：属性直读 + 会员专享判定 + 回退。"""

    def setUp(self):
        self.p = AmazonParser()

    def _d(self, html):
        return self.p._slx_parse_delivery(_Slx(html))

    def test_date_as_direct_text_node(self):
        # 日期是属性 span 的直接文本节点、无子元素：旧 '[attr] *' 抓不到 → 曾返回 N/A
        html = ('<div id="deliveryBlockMessage">'
                '<span data-csa-c-delivery-time="Monday, July 20">'
                'FREE delivery Monday, July 20</span></div>')
        date, _ = self._d(html)
        self.assertNotEqual(date, "N/A")

    def test_prime_gated_excluded_prefers_standard(self):
        html = ('<span data-csa-c-delivery-time="Monday, July 20">'
                'FREE delivery Monday, July 20</span>'
                '<span data-csa-c-delivery-time="Friday, July 17">'
                'Or Prime members get FREE delivery Friday, July 17. Join Prime</span>')
        date, _ = self._d(html)
        # 应取标准 July 20，而不是会员专享 July 17
        self.assertIn("20", date)
        self.assertNotIn("17", date)

    def test_all_prime_gated_still_returns_a_date(self):
        html = ('<span data-csa-c-delivery-time="Friday, July 17">'
                'Prime members get FREE delivery Friday, July 17. Join Prime</span>')
        date, _ = self._d(html)
        self.assertNotEqual(date, "N/A")

    def test_no_delivery_info_returns_na(self):
        self.assertEqual(self._d('<div id="foo">nothing</div>'), ("N/A", "N/A"))

    @unittest.skipUnless(_HAS_DP, "dateparser 未安装")
    def test_fallback_text_path_without_attribute(self):
        # 不带 data-csa-c-delivery-time 属性，日期只在 .delivery-message 文本里
        html = '<div class="delivery-message">Get it by <b>July 25</b></div>'
        date, days = self._d(html)
        self.assertIn("25", date)


if __name__ == "__main__":
    unittest.main(verbosity=2)
