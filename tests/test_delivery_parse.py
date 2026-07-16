"""
配送日期/时长解析测试（worker/parser.py 的 _slx_parse_delivery / _pick_delivery）。

语义：报**页面上能看到的最快**配送日期（含 Prime 会员专享）。覆盖：
  1) 取全局最快：多条配送候选里选距今天数最小者（含会员专享的更快日期）。
  2) 鲁棒性：日期直接读 data-csa-c-delivery-time 属性值，覆盖"日期是属性节点的
     直接文本节点、无子元素"这种旧 '[attr] *' 选择器抓不到 → 返回 N/A 的模板。

核心选择逻辑（_pick_delivery）只用 Tomorrow/Today，不依赖 dateparser，处处可跑；
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

# 其他测试（如 test_session_slot）会把 worker.parser 桩成 fake 塞进 sys.modules。
# 本文件需要真实解析器，运行顺序在其后时会拿到桩件 → 先移除，强制加载真实模块。
sys.modules.pop("worker.parser", None)

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
    """_pick_delivery 选择逻辑（Tomorrow/Today，不依赖 dateparser）。"""

    def setUp(self):
        self.p = AmazonParser()

    def test_fastest_wins(self):
        # 取全局最快：Tomorrow(1) vs Today(0) → Today
        d, t = self.p._pick_delivery(["Tomorrow", "Today"])
        self.assertEqual((d, t), ("Today", "0"))

    def test_fastest_wins_within_one_text(self):
        d, t = self.p._pick_delivery(["Delivery Tomorrow. Or Today if you hurry"])
        self.assertEqual((d, t), ("Today", "0"))

    def test_single_candidate(self):
        d, t = self.p._pick_delivery(["Tomorrow"])
        self.assertEqual((d, t), ("Tomorrow", "1"))

    def test_empty_returns_na(self):
        self.assertEqual(self.p._pick_delivery([]), ("N/A", "N/A"))

    def test_unparseable_returns_na(self):
        self.assertEqual(self.p._pick_delivery(["no date here"]), ("N/A", "N/A"))


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

    @unittest.skipUnless(_HAS_DP, "dateparser 未安装")
    def test_fastest_including_prime_is_preferred(self):
        html = ('<span data-csa-c-delivery-time="Monday, July 20">'
                'FREE delivery Monday, July 20</span>'
                '<span data-csa-c-delivery-time="Friday, July 17">'
                'Or Prime members get FREE delivery Friday, July 17. Join Prime</span>')
        date, _ = self._d(html)
        # 应取页面上最快的 July 17（含 Prime 会员专享），而不是较慢的 July 20
        self.assertIn("17", date)

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
