"""long_description 解析测试（worker/parser.py 的 _slx_parse_long_description / _parse_long_description）。

守的是 .agent/phase2/verify2 记的 FINDING 1：selectolax 路径用 ``Node.traverse()``
取 A+ / productDescription 容器里的文本，而 traverse() **不受子树约束**——文档原话是
"Iterate over all child and next nodes starting from the current level"，next 节点指
容器**之后**的兄弟节点。于是 long_description 把价格、库存、评分、评论数、BSR、
商品详情表、CDN 图片 URL 全都吸了进来。

两重后果：

1. ``long_description ∈ common/slowhash.py SLOW_HASH_FIELDS``。价格每天变，
   slow_hash 就每天变——实测 30 天序列里 29/29 次变更，纯噪声。
2. ``long_description`` 是导出字段，脏值一路流到 erpAPI。这是**先于 Phase 2 存在的
   生产数据质量 bug**，不是事件流引入的。

同一个函数还有第二处引擎语义分叉：判断「是不是叶子文本节点」用的是 ``node.iter()``，
但 selectolax 的 ``Node.iter()`` 只产出**直接子节点**，而 lxml 的 ``.iter()`` 是
「自身 + 全部后代」。``<div><a><span>text</span></a></div>`` 在 selectolax 下 div 被
判成叶子，div 与 span 各取一次 → 同一段文字重复两遍。

第三处：取文本用 ``text(deep=True, strip=True)``，strip=True 是**逐个文本节点** strip，
``Designed for <b>creators</b>`` 会被拼成 ``Designed forcreators``；lxml 版是
``"".join(node.xpath('.//text()')).strip()``，整体 strip，词间空格保留。

lxml 路径一直是对的，所以三处都以 lxml 为准对齐。
"""
import ast
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

# 与 test_delivery_parse.py 同理：test_session_slot 会把 worker.parser 桩成 fake
# 塞进 sys.modules，本文件需要真实解析器。
sys.modules.pop("worker.parser", None)

from worker.parser import (  # noqa: E402
    AmazonParser,
    _slx_iter_descendants,
    _slx_iter_subtree,
)

try:
    from selectolax.parser import HTMLParser as _Slx
    _HAS_SLX = True
except ImportError:
    _Slx = None
    _HAS_SLX = False

try:
    from lxml import html as _lxml_html
    _HAS_LXML = True
except ImportError:
    _lxml_html = None
    _HAS_LXML = False


# ---------------------------------------------------------------------------
# 页面素材：容器**之后**跟着价格/库存/评分/评论数/BSR/图片——正是 traverse() 会吸进来的东西
# ---------------------------------------------------------------------------
_DESC = ("The M80D is a 4K smart monitor that doubles as a streaming TV "
         "with built-in apps and a slim metal chassis.")

PAGE_FLAT = (
    '<html><body>'
    '<div id="productDescription"><p>' + _DESC + '</p></div>'
    '<div id="imgTagWrapperId"><img src="https://m.media-amazon.com/images/I/71xRPjIS8LL._AC_SL1500_.jpg"></div>'
    '<span class="a-price"><span class="a-offscreen">$549.99</span></span>'
    '<div id="availability"><span>In Stock</span></div>'
    '<span id="acrPopover" title="4.4 out of 5 stars"></span>'
    '<span id="acrCustomerReviewText">2431 ratings</span>'
    '<table id="productDetails_detailBullets_sections1">'
    '<tr><th>Best Sellers Rank</th><td>#312 in Electronics</td></tr>'
    '<tr><th>Item Weight</th><td>12.1 pounds</td></tr></table>'
    '</body></html>'
)

# 真实 A+ 模块的形状：非 _TEXT_TAGS 的包装元素（<a>/<b>/<i>）夹在中间
PAGE_APLUS = (
    '<html><body>'
    '<div class="aplus">'
    '  <div class="aplus-module"><h4>Why the M80D</h4>'
    '    <p>Stream <b>4K HDR</b> content without a PC, then switch to work mode.</p>'
    '    <div><a href="/x"><span>Learn more about SmartThings hub</span></a></div>'
    '    <img src="https://m.media-amazon.com/images/S/aplus-media/sc/aplus1.jpg">'
    '    <img src="https://m.media-amazon.com/images/G/01/pixel.gif">'
    '  </div>'
    '  <ul><li>Built-in streaming apps</li><li>Slim metal chassis</li></ul>'
    '</div>'
    '<div id="price"><span class="a-offscreen">$549.99</span></div>'
    '<div id="availability"><span>In Stock</span></div>'
    '</body></html>'
)

# 容器外一定不能出现在 long_description 里的串
_OUTSIDE = ["549.99", "In Stock", "2431 ratings", "Best Sellers Rank",
            "#312 in Electronics", "12.1 pounds", "71xRPjIS8LL"]


class SourceLevelGuard(unittest.TestCase):
    """常驻守卫：不依赖任何 HTML 引擎，永远执行。

    下面那些行为用例都得 skipUnless(_HAS_SLX)，而本仓库的 venv **不装** selectolax
    （requirements.txt 里 selectolax/lxml 是 worker 侧依赖，服务端 venv 只装
    requirements-dev.txt）。也就是说常规 CI 跑不到真正的 selectolax 分支——
    正是这个盲区让 traverse() 的 bug 活到了 Phase 2。
    所以留一条源码级断言：谁把 traverse() 加回来，这条立刻红。
    """

    def test_parser_source_contains_no_traverse_call(self):
        # 用 AST 找**真正的调用**：注释和文档字符串里解释 traverse() 为什么不能用的
        # 那几处不会被误伤（正则做不到这点）。
        path = os.path.join(_REPO_ROOT, "worker", "parser.py")
        tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
        offenders = [
            "%s:%d" % (path, node.lineno)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "traverse"
        ]
        self.assertEqual(
            offenders, [],
            "worker/parser.py 又出现了 Node.traverse() 调用。它不受子树约束，会把容器"
            "之后的价格/库存/BSR 吸进 long_description（见本文件模块 docstring）。"
            "要遍历子树请用 _slx_iter_subtree / _slx_iter_descendants。")


@unittest.skipUnless(_HAS_SLX, "selectolax 未安装")
class SlxSubtreeIterationTests(unittest.TestCase):
    """_slx_iter_subtree / _slx_iter_descendants 本身的语义。"""

    def test_subtree_iteration_does_not_escape_the_container(self):
        tree = _Slx(PAGE_FLAT)
        container = tree.css_first('#productDescription')
        # traverse() 会越界——把它的实际行为钉下来，这样 selectolax 哪天改了语义我们会知道
        self.assertGreater(len([n.tag for n in container.traverse()]), 2,
                           "selectolax Node.traverse() 不再越界？那本文件的前提变了，请复查")
        self.assertEqual([n.tag for n in _slx_iter_subtree(container)], ['div', 'p'])

    def test_descendants_excludes_self_and_reaches_nested(self):
        container = _Slx('<div id="d"><a><span>NESTED</span></a></div>').css_first('#d')
        # selectolax 的 Node.iter() 只有直接子节点，够不到 span
        self.assertEqual([n.tag for n in container.iter()], ['a'])
        self.assertEqual([n.tag for n in _slx_iter_descendants(container)], ['a', 'span'])
        self.assertEqual([n.tag for n in _slx_iter_subtree(container)], ['div', 'a', 'span'])

    def test_document_order_is_preserved(self):
        html = '<div id="d"><p>one</p><section><p>two</p><p>three</p></section><p>four</p></div>'
        container = _Slx(html).css_first('#d')
        texts = [n.text(deep=True).strip() for n in _slx_iter_descendants(container)
                 if n.tag == 'p']
        self.assertEqual(texts, ['one', 'two', 'three', 'four'])

    def test_deep_nesting_does_not_recurse(self):
        # 迭代实现，不该有 RecursionError（sys.setrecursionlimit 默认 1000）
        depth = 3000
        html = '<div id="d">' + '<div>' * depth + 'bottom' + '</div>' * depth + '</div>'
        container = _Slx(html).css_first('#d')
        self.assertEqual(sum(1 for _ in _slx_iter_descendants(container)), depth)

    def test_leaf_container_yields_only_itself(self):
        container = _Slx('<div id="d">text only</div>').css_first('#d')
        self.assertEqual([n.tag for n in _slx_iter_subtree(container)], ['div'])
        self.assertEqual(list(_slx_iter_descendants(container)), [])


@unittest.skipUnless(_HAS_SLX, "selectolax 未安装")
class SlxLongDescriptionTests(unittest.TestCase):
    """_slx_parse_long_description 的行为——B5 的核心回归。"""

    def setUp(self):
        self.p = AmazonParser()

    def _ld(self, html):
        return self.p._slx_parse_long_description(_Slx(html), html)

    def test_does_not_absorb_text_after_the_container(self):
        ld = self._ld(PAGE_FLAT)
        self.assertIn("4K smart monitor", ld)
        for bad in _OUTSIDE:
            self.assertNotIn(bad, ld,
                             "long_description 吸进了容器外的 %r —— traverse() 又回来了" % bad)

    def test_price_change_does_not_change_long_description(self):
        """B5 的本质：只改价格，long_description 必须逐字节不变（否则 slow_hash 变）。"""
        a = self._ld(PAGE_FLAT)
        b = self._ld(PAGE_FLAT.replace("$549.99", "$429.00"))
        self.assertEqual(a, b)

    def test_stock_and_rating_changes_do_not_change_long_description(self):
        base = self._ld(PAGE_FLAT)
        self.assertEqual(base, self._ld(PAGE_FLAT.replace("In Stock", "Currently unavailable")))
        self.assertEqual(base, self._ld(PAGE_FLAT.replace("2431 ratings", "2502 ratings")))
        self.assertEqual(base, self._ld(PAGE_FLAT.replace("#312 in Electronics",
                                                          "#1,908 in Electronics")))

    def test_aplus_container_stops_at_its_own_boundary(self):
        ld = self._ld(PAGE_APLUS)
        self.assertIn("Why the M80D", ld)
        self.assertIn("Built-in streaming apps", ld)
        self.assertNotIn("549.99", ld)
        self.assertNotIn("In Stock", ld)

    def test_no_duplicated_text_from_nested_wrappers(self):
        """<div><a><span>text</span></a></div>：div 与 span 只能取一次。"""
        ld = self._ld(PAGE_APLUS)
        self.assertEqual(ld.count("Learn more about SmartThings hub"), 1)

    def test_inline_markup_keeps_word_spacing(self):
        """text(deep=True, strip=True) 会拼成 'Stream4K HDRcontent'，必须整体 strip。"""
        ld = self._ld(PAGE_APLUS)
        self.assertIn("Stream 4K HDR content without a PC", ld)

    def test_images_inside_the_container_are_kept_and_pixels_dropped(self):
        ld = self._ld(PAGE_APLUS)
        self.assertIn("[Image: https://m.media-amazon.com/images/S/aplus-media/sc/aplus1.jpg]", ld)
        self.assertNotIn("pixel.gif", ld)

    def test_product_image_outside_the_container_is_not_absorbed(self):
        # 主图在 #imgTagWrapperId 里，属于 image_urls，不属于描述
        self.assertNotIn("71xRPjIS8LL", self._ld(PAGE_FLAT))

    def test_jsonld_fallback_when_no_container(self):
        html = ('<html><body><script type="application/ld+json">'
                '{"@type":"Product","description":"A 4K smart monitor that doubles as a streaming TV."}'
                '</script><div id="availability"><span>In Stock</span></div></body></html>')
        self.assertEqual(self._ld(html),
                         "A 4K smart monitor that doubles as a streaming TV.")

    def test_empty_when_neither_container_nor_jsonld(self):
        self.assertEqual(self._ld('<html><body><div id="x">nothing</div></body></html>'), "")


@unittest.skipUnless(_HAS_SLX and _HAS_LXML, "selectolax 或 lxml 未安装")
class EngineAgreementTests(unittest.TestCase):
    """两条解析路径必须对同一份 HTML 给出**同一个** long_description。

    不一致的代价不是抽象的：long_description ∈ SLOW_HASH_FIELDS，一旦换引擎上线，
    全库每个商品的 slow_hash 会在同一天集体重置，事件流被一次性刷屏。
    """

    def setUp(self):
        self.p = AmazonParser()

    def _both(self, html):
        slx = self.p._slx_parse_long_description(_Slx(html), html)
        lxm = self.p._parse_long_description(_lxml_html.fromstring(html), html)
        return slx, lxm

    def test_flat_product_description(self):
        slx, lxm = self._both(PAGE_FLAT)
        self.assertEqual(slx, lxm)
        self.assertIn("4K smart monitor", slx)

    def test_aplus_with_nested_wrappers(self):
        slx, lxm = self._both(PAGE_APLUS)
        self.assertEqual(slx, lxm)

    def test_inline_markup(self):
        html = ('<html><body><div id="productDescription">'
                '<p>Designed for <b>creators</b> and <i>gamers</i> alike.</p></div>'
                '<div id="availability"><span>In Stock</span></div></body></html>')
        slx, lxm = self._both(html)
        self.assertEqual(slx, lxm)
        self.assertEqual(slx, "Designed for creators and gamers alike.")

    def test_table_inside_aplus(self):
        html = ('<html><body><div class="aplus">'
                '<table><tr><td>Panel</td><td>VA 4K UHD 60Hz</td></tr></table>'
                '</div><div id="price"><span>$1.00</span></div></body></html>')
        slx, lxm = self._both(html)
        self.assertEqual(slx, lxm)

    def test_container_is_last_element(self):
        html = ('<html><body><div id="availability"><span>In Stock</span></div>'
                '<div id="productDescription"><p>' + _DESC + '</p></div></body></html>')
        slx, lxm = self._both(html)
        self.assertEqual(slx, lxm)

    def test_jsonld_fallback(self):
        html = ('<html><body><script type="application/ld+json">'
                '{"@type":"Product","description":"A 4K smart monitor."}'
                '</script></body></html>')
        slx, lxm = self._both(html)
        self.assertEqual(slx, lxm)


@unittest.skipUnless(_HAS_LXML, "lxml 未安装")
class LxmlLongDescriptionTests(unittest.TestCase):
    """把 lxml 路径的正确行为单独钉住——它是上面那组「对齐」的基准，不能被反向对齐坏。"""

    def setUp(self):
        self.p = AmazonParser()

    def _ld(self, html):
        return self.p._parse_long_description(_lxml_html.fromstring(html), html)

    def test_bounded_to_container(self):
        ld = self._ld(PAGE_FLAT)
        self.assertIn("4K smart monitor", ld)
        for bad in _OUTSIDE:
            self.assertNotIn(bad, ld)

    def test_price_change_does_not_change_long_description(self):
        self.assertEqual(self._ld(PAGE_FLAT),
                         self._ld(PAGE_FLAT.replace("$549.99", "$429.00")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
