"""Phase 4 采集侧数据质量测试（worker/parser.py）。

**这个文件存在的理由：黄金样本看不见这里的任何一处改动。**
tests/golden/ 的 64 步是行为规范，但它对本文件覆盖的东西有三个结构性盲区
（tests/golden/README.md 与 .agent/pg_migration_plan.md 都记了）：

1. `crawl_time` 及一切时间戳键都在 harness 的 ``_VOLATILE_KEYS`` 里，值被换成
   ``<VOLATILE>``。裸 UTC+8 改成带时区 UTC，**对 golden 完全不可见**。
2. harness 通过 HTTP 喂的是**合成的** result 字典，它从不 import worker.parser
   （Phase 2 用 MetaPathFinder 证过）。于是**每一处解析器改动对 golden 都不可见**。
3. bool/int 不参与差分的类型检查。

所以 Phase 4 的每一项都必须自带证据：真 HTML 过真解析器。这个文件就是那份证据的
可执行版本。覆盖：

  P4-1  zip_requested / zip_observed / zip_verify 三分
  P4-2  完整性位图，按 **HTML 区块存在性** 判定
  P4-4  `manufacturer` 不再被 "Manufacturer recommended age" 污染
  P4-5  set() 顺序 —— **跨 5 个独立进程**验证（单进程结构上看不见这个 bug）
  P4-6  rating / review_count / seller_id / seller_name 两条引擎路径都产出
  P4-7  crawl_time 带时区 UTC
  P4-9  parse_engine 记录

lxml 相关用例按依赖存在性 skip（与 tests/test_long_description.py 同约定：
requirements 里 selectolax/lxml 是 worker 侧依赖，服务端 venv 只装其一）。
"""
import os
import re
import subprocess
import sys
import unittest
from datetime import datetime, timezone

_REPO_ROOT = os.environ.get("REPO_ROOT") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
if not os.path.isdir(os.path.join(_REPO_ROOT, "worker")):
    _REPO_ROOT = os.getcwd()
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# 与 test_delivery_parse.py / test_long_description.py 同理：test_session_slot
# 会把 worker.parser 桩成 fake 塞进 sys.modules，本文件需要真实解析器。
sys.modules.pop("worker.parser", None)

from worker.parser import (  # noqa: E402
    COMPLETENESS_BREADCRUMB,
    COMPLETENESS_DETAIL_TABLE,
    COMPLETENESS_MAIN_IMAGE,
    COMPLETENESS_MEASURED,
    ENGINE_LXML,
    ENGINE_SELECTOLAX,
    ZIP_VERIFY_ASSUMED,
    ZIP_VERIFY_CONFIRMED,
    ZIP_VERIFY_MISMATCH,
    ZIP_VERIFY_UNVERIFIED,
    AmazonParser,
)

try:
    import selectolax  # noqa: F401
    _HAS_SLX = True
except ImportError:
    _HAS_SLX = False

try:
    from lxml import html as _lxml_html
    _HAS_LXML = True
except ImportError:
    _lxml_html = None
    _HAS_LXML = False


# ==========================================================================
# 真实形状的 HTML 夹具
# ==========================================================================
# 结构照抄解析器本来就在瞄的那些挂件：glow 配送挂件、wayfinding 面包屑、
# productDetails 规格表、colorImages 脚本、twister、acrPopover、merchant-info。

FULL = """<!doctype html><html><head>
<link rel="canonical" href="https://www.amazon.com/dp/B08N5WRWNW"/>
<meta name="title" content="Acme Storage Bin"/>
</head><body>
<div id="nav-global-location-slot">
  <span id="glow-ingress-line1">Deliver to</span>
  <span id="glow-ingress-line2">New York 10001</span>
</div>
<input type="hidden" id="ASIN" value="B08N5WRWNW"/>
<div id="wayfinding-breadcrumbs_feature_div">
  <ul class="a-unordered-list">
    <li><span class="a-list-item"><a class="a-link-normal" href="/b/ref=dp_bc_1?node=1055398">Home &amp; Kitchen</a></span></li>
    <li><span class="a-list-item"><a class="a-link-normal" href="/b/ref=dp_bc_2?node=3610851">Storage &amp; Organization</a></span></li>
  </ul>
</div>
<span id="productTitle">Acme Storage Bin, 12 Gal</span>
<a id="bylineInfo" href="/stores/Acme/page/X">Visit the Acme Store</a>
<div id="averageCustomerReviews">
  <span id="acrPopover" title="4.4 out of 5 stars"><span class="a-icon-alt">4.4 out of 5 stars</span></span>
  <span id="acrCustomerReviewText">1,234 ratings</span>
</div>
<div id="corePrice_feature_div"><span class="a-price"><span class="a-offscreen">$19.99</span></span></div>
<div id="availability"><span>In Stock</span></div>
<div id="merchant-info">Ships from <span>Amazon.com</span> Sold by <a href="#">Amazon.com</a></div>
<div id="imageBlock_feature_div">
  <div id="imgTagWrapperId"><img src="https://m.media-amazon.com/images/I/71main.jpg"/></div>
  <div id="altImages"><ul><li><img src="https://m.media-amazon.com/images/I/71alt1.jpg"/></li></ul></div>
</div>
<script type="text/javascript">
var obj = jQuery.parseJSON('{"colorImages":{"initial":[
{"hiRes":"https://m.media-amazon.com/images/I/71main.jpg"},
{"hiRes":"https://m.media-amazon.com/images/I/71alt1.jpg"}]}}');
</script>
<div id="productDetails_feature_div">
<table id="productDetails_techSpec_section_1" class="prodDetTable">
  <tr><th class="prodDetSectionEntry">Manufacturer</th><td class="prodDetAttrValue">Acme Industrial Ltd.</td></tr>
  <tr><th class="prodDetSectionEntry">Manufacturer recommended age</th><td class="prodDetAttrValue">3 years and up</td></tr>
  <tr><th class="prodDetSectionEntry">Item model number</th><td class="prodDetAttrValue">AC-1200</td></tr>
  <tr><th class="prodDetSectionEntry">Country of Origin</th><td class="prodDetAttrValue">China</td></tr>
  <tr><th class="prodDetSectionEntry">UPC</th><td class="prodDetAttrValue">889698335447</td></tr>
</table>
</div>
<script type="text/javascript">
var twister = {"dimensionValuesDisplayData":{"B08N5WRWNW":["Red","L"],"B08N5AAAA1":["Blue","L"],"B08N5BBBB2":["Green","L"]},"dimensions":["color_name","size_name"]};
var p = {"parentAsin":"B08N5PARENT","asin":"B08N5WRWNW"};
</script>
<script>var upcs = {"upc":"889698335447"};var upc2={"upc":"012345678905"};var upc3={"upc":"400638133792"};
var upc4={"upc":"701234567890"};var upc5={"upc":"888462018203"};var upc6={"upc":"190198001787"};</script>
<script>var others = {"asin":"B08XSPONS01"};var o2={"asin":"B08XSPONS02"};var o3={"asin":"B08XSPONS03"};
var o4={"asin":"B08XSPONS04"};var o5={"asin":"B08XSPONS05"};var o6={"asin":"B08XSPONS06"};</script>
</body></html>"""

#: 同一个商品的**软降级**页：面包屑区块 / 详情表 / 主图集三块被整块删掉。
#: 这正是审计 §2 描述的模式 —— category 只有面包屑一个数据源。
DEGRADED = """<!doctype html><html><head>
<link rel="canonical" href="https://www.amazon.com/dp/B08N5WRWNW"/>
</head><body>
<div id="nav-global-location-slot">
  <span id="glow-ingress-line1">Deliver to</span>
  <span id="glow-ingress-line2">Altoona 16602</span>
</div>
<input type="hidden" id="ASIN" value="B08N5WRWNW"/>
<span id="productTitle">Acme Storage Bin, 12 Gal</span>
<div id="corePrice_feature_div"><span class="a-price"><span class="a-offscreen">$19.99</span></span></div>
<div id="availability"><span>In Stock</span></div>
</body></html>"""

#: 三块区块**都在但都是空的**。按解析值判定的话，它与 DEGRADED 完全无法区分
#: —— 这就是 P4-2 必须按区块存在性判定的全部理由。
EMPTY_BLOCKS = """<!doctype html><html><body>
<span id="glow-ingress-line2">New York 10001</span>
<span id="productTitle">Acme Storage Bin, 12 Gal</span>
<div id="wayfinding-breadcrumbs_feature_div"><ul></ul></div>
<div id="productDetails_feature_div"><table id="productDetails_techSpec_section_1"></table></div>
<div id="imageBlock_feature_div"><div id="imgTagWrapperId"></div></div>
</body></html>"""

THIRD_PARTY = FULL.replace(
    '<div id="merchant-info">Ships from <span>Amazon.com</span> Sold by <a href="#">Amazon.com</a></div>',
    '<div id="merchant-info">Ships from <span>Amazon</span> Sold by '
    '<a id="sellerProfileTriggerId" href="/sp?seller=A1X2Y3Z4W5V6U7">Acme Direct</a></div>',
)

#: 无 glow 挂件的页面（Amazon 偶发不渲染配送挂件）。
NO_GLOW = FULL.replace('<span id="glow-ingress-line2">New York 10001</span>', "")

CAPTCHA = "<html><body>validateCaptcha</body></html>"

_FOUR_CARRIED = ("rating", "review_count", "seller_id", "seller_name")


class _ParserCase(unittest.TestCase):
    def setUp(self):
        self.p = AmazonParser()

    def slx(self, html, asin="B08N5WRWNW", zip_code="10001"):
        """强制走 selectolax 路径（不看模块级开关）。"""
        return self.p._parse_with_selectolax(
            html, asin, zip_code, self.p._default_result(asin, zip_code),
            self.p._extract_jsonld(html))

    def lxm(self, html, asin="B08N5WRWNW", zip_code="10001"):
        """强制走 lxml 路径。

        生产上这条分支只在 selectolax 缺失时才跑，靠模块级 `_USE_SELECTOLAX`
        永远测不到它 —— 而它正是 P4-6 那四个字段丢失的地方。
        """
        return self.p._parse_with_lxml(
            html, asin, zip_code, self.p._default_result(asin, zip_code),
            self.p._extract_jsonld(html))


# ==========================================================================
# P4-7  crawl_time -> 带时区 UTC
# ==========================================================================
class CrawlTimeUTC(_ParserCase):
    """契约变更，且是 golden 结构上看不见的那一类（crawl_time 被 scrub 成 <VOLATILE>）。"""

    def test_format_is_rfc3339_utc_z(self):
        ct = self.p._default_result("B0TEST00001", "10001")["crawl_time"]
        self.assertRegex(ct, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
                         "crawl_time 必须是 RFC3339 的 UTC 'Z' 形式")

    def test_value_is_actually_utc_not_plus_8(self):
        """旧实现是东八区墙上时间且丢掉偏移，按 UTC 读会早/晚 8 小时。"""
        before = datetime.now(timezone.utc).replace(microsecond=0)
        ct = self.p._default_result("B0TEST00001", "10001")["crawl_time"]
        after = datetime.now(timezone.utc)
        got = datetime.strptime(ct, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self.assertGreaterEqual(got, before)
        self.assertLessEqual(got, after)

    def test_not_parseable_as_the_old_naive_format(self):
        """新格式必须**无法**被老格式静默吃掉。

        换成 ``"%Y-%m-%d %H:%M:%S+00:00"`` 的话，没改造的消费者
        （如 relay.parse_collected_at 的 ``s[:19]`` 切片）会解析**成功**，
        然后重新贴上 +08:00 —— 静默错 8 小时。用 'T'+'Z' 则必然抛 ValueError，
        消费者走自己的兜底分支并留下计数。**同样是没改造的消费者，
        这个格式让它响，那个格式让它错。**
        """
        ct = self.p._default_result("B0TEST00001", "10001")["crawl_time"]
        with self.assertRaises(ValueError):
            datetime.strptime(ct[:19], "%Y-%m-%d %H:%M:%S")

    def test_real_html_path_uses_the_same_clock(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(FULL)
        self.assertRegex(r["crawl_time"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


# ==========================================================================
# P4-1  zip_requested / zip_observed / zip_verify
# ==========================================================================
class ZipObservedAndVerify(_ParserCase):
    def test_observed_comes_from_line2_not_line1(self):
        """FULL 的 line1 是 "Deliver to"（无邮编），line2 是 "New York 10001"。

        老的 `_slx_parse_zip_code` 读 line1，于是近乎恒返回 None。
        """
        self.assertEqual(self.p._parse_zip_observed(FULL), "10001")
        self.assertIsNone(self.p._parse_zip_observed(NO_GLOW))
        self.assertIsNone(self.p._parse_zip_observed(""))

    def test_line1_alone_is_not_a_zip_source(self):
        """只有 line1 带数字时不许当成观测邮编 —— 那一行不是邮编行。"""
        html = '<span id="glow-ingress-line1">Deliver to 99999</span>'
        self.assertIsNone(self.p._parse_zip_observed(html))

    def test_zip_plus_four_takes_the_5_digit_prefix(self):
        html = '<span id="glow-ingress-line2">Brooklyn 11201-1234</span>'
        self.assertEqual(self.p._parse_zip_observed(html), "11201")

    def test_verify_confirmed_mismatch_assumed_unverified(self):
        self.assertEqual(self.p._judge_zip("10001", "10001"), ZIP_VERIFY_CONFIRMED)
        self.assertEqual(self.p._judge_zip("90210", "10001"), ZIP_VERIFY_MISMATCH)
        self.assertEqual(self.p._judge_zip("10001", None), ZIP_VERIFY_ASSUMED)
        self.assertEqual(self.p._judge_zip("", None), ZIP_VERIFY_UNVERIFIED)
        self.assertEqual(self.p._judge_zip(None, "10001"), ZIP_VERIFY_UNVERIFIED)
        # 前导零：relay 侧 normalize_zip 会补零，这里也必须能对上
        self.assertEqual(self.p._judge_zip("2138", "02138"), ZIP_VERIFY_CONFIRMED)

    def test_zip_code_column_is_always_the_requested_zip(self):
        """消费侧的分组键是 (asin, marketplace, zip_requested)。

        一条声称 zip A、价格其实是 zip B 的记录会把价格序列悄悄劈成两组，
        所以 `zip_code` 必须恒等于请求值，观测值另放。
        """
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(FULL, zip_code="90210")     # 页面显示 10001
        self.assertEqual(r["zip_code"], "90210")
        self.assertEqual(r["_zip_observed"], "10001")
        self.assertEqual(r["_zip_verify"], ZIP_VERIFY_MISMATCH)

        r_ok = self.slx(FULL, zip_code="10001")
        self.assertEqual(r_ok["zip_code"], "10001")
        self.assertEqual(r_ok["_zip_verify"], ZIP_VERIFY_CONFIRMED)

    def test_no_glow_widget_is_assumed_never_confirmed(self):
        """取不到观测值时不许伪造 confirmed（ziputil.py:26-28 的同一条理由）。"""
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(NO_GLOW, zip_code="10001")
        self.assertIsNone(r["_zip_observed"])
        self.assertEqual(r["_zip_verify"], ZIP_VERIFY_ASSUMED)

    def test_blocked_page_is_unverified_not_assumed(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(CAPTCHA, zip_code="10001")
        self.assertEqual(r["title"], "[验证码拦截]")
        self.assertEqual(r["_zip_verify"], ZIP_VERIFY_UNVERIFIED)
        self.assertIsNone(r["_zip_observed"])


# ==========================================================================
# P4-2  完整性位图（按 HTML 区块存在性）
# ==========================================================================
class CompletenessBitmap(_ParserCase):
    ALL_THREE = (COMPLETENESS_MEASURED | COMPLETENESS_BREADCRUMB
                 | COMPLETENESS_DETAIL_TABLE | COMPLETENESS_MAIN_IMAGE)

    def test_bit_values_match_the_published_contract(self):
        """位定义已经写进 docs/sync_contract.md §6.4 发给沃尔玛侧了，不许改。"""
        self.assertEqual(COMPLETENESS_BREADCRUMB, 1)
        self.assertEqual(COMPLETENESS_DETAIL_TABLE, 2)
        self.assertEqual(COMPLETENESS_MAIN_IMAGE, 4)
        self.assertEqual(COMPLETENESS_MEASURED, 8)

    def test_full_page_sets_all_bits(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertEqual(self.slx(FULL)["_completeness"], self.ALL_THREE)

    def test_degraded_page_measured_but_no_blocks(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertEqual(self.slx(DEGRADED)["_completeness"], COMPLETENESS_MEASURED)

    def test_block_presence_not_parsed_value(self):
        """**本项的核心断言。**

        DEGRADED 与 EMPTY_BLOCKS 的解析值逐字段相同（category_tree ""、
        root_category_id "N/A"、manufacturer "N/A"、image_urls ""），
        但一个是「区块被删」另一个是「区块在、内容空」。按解析值判定的完整性
        无法区分二者；按区块存在性判定必须给出不同的位图。
        """
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        deg = self.slx(DEGRADED, zip_code="16602")
        emp = self.slx(EMPTY_BLOCKS)
        for f in ("category_tree", "root_category_id", "manufacturer", "image_urls"):
            self.assertEqual(deg[f], emp[f],
                             "前提被破坏：两张页面的 %s 解析值本应相同" % f)
        self.assertNotEqual(deg["_completeness"], emp["_completeness"])
        self.assertEqual(emp["_completeness"], self.ALL_THREE)
        self.assertEqual(deg["_completeness"], COMPLETENESS_MEASURED)

    def test_zero_means_unmeasured(self):
        """契约 §6.4：0 == 未测量，**不是**「三项都缺」。"""
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertEqual(self.slx(CAPTCHA)["_completeness"], 0)
        self.assertEqual(self.p.parse_product("", "B08N5WRWNW", "10001")["_completeness"], 0)
        # 而「三项都缺但测过」是 8，两者必须能区分
        self.assertNotEqual(self.slx(DEGRADED)["_completeness"], 0)

    def test_completeness_ok_semantics(self):
        """契约里 completeness_ok ⟺ (c & 8) != 0 AND (c & 7) == 7。"""
        def ok(c):
            return (c & COMPLETENESS_MEASURED) != 0 and (c & 7) == 7
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertTrue(ok(self.slx(FULL)["_completeness"]))
        self.assertFalse(ok(self.slx(DEGRADED)["_completeness"]))
        self.assertFalse(ok(self.slx(CAPTCHA)["_completeness"]))


# ==========================================================================
# P4-4  manufacturer 污染
# ==========================================================================
class ManufacturerKeyMatching(_ParserCase):
    def test_recommended_age_no_longer_wins(self):
        """FULL 里 "Manufacturer" 在前、"Manufacturer recommended age" 在后。

        子串匹配 + 无条件覆盖 ⇒ 后者赢 ⇒ manufacturer == "3 years and up"。
        """
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertEqual(self.slx(FULL)["manufacturer"], "Acme Industrial Ltd.")

    def test_document_order_does_not_matter(self):
        """把两行调换顺序，结果必须不变 —— 翻转正是两次误复审的来源。"""
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        swapped = FULL.replace(
            '<tr><th class="prodDetSectionEntry">Manufacturer</th><td class="prodDetAttrValue">Acme Industrial Ltd.</td></tr>\n'
            '  <tr><th class="prodDetSectionEntry">Manufacturer recommended age</th><td class="prodDetAttrValue">3 years and up</td></tr>',
            '<tr><th class="prodDetSectionEntry">Manufacturer recommended age</th><td class="prodDetAttrValue">3 years and up</td></tr>\n'
            '  <tr><th class="prodDetSectionEntry">Manufacturer</th><td class="prodDetAttrValue">Acme Industrial Ltd.</td></tr>')
        self.assertNotEqual(swapped, FULL, "夹具替换没生效，用例失去意义")
        self.assertEqual(self.slx(swapped)["manufacturer"], "Acme Industrial Ltd.")

    def test_key_variants(self):
        d = {}
        for key, expect in (
            ("Manufacturer", True),
            ("manufacturer", True),
            ("Manufacturer ‏ : ‎", True),          # detailBullets 的双向标记 + 冒号
            ("Manufacturer Name", True),
            ("Manufactured by", True),
            ("Manufacturer recommended age", False),
            ("Manufacturer Recommended Age", False),
            ("Manufacturer minimum age", False),
            ("Manufacturer’s suggested age", False),
            ("Manufacturer contact information", False),
            ("Manufacturer warranty", False),
        ):
            d.clear()
            self.p._map_detail(d, key, "VALUE")
            self.assertEqual("manufacturer" in d, expect,
                             "键 %r 的归属判错（归一化后 = %r）"
                             % (key, self.p._norm_detail_key(key)))

    def test_manufacturer_part_number_still_lands_in_part_number(self):
        """'part number' 的判断排在前面，这条历史行为不能被改坏。"""
        d = {}
        self.p._map_detail(d, "Manufacturer Part Number", "AC-1200")
        self.assertEqual(d.get("part_number"), "AC-1200")
        self.assertNotIn("manufacturer", d)


# ==========================================================================
# P4-5  set 顺序 —— 跨进程
# ==========================================================================
_CHILD = r"""
import os, sys
sys.path.insert(0, os.environ["REPO_ROOT"])
sys.modules.pop("worker.parser", None)
from worker.parser import AmazonParser
html = sys.stdin.read()
p = AmazonParser()
r = p.parse_product(html, "B08N5WRWNW", "10001")
r2 = p.parse_product(html.replace("dimensionValuesDisplayData", "xxDISABLEDxx"),
                     "B08N5WRWNW", "10001")
sys.stdout.write("%s\n%s\n" % (r["upc_list"], r2["variation_asins"]))
"""


class SetOrderDeterminism(_ParserCase):
    """``",".join(list(set(...)))`` 的顺序由 str 哈希决定，而 CPython 每个进程
    给 str 哈希**重新加盐**。所以单进程测试结构上看不见这个 bug —— 一个进程里
    它永远自洽。必须开多个进程。
    """

    def test_sorted_within_one_process(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(FULL)
        upcs = r["upc_list"].split(",")
        self.assertEqual(upcs, sorted(upcs))
        self.assertGreater(len(upcs), 1, "夹具至少要有两个 UPC，否则测不出顺序")

    def test_identical_across_five_separate_processes(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        outs = []
        # 显式给 5 个**不同**的 PYTHONHASHSEED：不加盐差异的话这个用例只能
        # 概率性地发现回归。实测（HEAD a34e0c6 的旧实现）这 5 个种子给出 5 个
        # 互不相同的 upc_list 顺序，所以一旦有人改回 list(set(...))，必红。
        for seed in ("0", "1", "2", "3", "4"):
            env = dict(os.environ, REPO_ROOT=_REPO_ROOT, PYTHONHASHSEED=seed)
            proc = subprocess.run(
                [sys.executable, "-c", _CHILD], input=FULL, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=120)
            self.assertEqual(proc.returncode, 0,
                             "子进程失败(seed=%s)：%s" % (seed, proc.stderr[-2000:]))
            outs.append(proc.stdout.strip().splitlines())

        upc_orders = {o[0] for o in outs}
        var_orders = {o[1] for o in outs}
        self.assertEqual(len(upc_orders), 1,
                         "upc_list 跨进程不稳定，实测到 %d 种顺序：%r"
                         % (len(upc_orders), sorted(upc_orders)))
        self.assertEqual(len(var_orders), 1,
                         "variation_asins 跨进程不稳定，实测到 %d 种顺序：%r"
                         % (len(var_orders), sorted(var_orders)))
        only = outs[0][0].split(",")
        self.assertEqual(only, sorted(only))

    def test_ean_join_is_sorted_too(self):
        """ean_list 当前恒空，但用的是同一个模式，别把地雷留在原地。"""
        html = '{"gtin13":"4006381337922"}{"gtin13":"0123456789012"}'
        self.assertEqual(self.p._parse_ean(html),
                         "0123456789012,4006381337922")


# ==========================================================================
# P4-6 / P4-9  两条引擎路径
# ==========================================================================
class CarriedForwardFields(_ParserCase):
    """rating / review_count / seller_id / seller_name。

    这四个键以前**只**在 selectolax 路径被赋值。lxml 路径上键根本不存在 ⇒
    server 的 `val = data.get(f); if val is not None:`（common/database.py:1906-1912）
    跳过 ⇒ 行里留着上一次采集的值，却配着一个全新的 crawl_time。
    """

    def test_present_in_default_result(self):
        d = self.p._default_result("B08N5WRWNW", "10001")
        for f in _FOUR_CARRIED:
            self.assertIn(f, d)
            self.assertIsNotNone(d[f])

    def test_selectolax_path_produces_all_four(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(FULL)
        for f in _FOUR_CARRIED:
            self.assertIn(f, r)
        self.assertEqual(r["rating"], "4.4")
        self.assertEqual(r["review_count"], "1234")

    @unittest.skipUnless(_HAS_LXML, "lxml 未安装")
    def test_lxml_path_produces_all_four(self):
        r = self.lxm(FULL)
        for f in _FOUR_CARRIED:
            self.assertIn(f, r, "lxml 路径缺键 %s —— 这正是旧值结转的入口" % f)
        self.assertEqual(r["rating"], "4.4")
        self.assertEqual(r["review_count"], "1234")

    def test_amazon_first_party_seller_is_detected(self):
        """`Sold by <a>Amazon.com</a>` —— 真实页面的嵌套形状。

        老实现用 selectolax 默认的 `text(strip=True)`（**无分隔符**拼接），
        拿到的是 `Sold byAmazon.com`，子串判断从来没命中过，自营页恒 ("N/A","N/A")。
        """
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(FULL)
        self.assertEqual(r["seller_id"], "AMAZON")
        self.assertEqual(r["seller_name"], "Amazon.com")

    def test_third_party_seller(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        r = self.slx(THIRD_PARTY)
        self.assertEqual(r["seller_id"], "A1X2Y3Z4W5V6U7")
        self.assertEqual(r["seller_name"], "Acme Direct")


class ParseEngineRecorded(_ParserCase):
    def test_selectolax_tagged(self):
        if not _HAS_SLX:
            self.skipTest("selectolax 未安装")
        self.assertEqual(self.slx(FULL)["_parse_engine"], ENGINE_SELECTOLAX)
        # 连拦截页也要能归因到引擎
        self.assertEqual(self.slx(CAPTCHA)["_parse_engine"], ENGINE_SELECTOLAX)

    @unittest.skipUnless(_HAS_LXML, "lxml 未安装")
    def test_lxml_tagged(self):
        self.assertEqual(self.lxm(FULL)["_parse_engine"], ENGINE_LXML)

    def test_none_when_no_engine_ran(self):
        self.assertIsNone(
            self.p.parse_product("", "B08N5WRWNW", "10001")["_parse_engine"])


@unittest.skipUnless(_HAS_SLX and _HAS_LXML, "selectolax 或 lxml 未安装")
class EngineParity(_ParserCase):
    """两条路径除了 `_parse_engine` 本身之外必须逐字段一致。

    引擎分叉一旦发生，同一个商品换台机器采就换一组值 —— 而这在事件流里
    表现为「无缘无故的哈希翻转」。P4-9 记录引擎是为了让分叉**可归因**；
    这个用例是为了让分叉**先别发生**。
    """

    FIELDS = ("_zip_observed", "_zip_verify", "_completeness", "zip_code",
              "rating", "review_count", "seller_id", "seller_name",
              "manufacturer", "upc_list", "variation_asins", "site",
              "category_tree", "root_category_id", "title", "image_urls",
              "brand", "model_number", "country_of_origin", "parent_asin",
              "variant_attributes", "bullet_points", "current_price")

    def test_parity_on_all_fixtures(self):
        for name, html, zip_code in (("FULL", FULL, "90210"),
                                     ("DEGRADED", DEGRADED, "16602"),
                                     ("EMPTY_BLOCKS", EMPTY_BLOCKS, "10001"),
                                     ("THIRD_PARTY", THIRD_PARTY, "10001")):
            a = self.slx(html, zip_code=zip_code)
            b = self.lxm(html, zip_code=zip_code)
            for f in self.FIELDS:
                self.assertEqual(a.get(f, "<<ABSENT>>"), b.get(f, "<<ABSENT>>"),
                                 "%s: 字段 %s 两引擎不一致" % (name, f))
            self.assertNotEqual(a["_parse_engine"], b["_parse_engine"])


if __name__ == "__main__":
    unittest.main()
