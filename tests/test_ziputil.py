"""
zip_effective_in_html 单元测试（纯 stdlib，只依赖 worker/ziputil.py）。

覆盖 Tier 2a 的邮编生效判定三分支：True / False / None。
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

from worker.ziputil import zip_effective_in_html  # noqa: E402


def _page(line2: str = None, body: str = "some product content") -> str:
    glow = ""
    if line2 is not None:
        glow = f'<span id="glow-ingress-line2" class="x">{line2}</span>'
    return f"<html><head></head><body>{glow}<div>{body}</div></body></html>"


class ZipEffectiveTests(unittest.TestCase):
    def test_match_returns_true(self):
        html = _page(line2="Altoona 16602")
        self.assertIs(zip_effective_in_html("16602", html), True)

    def test_match_with_whitespace(self):
        html = _page(line2="\n   New York 10001‌   ")
        self.assertIs(zip_effective_in_html("10001", html), True)

    def test_mismatch_returns_false(self):
        # glow 显示 10001（默认），但我们要的是 16602 → 明确未生效
        html = _page(line2="New York 10001")
        self.assertIs(zip_effective_in_html("16602", html), False)

    def test_non_us_currency_returns_false(self):
        # 无 glow 挂件，但页面出现非美国货币标识 → 明确未生效（串区）
        html = _page(line2=None, body="价格 ¥199.00 CNY")
        self.assertIs(zip_effective_in_html("16602", html), False)

    def test_no_glow_no_currency_returns_none(self):
        # 商品页既无 glow、也无异常货币 → 无法判定
        html = _page(line2=None, body="normal usd $19.99 product")
        self.assertIsNone(zip_effective_in_html("16602", html))

    def test_empty_inputs_return_none(self):
        self.assertIsNone(zip_effective_in_html("", _page(line2="New York 10001")))
        self.assertIsNone(zip_effective_in_html("10001", ""))

    def test_zip_as_substring_still_matches(self):
        # 目标邮编作为 city+zip 文本的子串出现即视为生效
        html = _page(line2="Beverly Hills 90210")
        self.assertIs(zip_effective_in_html("90210", html), True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
