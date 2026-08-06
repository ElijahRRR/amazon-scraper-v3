"""导出适配器的多值字段切分 —— 必须和 parser 的拼接方式一致。

这条防线是被**真机第一条真实记录**逼出来的，不是想出来的。当时导出的
``slow.images`` 是一个长度为 1 的列表，里面塞着 6 条用换行拼起来的 URL；
``slow.bullet_points`` 同理。两个都是契约字段，消费侧拿到的是垃圾。

根因：导出适配器自带了一份分隔符（images 按 ``,``、bullet_points 按 ``|``），
而 parser 的四条路径（selectolax/lxml × images/bullets）全是 ``"\n".join(...)``。
``common/slowhash.py`` 那份分隔符表**是对的**，所以 ``slow_hash`` 一直算得对 ——
只有对外那一份错了，两份各自演化，没人发现。

为什么原来的用例没抓到：夹具只喂了**一条** image URL，bullet_points 一次都没喂
内容。单元素输入下，任何分隔符都能"通过"。所以下面每条都用**多元素**输入。
"""
from __future__ import annotations

import os
import re
import sys
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from common.slowhash import extract_image_ids, split_multivalue  # noqa: E402
from server.api.export_incremental import _split_list, _to_record  # noqa: E402

# parser 拼接用的分隔符。改这里之前先改 parser，再改 slowhash 的 _SPLIT_PATTERNS。
PARSER_SEP = "\n"

_IMAGES = [
    "https://m.media-amazon.com/images/I/61VjrZZw2GL._AC_SL1151_.jpg",
    "https://m.media-amazon.com/images/I/61WAP4Do4xL._AC_SL1100_.jpg",
    "https://m.media-amazon.com/images/I/71tkTJ1oPRL._AC_SL1200_.jpg",
    # 老式变换参数里**带逗号**：按逗号切会把这一条打成三段碎片。
    "https://m.media-amazon.com/images/I/610tdJCWGLL._AC_,0,0_SL1091_.jpg",
]
_BULLETS = [
    "✔Door handle colour: antique black cabinet pull",
    "✔Dresser knobs vintage pull handle made of cast zinc alloy",
    "✔Package content: 4 Set x antiquepewter cabinet pulls",
]


class SplitAgreesWithParser(unittest.TestCase):

    def test_images_split_into_separate_urls(self):
        got = _split_list("image_urls", PARSER_SEP.join(_IMAGES))
        self.assertEqual(got, _IMAGES,
                         "images 必须切成一条一个元素，而不是整块塞进一个元素")

    def test_a_url_containing_commas_survives_intact(self):
        """这条守的是「比不切更坏」的那种坏法。"""
        got = _split_list("image_urls", PARSER_SEP.join(_IMAGES))
        self.assertIn("https://m.media-amazon.com/images/I/610tdJCWGLL._AC_,0,0_SL1091_.jpg",
                      got, "含逗号的 URL 被切碎了")

    def test_bullet_points_split_into_separate_lines(self):
        got = _split_list("bullet_points", PARSER_SEP.join(_BULLETS))
        self.assertEqual(got, _BULLETS)

    def test_case_is_preserved_in_image_urls(self):
        """不能顺手复用哈希那条切分路径：它会 casefold，而图片 ID 大小写敏感。"""
        got = _split_list("image_urls", PARSER_SEP.join(_IMAGES))
        self.assertTrue(any("61VjrZZw2GL" in u for u in got),
                        f"图片 ID 的大小写被改了：{got}")

    def test_single_element_input_still_works(self):
        """原来的夹具就停在这里 —— 保留它，但它证明不了什么，见模块文档。"""
        self.assertEqual(_split_list("image_urls", _IMAGES[0]), [_IMAGES[0]])

    def test_sentinels_and_empties_become_empty_list(self):
        for v in (None, "", "N/A", "n/a", "  "):
            self.assertEqual(_split_list("image_urls", v), [], repr(v))
            self.assertEqual(_split_list("bullet_points", v), [], repr(v))

    def test_export_and_hash_see_the_same_number_of_images(self):
        """导出列表与哈希输入必须同源。数量对不上说明两份分隔符又分叉了。"""
        blob = PARSER_SEP.join(_IMAGES)
        self.assertEqual(len(_split_list("image_urls", blob)),
                         len(extract_image_ids(blob)))

    def test_export_delegates_to_slowhash(self):
        blob = PARSER_SEP.join(_IMAGES)
        self.assertEqual(_split_list("image_urls", blob),
                         split_multivalue("image_urls", blob))


class ParserStillJoinsWithNewline(unittest.TestCase):
    """源码级守卫：parser 换了拼接符而没同步分隔符表的话，上面全部会假绿。

    这四个方法是 images/bullets 在两个引擎下的**全部**产出点。
    """

    def _body(self, name: str) -> str:
        src = open(os.path.join(_ROOT, "worker", "parser.py"), encoding="utf-8").read()
        m = re.search(rf"\n    def {name}\(.*?(?=\n    def )", src, re.S)
        self.assertIsNotNone(m, f"{name} 不见了 —— 本文件的前提失效，请更新")
        return m.group(0)

    def test_all_four_builders_join_with_newline(self):
        for name in ("_slx_parse_bullet_points", "_slx_parse_images",
                     "_parse_bullet_points", "_parse_images"):
            body = self._body(name)
            self.assertIn('"\\n".join', body,
                          f"{name} 不再用换行拼接了 —— "
                          f"common/slowhash.py 的 _SPLIT_PATTERNS 必须同步改")


class RecordLevelRegression(unittest.TestCase):
    """走完整的 _to_record，确认字段真的落到 slow.images / slow.bullet_points。"""

    def test_record_carries_split_lists(self):
        rec = _to_record({
            "source_id": "g:1", "seq": 1, "asin": "B0TEST0001",
            "collected_at": None, "recorded_at": None, "outcome": "ok",
            "completeness": 0,
            "payload": {"title": "T", "brand": "B",
                        "category_tree": "Home > Tools",
                        "image_urls": PARSER_SEP.join(_IMAGES),
                        "bullet_points": PARSER_SEP.join(_BULLETS)},
        })
        self.assertEqual(rec["slow"]["images"], _IMAGES)
        self.assertEqual(rec["slow"]["bullet_points"], _BULLETS)


if __name__ == "__main__":
    unittest.main()
