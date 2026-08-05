"""common/slowhash.py 的自检。

重点不在"函数返回了个 hex"，在四件**单进程测不出来**或**容易被想当然**的事：

1. **跨进程确定性**。现成的 ``content_hash`` 就栽在这里：``_HASH_FIELDS`` 里的
   ``upc_list`` / ``variation_asins`` 由 ``",".join(list(set(...)))`` 生成，
   ``set`` 迭代顺序取决于 str 哈希、按 ``PYTHONHASHSEED`` 每进程随机。
   同一个进程里跑一万遍都发现不了。这里真的起 5 个独立解释器。
2. **哨兵全等匹配**。``startswith("[")`` 会把 ``[2-Pack] Storage Bins`` 打成 null。
3. **哪些字段该忽略**。价格/库存/BSR/评分变了，两个哈希都不许动。
4. **两个哈希的分工**。review 窄、slow 宽；改 manufacturer 只有 slow 该翻。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from common.slowhash import (
    HASH_VER,
    REVIEW_HASH_FIELDS,
    SENTINELS,
    SLOW_HASH_FIELDS,
    canonical_object,
    compute_hashes,
    extract_image_ids,
    normalize_text,
    parse_variant_attributes,
    review_hash,
    slow_hash,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# 样本
# --------------------------------------------------------------------------

def _sample() -> dict:
    """一条形态贴近真实 parser 输出的结果 dict（分隔字符串，不是 JSON）。"""
    return {
        "asin": "B0CXXXXXXX",
        "title": "Acme Widget Pro, 12-Pack",
        "brand": "Acme",
        "product_type": "Widget",
        "manufacturer": "Acme Manufacturing",
        "model_number": "AW-12",
        "part_number": "AW12-BLK",
        "country_of_origin": "China",
        "is_customized": "No",
        "long_description": "A very long description.",
        "bullet_points": "Durable steel body\nFits standard mounts\n12 per box",
        "image_urls": (
            "https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg\n"
            "https://m.media-amazon.com/images/I/61XYZ789._AC_SX679_.jpg"
        ),
        "upc_list": "012345678905,889698435154,195949012345",
        "ean_list": "",
        "parent_asin": "B0CPARENT1",
        "variation_asins": "B0CSIB0001,B0CSIB0002",
        "variant_attributes": "color_name=Red; size_name=L",
        "root_category_id": "16225009011",
        "category_ids": "16225009011,1055398",
        "category_tree": "Home & Kitchen > Storage > Bins",
        "first_available_date": "January 1, 2020",
        "package_dimensions": "10 x 8 x 4 inches",
        "package_weight": "2.5 pounds",
        "item_dimensions": "9 x 7 x 3 inches",
        "item_weight": "2.1 pounds",
        # 快变字段——两个哈希都必须无视
        "best_sellers_rank": "#1,234 in Home & Kitchen",
        "original_price": "29.99",
        "current_price": "19.99",
        "buybox_price": "19.99",
        "buybox_shipping": "FREE",
        "is_fba": "Yes",
        "stock_count": "42",
        "stock_status": "In Stock",
        "delivery_date": "Tomorrow",
        "delivery_time": "1 day",
        "rating": "4.5",
        "review_count": "1234",
        "seller_id": "A1SELLER",
        "seller_name": "Acme Store",
        # 采集参数
        "site": "US",
        "zip_code": "10001",
        "crawl_time": "2026-08-05 10:00:00",
        "product_url": "https://www.amazon.com/dp/B0CXXXXXXX",
    }


# --------------------------------------------------------------------------
# 1. 跨进程确定性 —— 本文件存在的理由
# --------------------------------------------------------------------------

_CHILD_HASH = """
import json, sys
sys.path.insert(0, %(root)r)
from common.slowhash import review_hash, slow_hash
data = json.loads(sys.stdin.read())
print(review_hash(data))
print(slow_hash(data))
"""

# 对照组：复刻 worker/parser.py:1563-1569 / :738-746 的 ",".join(list(set(...)))。
# 它必须在同一套 5 进程夹具下**跳变**——否则说明夹具根本没在制造不同的 set 顺序，
# 上面那个"输出一致"的断言就是空的。
_CHILD_SETJOIN = """
import re, sys
html = '"upc":"012345678905" "upc":"889698435154" "upc":"195949012345" '
html += '"upc":"760557849490" "upc":"014633743937" "upc":"711719541028" '
html += '"upc":"045496590420" "upc":"887276392837" "upc":"194252056318" '
html += '"upc":"190198001787" "upc":"810116120031" "upc":"073558766544"'
print(",".join(list(set(re.findall(r'"upc":"(\\d+)"', html)))))
"""

_SEEDS = ("0", "1", "2", "3", "4")


def _run_children(code: str, stdin: str = "") -> list:
    """在 5 个**独立解释器进程**里跑同一段代码，每个进程一个不同的 PYTHONHASHSEED。"""
    outs = []
    for seed in _SEEDS:
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = seed
        proc = subprocess.run(
            [sys.executable, "-c", code],
            input=stdin,
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            env=env,
            timeout=120,
        )
        assert proc.returncode == 0, proc.stderr
        outs.append(proc.stdout.strip())
    return outs


def test_cross_process_determinism():
    """同一份输入，5 个独立进程 × 5 个不同 PYTHONHASHSEED，必须输出同一个哈希。"""
    payload = json.dumps(_sample(), ensure_ascii=False)
    outs = _run_children(_CHILD_HASH % {"root": str(REPO_ROOT)}, stdin=payload)
    assert len(set(outs)) == 1, "跨进程哈希不一致:\n" + "\n".join(outs)
    # 同时确认子进程算的和父进程一致。
    parent = "%s\n%s" % (review_hash(_sample()), slow_hash(_sample()))
    assert outs[0] == parent


def test_cross_process_determinism_survives_permuted_multivalue_input():
    """多值字段以不同顺序递进来（正是 set 顺序会造成的），跨进程仍是同一个哈希。

    这一条把"跨进程"和"顺序无关"两个性质**同时**压住：即便 parser 侧的
    ``set`` 每个进程吐出不同顺序，服务端算出的哈希也不动。
    """
    orders = [
        "012345678905,889698435154,195949012345",
        "195949012345,012345678905,889698435154",
        "889698435154,195949012345,012345678905",
        "195949012345,889698435154,012345678905",
        "889698435154,012345678905,195949012345",
    ]
    outs = []
    for seed, upcs in zip(_SEEDS, orders):
        data = _sample()
        data["upc_list"] = upcs
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = seed
        proc = subprocess.run(
            [sys.executable, "-c", _CHILD_HASH % {"root": str(REPO_ROOT)}],
            input=json.dumps(data, ensure_ascii=False),
            capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=120,
        )
        assert proc.returncode == 0, proc.stderr
        outs.append(proc.stdout.strip())
    assert len(set(outs)) == 1, "顺序扰动 + 跨进程后哈希不一致:\n" + "\n".join(outs)


def test_the_probe_can_actually_detect_the_bug():
    """夹具灵敏度对照：老的 ",".join(list(set(...))) 在同一套 5 进程下必须跳变。

    没有这一条，上面两个"全都相同"的断言可能只是因为夹具压根没造成差异。
    """
    outs = _run_children(_CHILD_SETJOIN)
    assert len(set(outs)) > 1, (
        "对照组没有跳变，说明这套 5 进程夹具没在制造不同的 set 顺序，"
        "上面的确定性断言是空的:\n" + "\n".join(outs)
    )


# --------------------------------------------------------------------------
# 2. 哨兵
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sentinel", sorted(SENTINELS))
def test_every_sentinel_normalizes_to_none(sentinel):
    assert normalize_text(sentinel) is None


def test_real_title_starting_with_bracket_survives():
    """``startswith("[")`` 会把这个真标题打成 null。全等匹配不会。"""
    assert normalize_text("[2-Pack] Storage Bins") == "[2-pack] storage bins"

    a, b = _sample(), _sample()
    a["title"] = "[2-Pack] Storage Bins"
    b["title"] = "[3-Pack] Storage Bins"
    assert review_hash(a) != review_hash(b), "方括号开头的真标题被当哨兵吞了"


def test_sentinel_only_matches_exactly_not_as_substring():
    assert normalize_text("Contains N/A in text") == "contains n/a in text"
    assert normalize_text("[商品不存在]的替代品") == "[商品不存在]的替代品"


def test_sentinel_tolerates_surrounding_whitespace():
    assert normalize_text("  N/A  ") is None
    assert normalize_text("\n[验证码拦截]\t") is None


def test_absent_empty_and_na_are_all_equivalent():
    """缺席 / "" / "N/A" 三者必须算出同一个哈希，否则解析抖动 = 假变化。"""
    base = _sample()
    absent = {k: v for k, v in base.items() if k != "manufacturer"}
    empty = dict(base, manufacturer="")
    na = dict(base, manufacturer="N/A")
    assert slow_hash(absent) == slow_hash(empty) == slow_hash(na)
    # 而真值必须不同
    assert slow_hash(dict(base, manufacturer="Acme Manufacturing")) != slow_hash(na)


def test_sentinel_elements_are_dropped_from_lists():
    a = dict(_sample(), bullet_points="Durable steel body\nN/A\nFits standard mounts\n12 per box")
    b = dict(_sample(), bullet_points="Durable steel body\nFits standard mounts\n12 per box")
    assert review_hash(a) == review_hash(b)


# --------------------------------------------------------------------------
# 3. 列表排序
# --------------------------------------------------------------------------

def test_upc_list_order_does_not_matter():
    a = dict(_sample(), upc_list="012345678905,889698435154,195949012345")
    b = dict(_sample(), upc_list="195949012345,012345678905,889698435154")
    assert slow_hash(a) == slow_hash(b)
    assert canonical_object(a, SLOW_HASH_FIELDS)["upc_list"] == [
        "012345678905", "195949012345", "889698435154",
    ]


def test_image_url_order_does_not_matter():
    urls = [
        "https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg",
        "https://m.media-amazon.com/images/I/61XYZ789._AC_SX679_.jpg",
    ]
    a = dict(_sample(), image_urls="\n".join(urls))
    b = dict(_sample(), image_urls="\n".join(reversed(urls)))
    assert slow_hash(a) == slow_hash(b)


def test_category_tree_is_a_path_and_keeps_its_order():
    """category_tree 是 root→leaf 的路径，不是集合。排序会把它毁掉。"""
    a = dict(_sample(), category_tree="Home & Kitchen > Storage > Bins")
    b = dict(_sample(), category_tree="Bins > Storage > Home & Kitchen")
    assert review_hash(a) != review_hash(b)
    assert canonical_object(a, REVIEW_HASH_FIELDS)["category_tree"] == [
        "home & kitchen", "storage", "bins",
    ]


def test_bullet_points_keep_their_order():
    a = dict(_sample(), bullet_points="First\nSecond")
    b = dict(_sample(), bullet_points="Second\nFirst")
    assert review_hash(a) != review_hash(b)


def test_bullet_boundaries_are_not_collapsible():
    """换行折成空格会让 "A\\nB" 和 "A B" 撞车；拆成列表就不会。"""
    a = dict(_sample(), bullet_points="Blue\nLarge")
    b = dict(_sample(), bullet_points="Blue Large")
    assert review_hash(a) != review_hash(b)


def test_input_dict_key_order_does_not_matter():
    base = _sample()
    shuffled = {k: base[k] for k in sorted(base, reverse=True)}
    assert review_hash(base) == review_hash(shuffled)
    assert slow_hash(base) == slow_hash(shuffled)


# --------------------------------------------------------------------------
# 4. 图片 ID 归约
# --------------------------------------------------------------------------

def test_image_id_extraction():
    ids = extract_image_ids(
        "https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg\n"
        "https://m.media-amazon.com/images/I/61XYZ789._AC_SX679_.jpg"
    )
    assert ids == ["61xyz789", "71abc123"]


def test_image_id_dedupes_size_variants_of_the_same_image():
    """同一张图的不同尺寸归约到同一个 ID，必须去重——否则"页面列了几个尺寸"会翻转哈希。"""
    ids = extract_image_ids(
        "https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg\n"
        "https://m.media-amazon.com/images/I/71ABC123._AC_SX466_.jpg\n"
        "https://m.media-amazon.com/images/I/71ABC123.jpg"
    )
    assert ids == ["71abc123"]


def test_image_id_strips_query_and_fragment():
    assert extract_image_ids(
        "https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg?v=2#frag"
    ) == ["71abc123"]


def test_unrecognisable_image_url_is_kept_whole_not_dropped():
    """认不出 ID 就保留整条 URL。丢弃会让两组不同的图塌缩成同一个值 = 假"没变"。"""
    a = extract_image_ids("https://example.com/a/b/")
    b = extract_image_ids("https://example.com/c/d/")
    assert a and b and a != b


def test_empty_image_urls_is_none_not_empty_list():
    assert canonical_object({"image_urls": ""}, SLOW_HASH_FIELDS)["image_ids"] is None
    assert canonical_object({}, SLOW_HASH_FIELDS)["image_ids"] is None


# --------------------------------------------------------------------------
# 5. Unicode 归一化
# --------------------------------------------------------------------------

def test_nfkc_folds_compatibility_forms():
    assert normalize_text("ＡＣＭＥ Ｗｉｄｇｅｔ") == normalize_text("ACME Widget")
    assert normalize_text("ﬁlter") == normalize_text("filter")
    assert normalize_text("①") == "1"


def test_whitespace_is_folded_and_stripped():
    assert normalize_text("  Acme   Widget \t Pro \n") == "acme widget pro"
    assert normalize_text("Acme Widget") == "acme widget"  # NBSP → 空格


def test_casefold_makes_case_irrelevant():
    a = dict(_sample(), title="ACME WIDGET PRO, 12-PACK")
    b = dict(_sample(), title="acme widget pro, 12-pack")
    assert review_hash(a) == review_hash(b)


def test_nul_is_stripped():
    """jsonb 与 text 列都存不下 NUL；哈希不该对消费侧永远看不到的差异翻转。"""
    assert normalize_text("bad\x00title") == "badtitle"


def test_hash_is_stable_under_equivalent_unicode_spellings():
    a = dict(_sample(), title="Café Widget")            # é 单码点
    b = dict(_sample(), title="Café Widget")      # e + 组合重音
    assert review_hash(a) == review_hash(b)


# --------------------------------------------------------------------------
# 6. variant_attributes
# --------------------------------------------------------------------------

def test_variant_attributes_parse_and_sort_by_key():
    assert parse_variant_attributes("color_name=Red; size_name=L") == {
        "color_name": "red", "size_name": "l",
    }
    a = dict(_sample(), variant_attributes="color_name=Red; size_name=L")
    b = dict(_sample(), variant_attributes="size_name=L; color_name=Red")
    assert review_hash(a) == review_hash(b), "k=v 的书写顺序不该影响哈希"


def test_variant_attribute_keys_are_casefolded():
    assert parse_variant_attributes("Color_Name=Red") == {"color_name": "red"}


def test_variant_attributes_unkeyed_fallback_keeps_value_order():
    """拿不到维度名时 parser 只吐值（worker/parser.py:1654），顺序对应维度顺序。"""
    parsed = parse_variant_attributes("Red; L")
    assert parsed == {"": ["red", "l"]}
    a = dict(_sample(), variant_attributes="Red; L")
    b = dict(_sample(), variant_attributes="L; Red")
    assert review_hash(a) != review_hash(b)


def test_variant_attributes_empty_is_none():
    assert parse_variant_attributes("") is None
    assert parse_variant_attributes("N/A") is None
    assert parse_variant_attributes(None) is None


# --------------------------------------------------------------------------
# 7. 两个哈希的字段集分工
# --------------------------------------------------------------------------

_FAST_CHANGING = {
    "best_sellers_rank": "#9,999 in Home & Kitchen",
    "current_price": "9.99",
    "original_price": "39.99",
    "buybox_price": "9.99",
    "buybox_shipping": "PAID",
    "is_fba": "No",
    "stock_count": "0",
    "stock_status": "Out of Stock",
    "delivery_date": "Next week",
    "delivery_time": "7 days",
    "rating": "1.0",
    "review_count": "99999",
    "seller_id": "A2OTHER",
    "seller_name": "Other Store",
    "variation_asins": "B0CDIFF001,B0CDIFF002",
    "ean_list": "1234567890123",
    "crawl_time": "2027-01-01 00:00:00",
    "zip_code": "90210",
    "site": "UK",
    "screenshot_path": "/tmp/other.png",
}


@pytest.mark.parametrize("field,new", sorted(_FAST_CHANGING.items()))
def test_neither_hash_reacts_to_fast_changing_fields(field, new):
    base = _sample()
    changed = dict(base, **{field: new})
    assert review_hash(base) == review_hash(changed), f"review_hash 不该看 {field}"
    assert slow_hash(base) == slow_hash(changed), f"slow_hash 不该看 {field}"


def test_both_hashes_react_to_a_title_change():
    base = _sample()
    changed = dict(base, title="Acme Widget Pro, 24-Pack")
    assert review_hash(base) != review_hash(changed)
    assert slow_hash(base) != slow_hash(changed)


@pytest.mark.parametrize("field,new", [
    ("brand", "Globex"),
    ("product_type", "Gadget"),
    ("root_category_id", "999"),
    ("category_tree", "Home & Kitchen > Storage > Baskets"),
    ("bullet_points", "Durable steel body\nNow fits wide mounts\n12 per box"),
    ("variant_attributes", "color_name=Blue; size_name=L"),
])
def test_both_hashes_react_to_review_fields(field, new):
    base = _sample()
    changed = dict(base, **{field: new})
    assert review_hash(base) != review_hash(changed)
    assert slow_hash(base) != slow_hash(changed)


@pytest.mark.parametrize("field,new", [
    ("manufacturer", "Globex Manufacturing"),
    ("model_number", "AW-24"),
    ("part_number", "AW24-RED"),
    ("country_of_origin", "Vietnam"),
    ("is_customized", "Yes"),
    ("long_description", "A different description."),
    ("upc_list", "012345678905,889698435154,999999999999"),
    ("image_urls", "https://m.media-amazon.com/images/I/99NEW999._AC_SL1500_.jpg"),
    ("parent_asin", "B0COTHER01"),
    ("package_dimensions", "20 x 8 x 4 inches"),
    ("package_weight", "5.0 pounds"),
    ("item_dimensions", "19 x 7 x 3 inches"),
    ("item_weight", "4.5 pounds"),
    ("first_available_date", "March 3, 2021"),
])
def test_only_slow_hash_reacts_to_slow_only_fields(field, new):
    """两个哈希的分工全在这里：这些字段变了不该触发复审，但身份层要能看见。"""
    base = _sample()
    changed = dict(base, **{field: new})
    assert review_hash(base) == review_hash(changed), f"review_hash 不该看 {field}"
    assert slow_hash(base) != slow_hash(changed), f"slow_hash 必须看 {field}"


def test_field_sets_match_the_spec():
    assert REVIEW_HASH_FIELDS == (
        "title", "brand", "product_type", "root_category_id",
        "category_tree", "bullet_points", "variant_attributes",
    )
    assert SLOW_HASH_FIELDS[:len(REVIEW_HASH_FIELDS)] == REVIEW_HASH_FIELDS
    assert set(SLOW_HASH_FIELDS) - set(REVIEW_HASH_FIELDS) == {
        "manufacturer", "model_number", "part_number", "country_of_origin",
        "is_customized", "long_description", "upc_list", "image_ids",
        "parent_asin", "package_dimensions", "package_weight",
        "item_dimensions", "item_weight", "first_available_date",
    }


# --------------------------------------------------------------------------
# 8. 输出形态 / outcome 门
# --------------------------------------------------------------------------

def test_output_format():
    h = review_hash(_sample())
    assert HASH_VER == "v1"
    assert h.startswith("v1:")
    hex_part = h.split(":", 1)[1]
    assert len(hex_part) == 64 and all(c in "0123456789abcdef" for c in hex_part)


def test_compute_hashes_nulls_out_non_ok_outcomes():
    """outcome != 'ok' 的 payload 是占位符堆，对它取哈希 = 好→404→好 每次翻转。"""
    data = _sample()
    assert compute_hashes(data, "ok") == (review_hash(data), slow_hash(data))
    for outcome in ("not_found", "blocked", "parse_failed", "stale"):
        assert compute_hashes(data, outcome) == (None, None)


def test_empty_dict_does_not_explode():
    assert review_hash({}).startswith("v1:")
    assert slow_hash({}).startswith("v1:")
    assert all(v is None for v in canonical_object({}, SLOW_HASH_FIELDS).values())


def test_canonical_object_key_set_is_fixed():
    """键集固定 —— "字段消失"与"字段是 N/A"必须落在同一个规范化对象上。"""
    assert set(canonical_object({}, SLOW_HASH_FIELDS)) == set(SLOW_HASH_FIELDS)
    assert set(canonical_object(_sample(), REVIEW_HASH_FIELDS)) == set(REVIEW_HASH_FIELDS)


# --------------------------------------------------------------------------
# 9. 不许碰旧哈希
# --------------------------------------------------------------------------

def test_legacy_hashes_are_untouched():
    """asin_changes 依赖 content_hash / title_bullets_hash，Phase 1 等价性要求逐字不变。"""
    import hashlib

    from common.database import (
        _HASH_FIELDS,
        _TITLE_BULLETS_FIELDS,
        _compute_content_hash,
        _compute_title_bullets_hash,
    )

    data = _sample()
    expect_content = hashlib.md5(
        "|".join(str(data.get(f, "") or "") for f in _HASH_FIELDS).encode()
    ).hexdigest()
    expect_tb = hashlib.md5(
        "|".join(str(data.get(f, "") or "") for f in _TITLE_BULLETS_FIELDS).encode()
    ).hexdigest()
    assert _compute_content_hash(data) == expect_content
    assert _compute_title_bullets_hash(data) == expect_tb
    # 新哈希与旧哈希是不同的东西，别有人把它们接错。
    assert not _compute_content_hash(data).startswith("v1:")


def test_image_url_containing_commas_is_not_shredded():
    """老式 Amazon 变换参数里含逗号（``._AC_,0,0_.jpg``）。按逗号切会把一条 URL
    打成碎片混进哈希；image_urls 的分隔符只有换行。"""
    assert extract_image_ids(
        "https://m.media-amazon.com/images/I/71ABC123._AC_,0,0_.jpg"
    ) == ["71abc123"]
