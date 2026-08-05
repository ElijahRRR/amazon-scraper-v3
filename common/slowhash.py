"""common/slowhash.py —— 事件流 v1 慢字段哈希（`.agent/pg_migration_plan.md` §4 的实现）。

本模块是**自包含**的：只依赖标准库，不 import 任何 `common.*` / `worker.*`，
不碰数据库，没有副作用。任何一侧（采集侧写钩子、relay、将来的导出 API）都可以
直接 `from common.slowhash import review_hash, slow_hash`。

产出两个哈希，都是 ``"v1:<sha256 hex>"``：

* ``review_hash`` —— **复审门**。字段集刻意窄：只有那些一变就意味着
  "这条商品需要人再看一眼"的身份字段。
* ``slow_hash`` —— **身份层变化检测，不当门**。review 字段全集 + 更宽的一圈
  慢变字段。它变了只是"身份层动过"，不触发复审。

--------------------------------------------------------------------------
为什么不复用现成的 ``content_hash``
--------------------------------------------------------------------------

``common/database.py:265`` 的 ``_compute_content_hash`` **跨进程不可复现**，
因此不能用作任何跨机器/跨时间的比较基准。根因不在哈希函数，在它的输入：
``_HASH_FIELDS`` 里的 ``upc_list`` 与 ``variation_asins`` 由
``worker/parser.py:1563-1569`` / ``:738-746`` 的 ``",".join(list(set(...)))``
生成，而 ``set`` 的迭代顺序取决于 str 哈希，后者按 ``PYTHONHASHSEED`` 每进程随机。
同一张没变过的页面，两个 worker 会算出不同的 ``content_hash``。

本模块的对策不是"换个哈希算法"，而是**把顺序从输入里消除**：所有多值字段
先解析成列表、再显式排序（见 ``_LIST_FIELDS``），因此调用方以任何顺序递进来
同一组值，输出都相同。``tests/test_slowhash.py`` 里有跨 5 个独立解释器进程的
断言，单进程测试**测不出**这类 bug。

⚠ 本模块**不修改** ``content_hash`` / ``title_bullets_hash``：``asin_changes``
依赖它们，Phase 1 的等价性要求它们逐字不变。这里是新增的第三、第四个哈希。

--------------------------------------------------------------------------
归一化（§4.2）
--------------------------------------------------------------------------

1. 哨兵值 **全等匹配** 归一为 ``None``（见 ``SENTINELS``）。
   **绝不用 ``startswith("[")``** —— ``[2-Pack] Storage Bins`` 是真标题。
2. Unicode NFKC → 连续空白折叠为单空格 → ``strip()`` → ``casefold()``。
3. 所有列表字段排序：``upc_list``、``category_ids``、``image_ids``。
4. ``image_urls`` → 图片 ID：``.../images/I/71ABC123._AC_SL1500_.jpg`` → ``71ABC123``。
5. ``variant_attributes`` 解析为 k→v，key ``casefold`` 后按 key 排序。
6. ``json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",",":"))``
   → SHA-256 → ``"v1:<hex>"``。

缺席、``""``、``"N/A"`` 三者归一后**完全等价**（都是 ``null``），因此
"这次没解析到 manufacturer"和"这次解析到 N/A"不会造成哈希翻转。
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "HASH_VER",
    "SENTINELS",
    "REVIEW_HASH_FIELDS",
    "SLOW_HASH_FIELDS",
    "review_hash",
    "slow_hash",
    "compute_hashes",
    "canonical_object",
    "normalize_text",
    "extract_image_ids",
    "parse_variant_attributes",
]

# ``hash_ver``：写进 scrape_events.hash_ver，也是输出字符串的前缀。
# 升级策略见 §4.4（过渡期双输出，消费侧继续用旧版把门）。
HASH_VER = "v1"


# --------------------------------------------------------------------------
# 哨兵值
# --------------------------------------------------------------------------
# 全等匹配，一个都不能靠前缀判断。
# "N/A" / "" 来自 worker/parser.py:1339-1370 的 _default_result；
# 四个方括号值来自 worker/engine.py 的拦截/失败分支与 :1172 的 404 分支。
SENTINELS = frozenset({
    "N/A",
    "",
    "[页面为空]",
    "[HTML解析失败]",
    "[验证码拦截]",
    "[API封锁]",
    "[商品不存在]",
})


# --------------------------------------------------------------------------
# 字段集（§4.1）
# --------------------------------------------------------------------------
REVIEW_HASH_FIELDS: Tuple[str, ...] = (
    "title",
    "brand",
    "product_type",
    "root_category_id",
    "category_tree",
    "bullet_points",
    "variant_attributes",
)

# slow = review 全集 + 下面这一圈。
_SLOW_EXTRA_FIELDS: Tuple[str, ...] = (
    "manufacturer",
    "model_number",
    "part_number",
    "country_of_origin",
    "is_customized",
    "long_description",
    "upc_list",
    "image_ids",          # 由 image_urls 归约而来，见 extract_image_ids
    "parent_asin",
    "package_dimensions",
    "package_weight",
    "item_dimensions",
    "item_weight",
    "first_available_date",
)

SLOW_HASH_FIELDS: Tuple[str, ...] = REVIEW_HASH_FIELDS + _SLOW_EXTRA_FIELDS

# 有意排除的字段，及理由（§4.1 的表，复述在代码里免得后来人"顺手加回来"）：
#   best_sellers_rank        Amazon 每小时重算
#   variation_asins          set() 顺序跨进程不定；兜底正则会捞进赞助位 ASIN
#   rating / review_count    快变；lxml 路径不赋值，是结转旧值（§1.5 的四个结转字段）
#   seller_id / seller_name  BuyBox 轮换，属快变
#   价格 / 库存 / 配送 / is_fba   按定义属 snapshots 层
#   ean_list                 实测 100% 为空
#   crawl_time / zip_code / site   采集参数，不是商品属性
#   screenshot_path          内部字段
#   category_ids             与 category_tree 同源（同一段面包屑的 href 与文本），
#                            类目信号由 root_category_id + category_tree 承载。
#                            §4.2 规则 3 仍点名了它，因此归一化器把它注册为列表字段
#                            （排序 + 去重），只是当前两个哈希都不取它。


# --------------------------------------------------------------------------
# 各字段的形态
# --------------------------------------------------------------------------
# 需要排序的多值字段（§4.2 规则 3）。排序 = 消除跨进程 set 顺序。
_LIST_FIELDS = frozenset({"upc_list", "category_ids", "image_ids"})

# 保序的多值字段：顺序本身携带语义，排序反而破坏信息。
#   bullet_points —— DOM 顺序即卖点优先级
#   category_tree —— 是一条路径（root → leaf），不是集合
_ORDERED_LIST_FIELDS = frozenset({"bullet_points", "category_tree"})

# 各多值字段的切分符。parser 侧的拼接方式：
#   bullet_points  "\n".join(...)        worker/parser.py:667
#   image_urls     "\n".join(...)        worker/parser.py:734, :1491
#   upc_list       ",".join(...)         worker/parser.py:745
#   category_ids   ",".join(...)         worker/parser.py:762
#   category_tree  " > ".join(...)       worker/parser.py:764
_SPLIT_PATTERNS = {
    "bullet_points": re.compile(r"\n+"),
    "upc_list": re.compile(r","),
    "category_ids": re.compile(r","),
    "category_tree": re.compile(r">"),
    # 只切换行：parser 两条路径都是 "\n".join(...)（:734 / :1491），逗号从不是
    # image_urls 的分隔符。而老式 Amazon 图片 URL 的变换参数里**含逗号**
    # （如 ``._AC_,0,0_.jpg``），按逗号切会把一条 URL 打成碎片混进哈希。
    "image_ids": re.compile(r"\n+"),
}

_WHITESPACE_RE = re.compile(r"\s+")


# --------------------------------------------------------------------------
# 归一化原语
# --------------------------------------------------------------------------

def normalize_text(value: Any) -> Optional[str]:
    """单个标量归一化：哨兵 → None，否则 NFKC → 折叠空白 → strip → casefold。

    哨兵判定用**全等匹配**，且在 casefold **之前**做，比对两个形态：
      * 原始字符串本身；
      * NFKC + 空白折叠 + strip 之后（吃掉 ``" N/A "`` 这类前后空白）。
    不在 casefold 之后再比一次，是为了守住"全等"的字面含义。

    NUL（U+0000）无条件剔除：jsonb 与 text 列都表示不了它（22P05 / 22021），
    body 那侧本来就要洗掉（设计规格 §1.2）。哈希若保留 NUL，就会对
    "消费侧永远看不到的差异"翻转。
    """
    if value is None:
        return None
    if not isinstance(value, str):
        # 数字/布尔等：走 str()，保证可哈希且确定。
        value = str(value)

    if value in SENTINELS:
        return None

    text = value.replace("\x00", "")
    text = unicodedata.normalize("NFKC", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()

    if text in SENTINELS:
        return None

    text = text.casefold()
    return text or None


def _normalize_many(values: Iterable[Any]) -> List[str]:
    """逐元素归一化，丢掉归一化后为空/哨兵的元素。保序。"""
    out: List[str] = []
    for v in values:
        n = normalize_text(v)
        if n is not None:
            out.append(n)
    return out


def _dedupe_preserving_order(values: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _split(field: str, value: Any) -> List[str]:
    """把一个多值字段拆成元素列表。接受已经是 list/tuple 的输入（防御性）。"""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        parts: List[Any] = list(value)
    else:
        if not isinstance(value, str):
            value = str(value)
        # 整体是哨兵（如 "N/A"）就是空列表，别拆出一个 "n/a" 元素。
        if value in SENTINELS or value.strip() in SENTINELS:
            return []
        pattern = _SPLIT_PATTERNS.get(field)
        parts = pattern.split(value) if pattern is not None else [value]
    return _normalize_many(parts)


# --------------------------------------------------------------------------
# image_urls → image ids（§4.2 规则 4）
# --------------------------------------------------------------------------
# Amazon 图片 URL：https://m.media-amazon.com/images/I/71ABC123._AC_SL1500_.jpg
# 尺寸/格式后缀在第一个 '.' 之后，图片 ID 是 basename 里第一个 '.' 之前的部分。
_IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9%+_-]+$")


def extract_image_ids(image_urls: Any) -> List[str]:
    """把 image_urls 归约成**排序去重**的 Amazon 图片 ID 列表。

    ``.../images/I/71ABC123._AC_SL1500_.jpg`` → ``71abc123``（归一化后小写）。

    同一张图的不同尺寸会归约到同一个 ID，所以必须去重：否则哈希会随
    "这次页面列了几个尺寸变体"翻转，而那不是商品身份的变化。

    认不出 ID 形态的 URL（非 Amazon、被改写过的）**保留整条归一化后的 URL**，
    而不是丢弃——丢弃会让两组不同的图片塌缩成同一个值，造成"没变"的假象。
    """
    ids: List[str] = []
    for raw in _split("image_ids", image_urls):
        # raw 已经 NFKC + casefold + strip 过。
        url = raw.split("?", 1)[0].split("#", 1)[0]
        basename = url.rsplit("/", 1)[-1]
        candidate = basename.split(".", 1)[0]
        if candidate and _IMAGE_ID_RE.match(candidate):
            ids.append(candidate)
        else:
            ids.append(raw)
    return sorted(_dedupe_preserving_order(ids))


# --------------------------------------------------------------------------
# variant_attributes（§4.2 规则 5）
# --------------------------------------------------------------------------
# parser 侧两种形态（worker/parser.py:1649-1655）：
#   有维度名："color_name=Red; size_name=L"
#   拿不到维度名："Red; L"      ← 只有值，顺序对应维度顺序，不能乱排
_ATTR_SPLIT_RE = re.compile(r";")

# 无键值落在保留槽 ""。归一化后的真实 key 不可能是空串（空 → None → 丢弃），
# 所以这个槽不会与真 key 相撞。槽内保**序**：值的顺序对应维度顺序。
_UNKEYED_SLOT = ""


def parse_variant_attributes(value: Any) -> Optional[Dict[str, Any]]:
    """``"color_name=Red; size_name=L"`` → ``{"color_name": "red", "size_name": "l"}``。

    * key 归一化（含 casefold）后作为字典键；``json.dumps(sort_keys=True)`` 负责按 key 排序。
    * 重复 key 取**首次**出现，确定且与输入顺序无关地可复现。
    * 没有 ``=`` 的裸值收进保留槽 ``""`` 下的**有序**列表——它们的位置对应
      维度顺序，排序会丢掉这个信息。
    * 全空 → ``None``（与"字段缺席"等价）。
    """
    if value is None:
        return None
    if isinstance(value, dict):
        items: List[Tuple[Any, Any]] = list(value.items())
    else:
        if not isinstance(value, str):
            value = str(value)
        if value in SENTINELS or value.strip() in SENTINELS:
            return None
        items = []
        for chunk in _ATTR_SPLIT_RE.split(value):
            if "=" in chunk:
                k, _, v = chunk.partition("=")
                items.append((k, v))
            else:
                items.append((None, chunk))

    out: Dict[str, Any] = {}
    unkeyed: List[str] = []
    for k, v in items:
        nv = normalize_text(v)
        nk = normalize_text(k) if k is not None else None
        if nk is None:
            if nv is not None:
                unkeyed.append(nv)
            continue
        if nk in out:      # 重复 key：首次出现胜出
            continue
        out[nk] = nv

    if unkeyed:
        out[_UNKEYED_SLOT] = unkeyed
    return out or None


# --------------------------------------------------------------------------
# 规范化对象 → 哈希
# --------------------------------------------------------------------------

def _canonical_value(field: str, data: Dict[str, Any]) -> Any:
    """取一个字段的规范化值。缺席 / "" / "N/A" 一律得到 ``None``。"""
    if field == "image_ids":
        # 采集侧字段名是 image_urls；也接受调用方直接给 image_ids。
        raw = data.get("image_ids", data.get("image_urls"))
        return extract_image_ids(raw) or None

    if field == "variant_attributes":
        return parse_variant_attributes(data.get(field))

    raw = data.get(field)
    if field in _LIST_FIELDS:
        return sorted(_dedupe_preserving_order(_split(field, raw))) or None
    if field in _ORDERED_LIST_FIELDS:
        return _split(field, raw) or None
    return normalize_text(raw)


def canonical_object(data: Dict[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    """给定字段集，产出参与哈希的规范化 dict。

    键集**固定**（每个字段都在，缺席时值为 ``None``），这样"字段消失"与
    "字段是 N/A"不会算出两个哈希。调试和测试直接看这个对象，比看 hex 有用。
    """
    return {f: _canonical_value(f, data) for f in fields}


def _digest(obj: Dict[str, Any]) -> str:
    payload = json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return "%s:%s" % (HASH_VER, hashlib.sha256(payload.encode("utf-8")).hexdigest())


def review_hash(data: Dict[str, Any]) -> str:
    """复审门哈希。字段集见 ``REVIEW_HASH_FIELDS``。

    ⚠ 它只是复审判据的**一个合取项**。§4.3 的门是
    ``outcome='ok' AND 本条 completeness_ok AND 上条 completeness_ok
    AND review_hash 不同 AND hash_ver 相同``。
    单独拿 review_hash 变化当复审信号，会被"好页 → 软降级页 → 好页"打成两次误复审。
    """
    return _digest(canonical_object(data, REVIEW_HASH_FIELDS))


def slow_hash(data: Dict[str, Any]) -> str:
    """身份层变化检测哈希。字段集见 ``SLOW_HASH_FIELDS``。**不当门用。**"""
    return _digest(canonical_object(data, SLOW_HASH_FIELDS))


def compute_hashes(
    data: Dict[str, Any], outcome: str = "ok"
) -> Tuple[Optional[str], Optional[str]]:
    """``(review_hash, slow_hash)``；``outcome != 'ok'`` 时两个都是 ``None``。

    这条规则收在这里而不是散在调用方，是因为它是**哈希语义的一部分**：
    ``not_found`` 的 payload 有 30/40 个占位符，对它取哈希得到的是一个
    "好 → 404 → 好"每次都翻转的值——正是 §4.3 要防的误复审风暴。
    ``blocked`` / ``parse_failed`` / ``stale`` 同理。
    """
    if outcome != "ok":
        return None, None
    return review_hash(data), slow_hash(data)
