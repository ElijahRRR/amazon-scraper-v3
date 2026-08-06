#!/usr/bin/env python3
"""Phase 5 并行验证：同一批 ASIN 在新旧两套系统上采出来的内容比对。

    python tools/phase5_compare.py \
        --old  http://旧系统:8899 \
        --new  http://新系统:8899 \
        [--asins asins.txt] [--limit 500] [--json out.json]

------------------------------------------------------------------------
为什么需要这个工具，而不是直接 diff
------------------------------------------------------------------------

黄金样本证明的是**存储层语义等价**：它喂合成的结果字典，从不 import
worker.parser，也从不真的去抓亚马逊。所以「两个后端各 64/64」**证明不了**
「新系统采出来的数据和旧系统一样」——那正是 Phase 5 要回答的问题，也是整条
迁移里唯一还没有任何证据覆盖的环节。

但直接 diff 会**全红**：本次迁移**有意**改了七八个字段的值（解析器修复、
时区格式、列表排序…）。一个不认识这些有意变更的差异报告，会把真正的问题
淹死在噪声里。所以本工具把每一处差异分成三类：

    EXPECTED    有意变更，带 D 编号。**出现是正常的，不出现反而可疑**
    VOLATILE    价格/库存这类天然会变的（两次采集本来就隔了时间）
    UNEXPECTED  以上都不是 —— **这才是 Phase 5 要找的东西**

判定：UNEXPECTED 为 0 才算通过。EXPECTED 里若有**该出现却没出现**的，
单独列出来（说明那处修复没生效，或者两边跑的是同一个版本）。

------------------------------------------------------------------------
怎么用
------------------------------------------------------------------------

前提：两套系统都在跑，且**都已经采过同一批 ASIN**。
本工具不触发采集，只读 /api/results 做比对——所以你要先在两边各上传一次
同一份 ASIN 表、等采完。

采集时间差会让价格/库存合理地不同，这在 VOLATILE 里，不影响判定。
但两边采的时间不要差太远（建议 1 小时内），否则慢变字段也可能真的变了。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------- 分类表

#: 本次迁移**有意**改变的字段 -> (D 编号, 期望的差异形态, 判定函数)
#: 判定函数拿 (old, new) 返回 True 表示「这个差异符合预期」。
#: 返回 False 的会被升级成 UNEXPECTED —— 有意变更也可能改错方向。

_NA = {"", "n/a", "none", "null", "-"}


def _is_na(v) -> bool:
    return v is None or str(v).strip().lower() in _NA


def _exp_crawl_time(old, new) -> bool:
    """D-61：裸 UTC+8 'YYYY-MM-DD HH:MM:SS' -> RFC3339 'YYYY-MM-DDTHH:MM:SSZ'。"""
    if _is_na(new):
        return False
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", str(new)))


def _exp_long_description(old, new) -> bool:
    """D-60/B5：不再吸进容器外的文本，所以**新值应当是旧值的子集或更短**。

    旧值里混进了价格/库存/评分/BSR/CDN 图片 URL。新值变长几乎一定是错的。
    """
    if _is_na(old) and _is_na(new):
        return False                      # 都空，本就不该算差异
    o, n = str(old or ""), str(new or "")
    return len(n) <= len(o)


def _exp_sorted_list(old, new) -> bool:
    """D-59：逗号列表现在排序输出，所以**集合相同、顺序不同**。"""
    o = {x.strip() for x in str(old or "").split(",") if x.strip()}
    n = {x.strip() for x in str(new or "").split(",") if x.strip()}
    return o == n and o != set()


def _exp_manufacturer(old, new) -> bool:
    """D-58：不再被 'Manufacturer recommended age' 之类污染。

    典型旧值是年龄段/尺寸这类明显不是厂商名的东西。这里只要求「变了且新值非空」，
    因为「什么算厂商名」没法机械判定 —— 真要确认得人工抽查，报告里会列样本。
    """
    return not _is_na(new)


def _exp_now_present(old, new) -> bool:
    """D-60：四个字段过去在 lxml 路径/早退路径上**缺席**（于是保留上次的旧值），
    现在恒存在。所以差异形态是「旧的是结转值或缺席，新的有值（可能是 N/A）」。"""
    return new is not None


def _exp_zip(old, new) -> bool:
    """D-55：现在恒为**请求的**邮编。旧值可能是页面观测值。"""
    return bool(re.fullmatch(r"\d{5}", str(new or "")))


def _exp_hash_changed(old, new) -> bool:
    """content_hash / title_bullets_hash 的**输入**变了，所以它们必然变。"""
    return not _is_na(new)


INTENTIONAL: Dict[str, Tuple[str, str, Any]] = {
    "crawl_time":          ("D-61", "裸 UTC+8 -> RFC3339 带 Z", _exp_crawl_time),
    "long_description":    ("D-60", "不再吸进容器外的价格/库存/评分文本（应变短）", _exp_long_description),
    "manufacturer":        ("D-58", "精确匹配，不再被 'Manufacturer recommended age' 污染", _exp_manufacturer),
    "upc_list":            ("D-59", "排序输出（集合应相同）", _exp_sorted_list),
    "variation_asins":     ("D-59", "排序输出（集合应相同）", _exp_sorted_list),
    "ean_list":            ("D-59", "排序输出（集合应相同）", _exp_sorted_list),
    "rating":              ("D-60", "不再结转旧值，恒存在", _exp_now_present),
    "review_count":        ("D-60", "不再结转旧值，恒存在", _exp_now_present),
    "seller_id":           ("D-60", "不再结转旧值；Amazon 自营页不再恒 N/A", _exp_now_present),
    "seller_name":         ("D-60", "同上", _exp_now_present),
    "zip_code":            ("D-55", "恒为请求的邮编，不再被页面观测值顶替", _exp_zip),
    "content_hash":        ("—",    "输入字段变了，必然变", _exp_hash_changed),
    "title_bullets_hash":  ("—",    "输入字段变了，必然变", _exp_hash_changed),
}

#: 天然会变的（两次采集本就隔了时间）。差异不计入判定。
VOLATILE = {
    "current_price", "original_price", "buybox_price", "buybox_shipping",
    "is_fba", "stock_count", "stock_status", "delivery_date", "delivery_time",
    "best_sellers_rank", "created_at", "updated_at", "id",
    "baseline_price", "baseline_buybox_price", "baseline_stock_count",
    "baseline_stock_status", "baseline_title_bullets_hash", "baseline_updated_at",
    "screenshot_path",
}

#: 慢变字段：两次采集隔一小时内不该变。**这些出现差异就是 UNEXPECTED**，
#: 也正是 Phase 5 最该盯的。
SLOW_CRITICAL = {
    "title", "brand", "product_type", "model_number", "part_number",
    "country_of_origin", "is_customized", "image_urls", "bullet_points",
    "parent_asin", "variant_attributes",
    "root_category_id", "category_ids", "category_tree",
    "first_available_date", "package_dimensions", "package_weight",
    "item_dimensions", "item_weight", "product_url", "site",
}


# ---------------------------------------------------------------- 取数


def resolve_batch(base: str, spec: Optional[str], timeout: int = 60) -> Optional[int]:
    """把 ``--old-batch/--new-batch`` 的取值解析成 batch_id。

    接受两种写法：纯数字就是 id；否则当**批次名**，去 ``/api/batches`` 查。
    真机上第一次给的就是名字（``batch_20260806_145034``）—— 界面上看得见的
    是名字，不是 id，所以只认 id 是把内部标识泄漏给使用者。
    """
    if spec is None:
        return None
    spec = spec.strip()
    if spec.isdigit():
        return int(spec)
    url = f"{base.rstrip('/')}/api/batches"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        batches = (json.loads(r.read()) or {}).get("batches") or []
    # ⚠ 键名是 ``name``（common/database.py:831 的 `SELECT b.id, b.name, ...`），
    # **不是** ``batch_name``。后者只出现在 /api/upload 与 /api/batches/{name}/status
    # 的响应里。两个都收：真机上写死 batch_name 的版本查出来全是 None。
    def _name_of(b):
        return b.get("name") or b.get("batch_name")

    for b in batches:
        if _name_of(b) is not None and str(_name_of(b)) == spec:
            bid = b.get("id")
            print(f"  批次 {spec!r} -> id {bid}", file=sys.stderr, flush=True)
            return int(bid)
    names = [str(_name_of(b)) for b in batches[:12]]
    raise SystemExit(
        f"❌ {base} 上找不到批次 {spec!r}（共 {len(batches)} 个）。"
        f"最近几个：{names}")


def fetch_results(base: str, limit: int, timeout: int = 60,
                  wanted: Optional[set] = None, max_scan: int = 200000,
                  batch_id: Optional[int] = None) -> Dict[str, dict]:
    """翻页拉 /api/results，返回 {asin: row}。两套系统的响应形状由黄金基线钉死。

    ``wanted`` 给定时**只收这些 ASIN**，并一直翻到全部找齐或翻完为止 ——
    这一条是必须的，不是优化。``/api/results`` 的排序是 ``ORDER BY d.id DESC``，
    而 ``asin_data`` 是**一 ASIN 一行、重采走 UPDATE**：行 id 在**第一次**见到
    这个 ASIN 时分配，之后永不变。

    于是「取前 N 行」= 「最近**首次收录**的 N 个 ASIN」。真机第一次跑比对就栽在
    这儿：旧系统是跑了很久的生产库（11920 行），测试用的 ASIN 早就在里面、id 很小，
    重采只是把行 UPDATE 了一遍，**永远排不进前 600**；新系统是全新库所以全在。
    两边明明采的是同一批，交集却是 0。
    """
    out: Dict[str, dict] = {}
    cursor: Optional[int] = None
    scanned = pages = 0
    while True:
        q = {"limit": 200}
        if batch_id is not None:
            q["batch_id"] = batch_id       # 直接锁定一个批次，页数从几十降到个位数
        if cursor is not None:
            q["cursor"] = cursor
        url = f"{base.rstrip('/')}/api/results?" + urllib.parse.urlencode(q)
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = json.loads(r.read())
        items = body.get("items") or []
        scanned += len(items)
        pages += 1
        for it in items:
            asin = it.get("asin")
            if not asin:
                continue
            if wanted is None or asin in wanted:
                out[asin] = it
        # 跨公网翻 60+ 页、每页服务端还要算一次带 join 的 COUNT —— 没有进度输出的话
        # 分不清「在动」和「卡死」。真机上就是这么卡了一次。
        if pages % 5 == 0 or (wanted and len(out) >= len(wanted)):
            found = f"{len(out)}/{len(wanted)}" if wanted else str(len(out))
            print(f"  … 第 {pages} 页，已扫 {scanned} 行，命中 {found}",
                  file=sys.stderr, flush=True)
        if not items or not body.get("has_more"):
            break
        if wanted is None:
            if len(out) >= limit:
                break
        else:
            if len(out) >= len(wanted):
                break                      # 全找齐了，不用再翻
            if scanned >= max_scan:
                print(f"⚠ 已扫描 {scanned} 行仍未找齐（{len(out)}/{len(wanted)}），"
                      f"提前停止。加大 --max-scan 或缩小 ASIN 列表。", file=sys.stderr)
                break
        cursor = body.get("next_cursor")
        if cursor is None:
            break
    return out


def read_asin_file(path: str) -> set:
    """从文件读 ASIN 列表（每行一个，忽略空行与 # 注释）。"""
    out = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line.split()[0])
    if not out:
        raise SystemExit(f"❌ {path} 里没有任何 ASIN")
    return out


# ---------------------------------------------------------------- 比对

def classify(field: str, old: Any, new: Any) -> Tuple[str, str]:
    """-> (类别, 说明)。类别 ∈ EXPECTED / VOLATILE / UNEXPECTED"""
    if field in VOLATILE:
        return "VOLATILE", "天然快变"
    if field in INTENTIONAL:
        dnum, desc, ok = INTENTIONAL[field]
        if ok(old, new):
            return "EXPECTED", f"{dnum} {desc}"
        return "UNEXPECTED", f"{dnum} 有意变更，但**方向不对**：{desc}"
    if field in SLOW_CRITICAL:
        return "UNEXPECTED", "慢变字段，短时间内不该变"
    return "UNEXPECTED", "未分类字段"


def compare(old_rows: Dict[str, dict], new_rows: Dict[str, dict]) -> dict:
    both = sorted(set(old_rows) & set(new_rows))
    diffs: List[dict] = []
    per_field: Dict[str, Counter] = defaultdict(Counter)
    samples: Dict[str, list] = defaultdict(list)

    for asin in both:
        o, n = old_rows[asin], new_rows[asin]
        for field in sorted(set(o) | set(n)):
            ov, nv = o.get(field), n.get(field)
            if ov == nv:
                # 有意变更却**没变**：单独记，说明修复可能没生效
                if field in INTENTIONAL and not _is_na(ov):
                    per_field[field]["unchanged"] += 1
                continue
            cat, why = classify(field, ov, nv)
            per_field[field][cat] += 1
            if len(samples[f"{field}|{cat}"]) < 3:
                samples[f"{field}|{cat}"].append(
                    {"asin": asin, "old": _trim(ov), "new": _trim(nv), "why": why})
            if cat == "UNEXPECTED":
                diffs.append({"asin": asin, "field": field, "why": why,
                              "old": _trim(ov), "new": _trim(nv)})

    return {
        "old_only": sorted(set(old_rows) - set(new_rows)),
        "new_only": sorted(set(new_rows) - set(old_rows)),
        "compared": len(both),
        "unexpected": diffs,
        "per_field": {k: dict(v) for k, v in sorted(per_field.items())},
        "samples": {k: v for k, v in sorted(samples.items())},
    }


def _trim(v: Any, n: int = 160) -> Any:
    s = str(v)
    return s if len(s) <= n else s[:n] + f"…(共 {len(s)} 字符)"


# ---------------------------------------------------------------- 报告

def report(res: dict) -> int:
    print(f"\n{'='*72}\nPhase 5 并行比对\n{'='*72}")
    print(f"两边都有的 ASIN : {res['compared']}")
    if res["old_only"]:
        print(f"⚠ 只有旧系统有  : {len(res['old_only'])}  {res['old_only'][:5]}")
    if res["new_only"]:
        print(f"⚠ 只有新系统有  : {len(res['new_only'])}  {res['new_only'][:5]}")

    print(f"\n--- 逐字段差异统计 ---")
    print(f"{'字段':<26} {'预期内':>7} {'快变':>7} {'意外':>7} {'该变没变':>9}")
    missing_change = []
    for field, c in res["per_field"].items():
        exp, vol, unexp = c.get("EXPECTED", 0), c.get("VOLATILE", 0), c.get("UNEXPECTED", 0)
        unchanged = c.get("unchanged", 0)
        if field in INTENTIONAL and exp == 0 and unchanged > 0:
            missing_change.append((field, unchanged))
        mark = "  ← 意外" if unexp else ""
        print(f"{field:<26} {exp:>7} {vol:>7} {unexp:>7} {unchanged:>9}{mark}")

    if missing_change:
        print(f"\n⚠ 有意变更**没有出现**（修复可能没生效，或两边跑的是同一版本）：")
        for f, n in missing_change:
            print(f"    {f}: {n} 条完全没变 —— 期望 {INTENTIONAL[f][0]} {INTENTIONAL[f][1]}")

    if res["unexpected"]:
        print(f"\n{'='*72}\n❌ UNEXPECTED 差异 {len(res['unexpected'])} 条（前 20）\n{'='*72}")
        for d in res["unexpected"][:20]:
            print(f"\n  [{d['asin']}] {d['field']}  —— {d['why']}")
            print(f"    旧: {d['old']}")
            print(f"    新: {d['new']}")
    else:
        print(f"\n✅ 没有 UNEXPECTED 差异")

    print(f"\n--- 抽样（供人工核对「有意变更是否改对了方向」）---")
    for key in ("manufacturer|EXPECTED", "long_description|EXPECTED",
                "seller_name|EXPECTED", "upc_list|EXPECTED"):
        for s in res["samples"].get(key, [])[:2]:
            print(f"  [{s['asin']}] {key.split('|')[0]}")
            print(f"    旧: {s['old']}")
            print(f"    新: {s['new']}")

    ok = not res["unexpected"] and not res["new_only"] and not res["old_only"]
    print(f"\n{'='*72}")
    print("✅ 通过" if ok else "❌ 不通过 —— 上面每一条 UNEXPECTED 都要解释清楚才能切换")
    print(f"{'='*72}\n")
    return 0 if ok else 1



def _dump_asins(a) -> int:
    """导出 ASIN 列表，供两侧各上传一次。

    为什么需要这一步：比对的前提是**两边采过同一批 ASIN**。第一次跑的时候
    旧系统 600 个、新系统 44 个、交集 **0** —— 工具正确地报了不通过，但也
    没比出任何东西。两边各采各的，比对就是空转。
    """
    asins = set()
    if a.dump_from in ("old", "both"):
        print(f"拉取旧系统 {a.old} …", file=sys.stderr)
        asins |= set(fetch_results(a.old, a.limit))   # 无 wanted：取前 N 行
    if a.dump_from in ("new", "both"):
        print(f"拉取新系统 {a.new} …", file=sys.stderr)
        asins |= set(fetch_results(a.new, a.limit))

    ordered = sorted(asins)[:a.limit]
    with open(a.dump_asins, "w", encoding="utf-8") as f:
        f.write("\n".join(ordered) + "\n")
    print(f"已写出 {len(ordered)} 个 ASIN -> {a.dump_asins}\n")
    print("接下来：把这个文件**同时**传给两套系统，各采一遍，然后再跑比对。")
    print("⚠ 两边开采的时间别差太远（建议 1 小时内）——差太远的话慢变字段可能")
    print("  真的变了，那是商品变了不是系统错了，会造成假阳性。")
    return 0

def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 5 新旧系统内容比对")
    ap.add_argument("--old", required=True, help="旧 SQLite 系统 base URL")
    ap.add_argument("--new", required=True, help="新 PostgreSQL 系统 base URL")
    ap.add_argument("--limit", type=int, default=500, help="最多比对多少个 ASIN")
    ap.add_argument("--json", help="把完整结果写到这个文件")
    ap.add_argument("--dump-asins", metavar="FILE",
                    help="不比对，只把 ASIN 列表导出到文件（配合 --dump-from）。"
                         "两套系统必须采**同一份列表**才有得比")
    ap.add_argument("--asins", metavar="FILE",
                    help="只比对这个文件里的 ASIN（每行一个）。**强烈建议给**，"
                         "理由见 fetch_results 的 docstring")
    ap.add_argument("--old-batch", metavar="ID_或_名字",
                    help="只看旧系统这一个批次。可以给数字 id，也可以直接给批次名"
                         "（工具去 /api/batches 查）。生产库上强烈建议给 —— "
                         "否则要翻遍全表去找那几十个 ASIN")
    ap.add_argument("--new-batch", metavar="ID_或_名字", help="同上，新系统侧")
    ap.add_argument("--timeout", type=int, default=60,
                    help="单次 HTTP 超时秒数")
    ap.add_argument("--max-scan", type=int, default=200000,
                    help="配合 --asins：每侧最多翻多少行去找（默认 20 万）")
    ap.add_argument("--dump-from", choices=("old", "new", "both"), default="old",
                    help="从哪一侧取 ASIN。默认 old —— 旧系统是生产库，"
                         "它的 ASIN 才是真实语料；both 取并集")
    a = ap.parse_args()

    if a.dump_asins:
        return _dump_asins(a)

    wanted = read_asin_file(a.asins) if a.asins else None
    if wanted:
        print(f"限定在 {len(wanted)} 个指定 ASIN 上比对", file=sys.stderr)
    else:
        print("⚠ 没给 --asins：按「/api/results 的前 N 行」取，而那是"
              "「最近首次收录的 N 个 ASIN」，不是「最近采集的」。"
              "生产库上这样几乎必然交集为空 —— 强烈建议用 --asins。", file=sys.stderr)

    old_bid = resolve_batch(a.old, a.old_batch, a.timeout)
    new_bid = resolve_batch(a.new, a.new_batch, a.timeout)

    print(f"拉取旧系统 {a.old} …", file=sys.stderr, flush=True)
    old = fetch_results(a.old, a.limit, wanted=wanted, max_scan=a.max_scan,
                        timeout=a.timeout, batch_id=old_bid)
    print(f"  {len(old)} 行", file=sys.stderr)
    print(f"拉取新系统 {a.new} …", file=sys.stderr)
    new = fetch_results(a.new, a.limit, wanted=wanted, max_scan=a.max_scan,
                        timeout=a.timeout, batch_id=new_bid)
    print(f"  {len(new)} 行", file=sys.stderr)

    if not old or not new:
        print("❌ 有一边是空的，先确认两套系统都采过同一批 ASIN", file=sys.stderr)
        return 2

    res = compare(old, new)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        print(f"完整结果 -> {a.json}", file=sys.stderr)
    return report(res)


if __name__ == "__main__":
    raise SystemExit(main())
