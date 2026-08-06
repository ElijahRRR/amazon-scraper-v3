#!/usr/bin/env python3
"""量化 `<br>` 修复的真机效果：同一个 ASIN 重采前后，long_description 的对比。

    python tools/desc_glue_check.py                    # 概览（按采集时刻分组）
    python tools/desc_glue_check.py --compare          # 前后配对，精确判据

用途：`<br>` 修复的真机前后对比。Amazon 的 A+ 描述用 `<br>` 换行，而两个解析引擎
原本取叶子文本时把子树里的文字首尾相接，`<br>` 自身不含文字，于是它两侧的词被
粘成一个：`fight corrosion.<br>The durable` -> `fight corrosion.The durable`。

危害不止可读性：`long_description ∈ SLOW_HASH_FIELDS`，而算哈希前会**折叠空白** ——
`"a  b"` 和 `"a b"` 折叠后相同，但粘住的 `"ab"` 折不开。等于把两个词永久变成一个
不存在的新词，喂进哈希、也喂给消费侧。

判据有两个，**优先看第二个**：

1. 启发式（默认视图）：正则数「句末标点后紧跟大写」和「小写串里冒出大写」。
   它只抓得到**大小写边界**那一类 —— `corrosion.The`、`PullDrawer` 能抓，
   `alloythat`、`pushand` 抓不到（全小写粘连，不查词典分不出来）。所以它只能
   给个趋势，不能当验收标准。

2. **精确判据（`--compare`）**：这次修复唯一做的事就是在 `<br>` 处插入 `\n`，
   别的一个字符没动。所以对同一个 ASIN：

       后.replace("\n", "")  ==  前.replace("\n", "")        必须成立
       后.count("\n") - 前.count("\n")                        = 修好的粘连处数

   不需要启发式，也不会漏全小写那一类。等式不成立说明**描述本身变了**
   （A+ 内容更新、对比表里的价格评分变动），那是商品变了不是修复错了 ——
   工具会把这类单独列出来，不混进结论。
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import urllib.error
import urllib.request

# 两类粘连：句末标点后紧跟大写（corrosion.The），以及小写串中间冒出大写（alloythat
# 这类靠它抓不到，但 PullDrawer 能）。都要求两侧各有若干字母，压掉缩写噪声。
GLUE = re.compile(r'[a-z]{2,}[.!?][A-Z][a-z]{2,}|[a-z]{3,}[A-Z][a-z]{3,}')


def fetch_all(base: str, token: str, cap_pages: int = 50):
    cur, rows, pages = 0, [], 0
    while pages < cap_pages:
        req = urllib.request.Request(
            f"{base}/api/export/incremental?cursor={cur}&limit=1000",
            headers={"X-Export-Token": token} if token else {})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = json.load(r)
        except urllib.error.HTTPError as e:
            raise SystemExit(f"❌ HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}")
        except urllib.error.URLError as e:
            raise SystemExit(f"❌ 连不上 {base}：{e.reason}")
        if "records" not in body:
            raise SystemExit(f"❌ 服务端返回的不是数据页：{body}")
        rows += body["records"]
        pages += 1
        if not body.get("has_more"):
            return rows, pages
        cur = body["next_cursor"]
    print(f"⚠️  已拉满 {cap_pages} 页仍未到底，统计基于前 {len(rows)} 条")
    return rows, pages


def main() -> int:
    ap = argparse.ArgumentParser(description="long_description 词粘连统计")
    ap.add_argument("--base-url", default="http://127.0.0.1:8899")
    ap.add_argument("--token", default=os.environ.get("EXPORT_TOKEN", ""))
    ap.add_argument("--group", default=16, type=int,
                    help="按 scraped_at 前 N 个字符分组，默认 16（到分钟）")
    ap.add_argument("--top", default=5, type=int)
    ap.add_argument("--compare", action="store_true",
                    help="把每个 ASIN 最早与最新的两条配对，用精确判据比较")
    a = ap.parse_args()

    rows, pages = fetch_all(a.base_url.rstrip("/"), a.token)
    print(f"共 {len(rows)} 条记录（{pages} 页）\n")

    by_run = collections.defaultdict(lambda: [0, 0, 0])   # [记录数, 有描述数, 粘连总数]
    worst = []
    for r in rows:
        d = (r.get("slow") or {}).get("description") or ""
        run = (r.get("scraped_at") or "?")[:a.group]
        by_run[run][0] += 1
        if not d:
            continue
        by_run[run][1] += 1
        hits = GLUE.findall(d)
        by_run[run][2] += len(hits)
        if hits:
            worst.append((len(hits), r.get("asin"), r.get("scraped_at"), hits[:3]))

    print(f"{'采集时刻':<20}{'记录':>6}{'有描述':>8}{'粘连':>7}{'每条均值':>10}")
    for run in sorted(by_run):
        n, withd, glue = by_run[run]
        print(f"{run:<20}{n:>6}{withd:>8}{glue:>7}{(glue / withd if withd else 0):>10.2f}")

    if worst:
        print(f"\n粘连最多的 {a.top} 条：")
        for h, asin, ts, ex in sorted(worst, key=lambda x: -x[0])[:a.top]:
            print(f"  {asin}  {ts}  {h:>3} 处  {ex}")
    else:
        print("\n没有命中任何粘连。")

    if a.compare:
        _compare(rows)
    else:
        print("\n上面是启发式趋势，只抓得到大小写边界那一类粘连。"
              "\n重采一轮之后加 --compare，用精确判据看结果。")
    return 0


def _compare(rows) -> None:
    """把每个 ASIN 最早/最新的两条配对，用「去掉换行后必须逐字节相等」做判据。"""
    first, last = {}, {}
    for r in rows:
        d = (r.get("slow") or {}).get("description") or ""
        if not d:
            continue
        key, ts = r.get("asin"), r.get("scraped_at") or ""
        if key not in first or ts < first[key][0]:
            first[key] = (ts, d)
        if key not in last or ts > last[key][0]:
            last[key] = (ts, d)

    paired = [(k, first[k], last[k]) for k in first
              if k in last and first[k][0] != last[k][0]]
    if not paired:
        print("\n--compare：没有任何 ASIN 有两轮以上的采集，无法配对。"
              "\n先重采一遍同一批 ASIN 再跑。")
        return

    fixed, unchanged, changed = [], 0, []
    for asin, (ts0, d0), (ts1, d1) in paired:
        if d0.replace("\n", "") != d1.replace("\n", ""):
            changed.append((asin, ts0, ts1, len(d0), len(d1)))
            continue
        delta = d1.count("\n") - d0.count("\n")
        if delta:
            fixed.append((delta, asin, ts0, ts1))
        else:
            unchanged += 1

    print(f"\n--compare：{len(paired)} 个 ASIN 有前后两轮")
    print(f"  ✅ 描述正文一致、新增换行  : {len(fixed)} 个"
          f"（共修好 {sum(d for d, *_ in fixed)} 处粘连）")
    print(f"  ·  描述正文一致、换行数不变: {unchanged} 个（这些页面本来就没有 <br>）")
    print(f"  ⚠ 描述正文本身变了        : {len(changed)} 个"
          f"（A+ 内容或对比表变动，与本次修复无关）")

    for delta, asin, ts0, ts1 in sorted(fixed, reverse=True)[:10]:
        print(f"     {asin}  {ts0} -> {ts1}  +{delta} 处")
    for asin, ts0, ts1, n0, n1 in changed[:5]:
        print(f"     ⚠ {asin}  {ts0}({n0}) -> {ts1}({n1})")


if __name__ == "__main__":
    raise SystemExit(main())
