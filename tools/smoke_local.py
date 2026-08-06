#!/usr/bin/env python3
"""本机冒烟：把一条完整链路跑通并**验契约不变量**，不需要 worker、不需要代理。

    python tools/smoke_local.py                       # 默认 http://127.0.0.1:8899
    python tools/smoke_local.py --base-url http://... --token "$EXPORT_TOKEN"
    python tools/smoke_local.py --keep                # 不删测试批次，留着人工翻

上传 → 拉任务 → 提交结果（成功 + 失败各一条）→ 查结果 → 等 relay 抽干 →
增量导出，逐条断言。手工 curl 也能做完这些，但要来回拷 task_id/batch_id/
lease_epoch，而**契约里最容易坏的四条恰恰是手工最难验的**：

  * 空页必须 200 且 next_cursor 不推进 —— 坏成 404 的话消费方会读成「暂无数据」，
    游标永不推进、同步静默停摆，两侧都不报错（见 export_incremental.py 承重约束 1）
  * 同一个 cursor 重复拉必须完全一致 —— 契约允许重复投递，靠 source_id 兜底去重
  * source_id 全局唯一、cursor 严格升序 —— 消费侧的幂等和断点续传都压在这上面
  * 失败/降级的采集也必须进流 —— 「商品没了」正是要同步出去的信息，不是噪声

只读的检查在前，写入在后；退出码 0 全过、1 有失败。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

_fails: list = []
_warns: list = []


def ok(msg):    print(f"✅ {msg}")
def warn(msg):  _warns.append(msg); print(f"⚠️  {msg}")
def bad(msg):   _fails.append(msg); print(f"❌ {msg}")


def check(cond, msg):
    (ok if cond else bad)(msg)
    return cond


# ---------------------------------------------------------------- HTTP

class Http:
    def __init__(self, base: str, token: str = ""):
        self.base = base.rstrip("/")
        self.token = token

    def _req(self, method, path, *, body=None, ctype=None, headers=None):
        url = self.base + path
        data = None
        h = dict(headers or {})
        if body is not None:
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            h["Content-Type"] = ctype or "application/json"
        if self.token:
            h["X-Export-Token"] = self.token
        req = urllib.request.Request(url, data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                status = r.status
        except urllib.error.HTTPError as e:
            raw, status = e.read(), e.code
        except urllib.error.URLError as e:
            raise SystemExit(f"❌ 连不上 {url}：{e.reason}\n"
                             f"   服务起了吗？ DB_BACKEND=postgres python run_server.py")
        try:
            return status, json.loads(raw or b"null")
        except json.JSONDecodeError:
            return status, raw.decode("utf-8", "replace")

    def get(self, path, **kw):  return self._req("GET", path, **kw)
    def post(self, path, **kw): return self._req("POST", path, **kw)

    def upload(self, path, fields: dict, filename: str, content: bytes):
        """multipart/form-data，只用标准库拼。"""
        b = f"----smoke{uuid.uuid4().hex}"
        parts = []
        for k, v in fields.items():
            parts.append(f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n"
                         .encode())
        parts.append(
            f"--{b}\r\nContent-Disposition: form-data; name=\"file\"; "
            f"filename=\"{filename}\"\r\nContent-Type: text/plain\r\n\r\n".encode()
            + content + b"\r\n")
        parts.append(f"--{b}--\r\n".encode())
        return self._req("POST", path, body=b"".join(parts),
                         ctype=f"multipart/form-data; boundary={b}")


# ---------------------------------------------------------------- 各阶段

def stage_health(http: Http, backend_is_pg: bool):
    st, body = http.get("/api/diagnostic")
    check(st == 200, f"/api/diagnostic -> {st}")

    st, ev = http.get("/api/_debug/event-stream")
    if not backend_is_pg:
        warn(f"/api/_debug/event-stream -> {st}（SQLite 后端没有事件流，跳过增量导出）")
        return False
    if not check(st == 200, f"/api/_debug/event-stream -> {st}"):
        return False
    state = (ev or {}).get("relay_state")
    check(state == "running", f"relay_state = {state!r}（应为 'running'）")
    if isinstance(ev, dict) and ev.get("instance_id") in (None, "unconfigured"):
        warn("instance_id 是 'unconfigured' —— 配 SCRAPER_INSTANCE_ID，"
             "否则两个克隆部署的事件流无法区分")
    return True


def stage_submit(http: Http, batch_name: str, zip_code: str):
    """上传 3 个 ASIN，成功 2 条、失败 1 条。返回 batch_id。"""
    asins = ["B0SMOKE001", "B0SMOKE002", "B0SMOKE003", "B0SMOKE004"]
    st, body = http.upload("/api/upload",
                           {"batch_name": batch_name, "zip_code": zip_code},
                           "asins.txt", ("\n".join(asins) + "\n").encode())
    if not check(st == 200 and isinstance(body, dict) and body.get("ok") is not False,
                 f"POST /api/upload -> {st} {json.dumps(body, ensure_ascii=False)[:120]}"):
        return None
    batch_id = (body or {}).get("batch_id")

    # 只认本批次的任务：库里可能还有上一次冒烟/真实批次留下的 pending 任务，
    # 而 pull 是按队列给的，不区分批次。多拉几轮直到本批次的三条都到手，
    # 别人的任务立刻 release 回去 —— 冒烟不该把真实队列吃掉。
    mine, foreign, rounds = [], [], 0
    while len(mine) < len(asins) and rounds < 5:
        st, body = http.get("/api/tasks/pull?worker_id=w-smoke&count=20&enable_screenshot=false")
        if st != 200:
            bad(f"GET /api/tasks/pull -> {st}")
            return None
        got = (body or {}).get("tasks") or []
        if not got:
            break
        for t in got:
            (mine if t.get("batch_id") == batch_id else foreign).append(t)
        rounds += 1
    if foreign:
        http.post("/api/tasks/release",
                  body={"worker_id": "w-smoke",
                        "tasks": [{"task_id": t["id"],
                                   "lease_epoch": t.get("lease_epoch", 0)} for t in foreign]})
        warn(f"队列里有 {len(foreign)} 个别的批次的任务，已原样释放回去")
    tasks = mine
    if not check(len(tasks) == len(asins),
                 f"拿到本批次 {len(tasks)} 个任务（应 {len(asins)}）"):
        return None

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    accepted = 0

    for i, t in enumerate(tasks):
        common = {"task_id": t["id"], "batch_id": t.get("batch_id"),
                  "worker_id": "w-smoke", "lease_epoch": t.get("lease_epoch", 0)}
        if i < 2:
            payload = dict(common, success=True, asin=t["asin"],
                           title=f"Smoke Widget {i}", brand="SmokeBrand",
                           category_tree="Home > Tools > Wrenches",
                           image_urls="https://m.media-amazon.com/images/I/A1smoke.jpg",
                           # parser 用换行拼接 bullet_points，这里必须照抄真实形态。
                           # 曾经写成 "第一点|第二点" —— 夹具自己编了个分隔符，
                           # 于是导出成一个元素还全绿，正是它掩盖了真实的分隔符 bug。
                           bullet_points="第一点\n第二点",
                           long_description="一段正常的长描述。",
                           current_price="19.99", stock_status="In Stock",
                           stock_count="7", rating="4.5", review_count="128",
                           seller_name="Amazon.com", crawl_time=now,
                           site="US", zip_code=t.get("zip_code") or "10001")
        elif i == 2:
            # 「商品没了」= 404 页。这**不是**采集失败：worker 成功拿到了一个页面，
            # 所以走 success=True + _outcome 标记；失败路径上根本没有 not_found
            # 这个 error_type（worker/engine.py 的 last_error_type 值域里没有它）。
            #
            # 这个提交体照抄 worker 的 _not_found_body()（engine.py:1256）：
            #   * 慢变/目录字段**一个都不提交** —— 服务端写入是逐字段
            #     `if val is not None`，不提交才等于"保住上一次的值"，提交占位符
            #     就是把 products 里的标题/类目/图片永久抹成 N/A
            #   * 快变字段照旧给占位值，而 stock_count="0" 是**承重的**：
            #     _is_parse_failure 要求 price/buybox/stock_count/stock_status/brand
            #     全部落在 _NA_VALUES 里才判解析失败，而它跑在 classify_success_outcome
            #     之前。少了这个 "0"，一次真 404 会被降级成 server_reject，
            #     事件 outcome 变成 parse_failed 而不是 not_found —— 消费侧再也
            #     分不出"商品下架"和"解析坏了"。
            payload = dict(common, success=True, asin=t["asin"],
                           current_price="N/A", buybox_price="N/A",
                           stock_status="N/A", stock_count="0",
                           _outcome="not_found", _completeness=0,
                           crawl_time=now, site="US",
                           zip_code=t.get("zip_code") or "10001")
        else:
            payload = dict(common, success=False, asin=t["asin"],
                           error_type="network",
                           error_detail="smoke: 故意造一条失败，验证终态失败进事件流")
        st, r = http.post("/api/tasks/result", body=payload)
        if st == 200 and (r or {}).get("ok"):
            accepted += 1
        else:
            bad(f"提交第 {i} 条 -> {st} {r}")

    check(accepted == len(tasks), f"{accepted}/{len(tasks)} 条结果被接受")

    # 把那条失败任务打到**终态**。事件只在 retry_count >= cap（默认 MAX_RETRIES=3）
    # 时发：中途失败会回 pending 等重试，此时发事件等于同一个 ASIN 在流里刷三条
    # 噪声。「产品没了」这个结论要等重试用尽才成立，所以这里得真的把它耗干，
    # 否则这条断言验的是空气。
    doomed = tasks[-1]["asin"]
    for _ in range(6):
        st, body = http.get("/api/tasks/pull?worker_id=w-smoke&count=20&enable_screenshot=false")
        again = [t for t in ((body or {}).get("tasks") or [])
                 if t.get("batch_id") == batch_id and t.get("asin") == doomed]
        others = [t for t in ((body or {}).get("tasks") or [])
                  if t.get("batch_id") != batch_id]
        if others:
            http.post("/api/tasks/release",
                      body={"worker_id": "w-smoke",
                            "tasks": [{"task_id": t["id"],
                                       "lease_epoch": t.get("lease_epoch", 0)} for t in others]})
        if not again:
            break                      # 不再回到队列 = 已经终态
        t = again[0]
        http.post("/api/tasks/result",
                  body={"task_id": t["id"], "batch_id": batch_id, "worker_id": "w-smoke",
                        "lease_epoch": t.get("lease_epoch", 0), "success": False,
                        "asin": doomed, "error_type": "not_found",
                        "error_detail": "smoke: 耗尽重试，逼到终态"})

    st, body = http.get(f"/api/batches/{urllib.parse.quote(batch_name)}/status")
    stats = (body or {}).get("stats") or {}
    check(st == 200 and stats.get("failed") == 1,
          f"批次状态 stats={stats}（failed 应为 1，即那条耗尽重试的任务已终态）")

    st, body = http.get("/api/results?limit=10")
    rows = (body or {}).get("items") or []
    check(st == 200 and len(rows) >= 2,
          f"GET /api/results -> {st}，{len(rows)} 行（成功的 2 条应当在）")
    return batch_id


def _pull_all(http: Http, limit=500, cap=50):
    """从 0 分页拉完，返回 (records, pages, last_next_cursor)。"""
    cur, seen, pages = 0, [], 0
    while pages < cap:
        st, body = http.get(f"/api/export/incremental?cursor={cur}&limit={limit}")
        if st != 200:
            bad(f"分页拉取在 cursor={cur} 得到 {st}：{body}")
            return seen, pages, cur
        seen.extend(body.get("records") or [])
        pages += 1
        nxt = body.get("next_cursor")
        if not body.get("has_more"):
            return seen, pages, nxt
        if nxt == cur:
            bad(f"has_more=true 但 next_cursor 没推进（卡在 {cur}）—— 会死循环")
            return seen, pages, nxt
        cur = nxt
    bad(f"分页超过 {cap} 页仍未结束")
    return seen, pages, cur


_ISO_Z = __import__("re").compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def stage_export(http: Http):
    st, body = http.get("/api/export/incremental?cursor=0&limit=10")
    if st == 401:
        bad("401 —— 服务端配了 EXPORT_TOKEN，但脚本没拿到。加 --token \"$EXPORT_TOKEN\"")
        return
    if st == 503:
        bad(f"503 —— {body}。DB_BACKEND 不是 postgres，或事件流表没建")
        return
    if not check(st == 200, f"GET /api/export/incremental -> {st}"):
        return

    records, pages, tail = _pull_all(http)
    if not check(records, f"拉到 {len(records)} 条记录（{pages} 页）"):
        return

    sids = [r.get("source_id") for r in records]
    check(all(sids), "每条都有 source_id")
    check(len(set(sids)) == len(sids),
          f"source_id 全局唯一（{len(sids)} 条 / {len(set(sids))} 个不同值）")

    curs = [r.get("cursor") for r in records]
    check(all(isinstance(c, int) and c > 0 for c in curs), "cursor 都是正整数")
    check(curs == sorted(curs) and len(set(curs)) == len(curs), "cursor 严格升序且无重复")

    r0 = records[0]
    check(_ISO_Z.match(str(r0.get("scraped_at"))),
          f"scraped_at 形如 2026-08-06T10:00:00Z（实际 {r0.get('scraped_at')!r}）")
    check(r0.get("marketplace") == "US", f"marketplace = {r0.get('marketplace')!r}")
    check(isinstance(r0.get("slow"), dict) and isinstance(r0.get("fast"), dict),
          "slow / fast 两个对象都在")
    check(isinstance(r0.get("scrape_params"), dict)
          and "zipcode" in r0["scrape_params"],
          "scrape_params.zipcode 在（契约键名是 zipcode，不是 zip_code）")

    # 多值字段必须切开。整块塞进单个元素是这条链路真实发生过的坏法：
    # 导出适配器的分隔符和 parser 的拼接方式对不上，六条图片 URL 变成一个元素。
    multi = next((r for r in records
                  if len(r.get("slow", {}).get("bullet_points") or []) or
                     len(r.get("slow", {}).get("images") or [])), None)
    if multi:
        for f in ("images", "bullet_points"):
            vals = multi["slow"].get(f) or []
            check(not any("\n" in v for v in vals),
                  f"slow.{f} 的元素里不含换行（含了就是整块没切开）")

    outcomes = sorted({r.get("outcome") for r in records if r.get("outcome")})
    check(any(o != "ok" for o in outcomes), f"非 ok 的采集也进了流（outcome 取值：{outcomes}）")

    # 「商品没了」必须自成一档。归进 parse_failed 的话，消费侧分不出
    # 「页面解析坏了（该重采）」和「商品下架了（该下架）」—— 而后者正是
    # 事件流存在的理由之一。
    check("not_found" in outcomes,
          f"404/下架单独标成 outcome='not_found'（实际取值：{outcomes}）")

    # 专挑 not_found 那条看：404 提交体一个慢变字段都不该带。带了的话消费侧会把
    # 它 upsert 进 catalog.products，一次假 404 就永久损坏一条目录记录。
    for r in records:
        if r.get("outcome") == "not_found":
            slow = r.get("slow") or {}
            carried = [k for k in ("title", "brand", "category_path", "images",
                                   "bullet_points", "description")
                       if slow.get(k)]
            check(not carried,
                  f"not_found 记录 {r.get('asin')} 没带慢变字段"
                  + (f"（却带了 {carried}）" if carried else ""))
            break

    # 空页：必须 200 且游标不推进。坏成 404 会被消费方读成「暂无数据」而静默停摆。
    st, body = http.get(f"/api/export/incremental?cursor={tail}&limit=10")
    check(st == 200, f"拉到底之后再拉一次 -> {st}（必须 200，绝不能是 404）")
    if st == 200:
        check(body.get("records") == [], "空页 records 为 []")
        check(body.get("has_more") is False, "空页 has_more=false")
        check(body.get("next_cursor") == tail,
              f"空页 next_cursor 不推进（{body.get('next_cursor')} 应等于 {tail}）")

    # 重复拉同一个 cursor 必须完全一致 —— 契约允许重复投递，靠 source_id 兜底
    st1, b1 = http.get("/api/export/incremental?cursor=0&limit=10")
    st2, b2 = http.get("/api/export/incremental?cursor=0&limit=10")
    check(st1 == st2 == 200 and b1 == b2, "同一个 cursor 重复拉，结果逐字节一致")

    st, counts = http.get(f"/api/v1/sync/counts?from_seq=0&to_seq={tail + 1}")
    if st == 200 and isinstance(counts, dict):
        n = counts.get("count")
        check(n == len(records),
              f"/api/v1/sync/counts 对账：{n} 条 vs 导出的 {len(records)} 条")
    else:
        warn(f"/api/v1/sync/counts -> {st}，跳过对账")


# ---------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser(description="本机功能冒烟")
    ap.add_argument("--base-url", default="http://127.0.0.1:8899")
    ap.add_argument("--token", default="", help="X-Export-Token；服务端配了就必须给")
    ap.add_argument("--zip", default="10001")
    ap.add_argument("--keep", action="store_true", help="跑完不删测试批次")
    a = ap.parse_args()

    import os
    if not a.token:
        a.token = os.environ.get("EXPORT_TOKEN", "")
    backend_is_pg = os.environ.get("DB_BACKEND", "").strip().lower() == "postgres"

    http = Http(a.base_url, a.token)
    batch_name = f"smoke_{uuid.uuid4().hex[:8]}"

    print(f"\n{'='*70}\n本机冒烟 —— {a.base_url}  批次 {batch_name}\n{'='*70}")

    print("\n--- 1/3 健康检查 ---")
    has_stream = stage_health(http, backend_is_pg)

    print("\n--- 2/3 采集链路（上传 → 拉任务 → 提交 → 查结果）---")
    stage_submit(http, batch_name, a.zip)

    print("\n--- 3/3 增量导出契约 ---")
    if has_stream:
        # relay 每秒抽一次 outbox，给它一点时间
        time.sleep(2.0)
        stage_export(http)
    else:
        warn("跳过（需要 DB_BACKEND=postgres）")

    if not a.keep:
        http._req("DELETE", f"/api/batches/{urllib.parse.quote(batch_name)}")

    print(f"\n{'='*70}")
    if _fails:
        print(f"❌ {len(_fails)} 项失败，{len(_warns)} 项警告：")
        for f in _fails:
            print(f"   - {f}")
        return 1
    if _warns:
        print(f"⚠️  全部通过，但有 {len(_warns)} 项警告")
        return 0
    print("✅ 全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
