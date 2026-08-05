# 增量导出契约 v1 —— 采集侧存档副本

> **权威原本在沃尔玛侧 `docs/scraper_migration_brief.md` 第五节。**
> 本文是采集侧按约定各存一份的副本 + 实现说明。
> **改动需两侧同步升 `contract_version`。**
>
> 实现：`server/api/export_incremental.py`
> 用例：`tests/test_incremental_export.py`（每个用例对应契约里的一句话）

- `contract_version`: **1**
- 端点：`GET /api/export/incremental?cursor=<int>&limit=<≤1000>`
- 鉴权：请求头 `X-Export-Token`
- 返回：`{"records": [...], "next_cursor": <int>, "has_more": <bool>}`，按 `cursor` 升序

---

## ⚠ 本副本是照着要点写的，有 7 处我替你做了决定

沃尔玛侧给出的是要点摘要，`docs/scraper_migration_brief.md` §5 的完整文本采集侧没有。
下面每一条都是**实现时不得不定、但摘要里没写死**的地方。**请逐条对照 §5 核对**，
不一致的地方以 §5 为准，我改实现。

| # | 我定的 | 依据 / 风险 |
|---|---|---|
| 1 | `fast.currency` 恒为 `"USD"` | **采集侧根本不采币种**，这是适配器凭空补的常量。amazon.com 恒美元所以今天不出错，但它不是观测到的事实。若 §5 期望它反映真实币种，需要先在采集侧加抓取 |
| 2 | `fast.price` 是 **number** 或 `null` | 采集侧原始值是字符串（`"19.99"` / `"N/A"`）。取不到时返回 `null` 而**不是 0** —— 0 是合法价格，拿它当哨兵会让「没采到」被读成「免费」 |
| 3 | `fast.stock_state` 取 `in_stock` / `out_of_stock` / `unknown` | 采集侧只有自由文本 `stock_status`。三值枚举是我定的；若 §5 另有值域（例如带 `preorder`/`limited`）请给我值域 |
| 4 | `slow.category_path` 是**字符串数组** | 采集侧存的是 `"Home > Tools > Wrenches"`，按 `>` 切分。`[]` 表示**本次没采到**（软降级页会把面包屑整块剥掉），**不表示该商品无类目**——别拿 `[]` 覆盖你侧已有的类目 |
| 5 | `slow.images` 是 **URL 字符串数组** | 采集侧存的是逗号连接的字符串。注意是完整 URL，不是图片 ID |
| 6 | 游标掉出保留窗口时返回 **409** `cursor_below_retention` | **§5 没定义这一种。** 但静默跳过被裁区间是不可接受的（两侧都不会察觉丢数据），所以我按 409 实现。**这条需要进 v1.1**，你侧要实现「收到 409 ⇒ 告警 + 全量对账 + 停」 |
| 7 | 所有 `outcome` 的记录都进流，不只 `ok` | 依据是你们此前明确的「失败/降级采集要进流，否则自己也不知道产品没了」。record 里带 `outcome` 字段。**`outcome != "ok"` 的记录只进 snapshots，绝不 upsert products** |

另外两条是**语义提醒**，不是待定项：

- **`marketplace` 是「上架目的地」，不是「采集来源站点」。** 契约里它恒为 `"US"`，
  与你们 `(marketplace, asin)` 复合主键、默认 `'US'` 对齐。但采集侧内部还有一个
  同名概念指**从哪个亚马逊站点采的**（当前恒 `amazon.com`）。今天两者一一对应；
  等你们开 Walmart CA，很可能**仍然从 amazon.com 采、却要上架到 CA**，那时两者就分叉了。
  所以我没有把内部值改名，而是显式映射，并把来源站点原样放进
  `scrape_params.source_marketplace` —— **两个概念从第一天就是两个字段。**
- **`cursor` 是 `bigserial` 主键，结构上不可能重复。** 所以验收项「cursor 相同多条不丢」
  在本实现下是**平凡成立**的。我把这个事实写成了用例
  （`test_cursor_values_are_unique_so_the_same_cursor_case_is_vacuous`），
  哪天有人把 cursor 换成时间戳之类可重复的东西，它会立刻红。

---

## 1. 请求

```
GET /api/export/incremental?cursor=0&limit=1000
X-Export-Token: <token>
```

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `cursor` | int ≥ 0 | 0 | **独占**下界，返回 `cursor` **大于**它的记录。从头拉传 `0` |
| `limit` | int 1..1000 | 200 | 超过 1000 返回 422 |

**鉴权**：`X-Export-Token` 与服务端 `EXPORT_TOKEN` 环境变量比对（`hmac.compare_digest`）。

- 不匹配或缺失 → **401**
- 服务端**没配** `EXPORT_TOKEN` → **503**，不是放行。
  服务器是公网 IP，「没配就放行」等于把商品库敞在互联网上。**fail closed 是有意的。**

## 2. 响应

```jsonc
{
  "contract_version": 1,
  "records": [ { … } ],
  "next_cursor": 41208500,
  "has_more": true
}
```

### record

```jsonc
{
  "source_id": "a3f19c2b7e04:evt-41208001",   // 幂等键，全局唯一
  "cursor": 41208001,                          // 单调不回跳
  "marketplace": "US",                         // 上架目的地，当前恒 US
  "asin": "B0CXXXXXXX",
  "scraped_at": "2026-08-05T09:11:02Z",        // UTC ISO8601，恒带 Z

  "scrape_params": {
    "zip": "10001",                            // 请求的邮编（分组键的一部分）
    "zip_observed": "10001",                   // 页面上实际反映的邮编，可为 null
    "zip_verify": "confirmed",                 // confirmed|assumed|mismatch|unverified
    "source_marketplace": "amazon.com",        // 采集来源站点，见上文语义提醒
    "parse_engine": "selectolax"
  },

  "slow": {
    "title": "…",
    "brand": "…",
    "category_path": ["Home", "Tools", "Wrenches"],
    "images": ["https://m.media-amazon.com/images/I/71ABC._AC_SL1500_.jpg"]
  },

  "fast": {
    "price": 19.99,
    "currency": "USD",
    "stock_state": "in_stock"
  },

  "slow_hash": "v1:1f0c9a…",

  // 以下是采集侧附加，契约未要求，收着无害
  "outcome": "ok",
  "completeness_ok": true,
  "review_hash": "v1:77ab12…",
  "hash_ver": 1,
  "recorded_at": "2026-08-05T09:11:07Z"
}
```

## 3. 边界语义（验收会测的项）

| 项 | 行为 |
|---|---|
| **`cursor=0` 从头拉** | 返回流里现存的最小 cursor 起的记录 |
| **重复返回无害** | 同一个 `cursor` 拉两次，结果完全一致。消费侧靠 `source_id` 幂等去重 |
| **`cursor` 相同多条不丢** | 平凡成立：`cursor` 唯一。见上文语义提醒 |
| **空结果** | **200** + `records: []` + `has_more: false` + `next_cursor` **原样返回**（不推进）。**永不用 404 表达「没有数据」** |
| **游标推进** | 只用响应里的 `next_cursor`。不要自己算 `max(cursor)`，也不要用 `cursor + count`。空页不推进是**唯一不丢数据的方向** |
| **cursor 有空洞** | 正常。底层序列非事务性，回滚的批次会烧号。**不要用「cursor 不连续」判断丢数据** |

### 状态码

| 码 | `error` | 你要做什么 |
|---|---|---|
| 200 | — | 处理并推进游标（含空结果） |
| 401 | `invalid_export_token` | 修 token，不要重试 |
| **409** | `cursor_below_retention` | **告警 + 全量对账 + 停**。你要的下一条已被保留期裁掉（**待写进 v1.1**，见上文第 6 条） |
| 422 | `invalid_parameter` | 修请求，不要重试 |
| 503 | `event_stream_unavailable` / `export_token_not_configured` | 退避重试并告警 |

## 4. 一条实现上的坑，写下来免得两侧都踩

`/api/export/incremental` 落在采集侧既有的 `GET /api/export/{batch_name}` 这条
**catch-all** 路由的前缀里。实测（新端点未挂载时）：

```
GET /api/export/incremental  ->  404 {"detail":"批次不存在: incremental"}
```

**404 正是消费方最容易读成「暂无数据」的码**——游标永不推进，同步静默停摆，两侧都不报错。

采集侧靠「注册顺序在 catch-all 之前」解决，并有回归守卫
（`tests/test_incremental_export.py::test_route_order_is_load_bearing`）钉死。

**对你侧的意义**：如果哪天你收到 `404` 且响应体里带「批次不存在」字样，
那不是没有数据，是**请求打歪了或采集侧路由退化了**——按 5xx 处理并告警，不要推进游标。

## 5. 与 `/api/v1/sync/*` 的关系

采集侧另有一组 `/api/v1/sync/{records,status,counts,ack,ack-prune}`，是**运维面**：
保留期水位、对账、位点确认。**你侧不实现它们也能正常消费**，本端点自足。

但有一条值得知道：采集侧的保留期下界是
`max(磁盘应急下界, min(时间下界, ack 下界))`。**你侧不调 `/ack` 时，`ack` 那一项不参与**，
保留期退化成「按时间 + 磁盘尽力而为」。这不会丢已经拉走的数据（中心库是持久副本），
但意味着「保留期绝不裁掉你还没拉的数据」这条**从可证降级为尽力而为 + 可检测**
（靠 409）。若将来想要那条强保证，接 `POST /api/v1/sync/ack` 即可，一个字段。
