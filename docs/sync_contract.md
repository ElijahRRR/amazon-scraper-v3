# catalog_sync 拉取契约 v1

> 采集侧（amazon-scraper-v3）→ 沃尔玛侧（catalog_sync）的**唯一**数据出口。
> 实现：`server/api/sync.py`。规格来源：`.agent/pg_migration_plan.md` §4 / §5。
> 契约测试：`tests/pgdb/test_sync_api.py`（37 个用例，每个对应本文的一句话）。
>
> **本文中标「硬性规则」的条目，违反即数据错误。** 它们不是建议，也不是
> 「按需实现」——每一条都对应一种在数据上看不出异常的静默错误。

- `contract_version`: **1**（每个响应都带；变了就是不兼容变更，会提前通知）
- 传输：HTTP/1.1，`Content-Type: application/json; charset=utf-8`
- 鉴权：**暂无**。服务端已留 router 级依赖口子，加的时候会提前通知并给过渡期。
- 建议节奏：**每 5 分钟一轮**，每页 `limit=1000`

---

## 0. 三十秒版

```
每 5 分钟：
  GET /api/v1/sync/status          → 检查 gen / max_seq / forced_prune_log
  GET /api/v1/sync/records?after_seq=<游标-OVERLAP>&limit=1000
      → 200：写 snapshots；outcome=='ok' 且 completeness_ok 才 upsert products
      → 409：告警 + 全量对账 + 停
  POST /api/v1/sync/ack {gen, ack_seq}
  has_more 为 true 就继续下一页
```

排序权威**只有 `seq`**。分组键**只有 `(asin, marketplace, zip_requested)`。
「没有新记录」**永远不等于**商品下架。

---

## 1. 端点

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/v1/sync/records` | 拉数据。拉取循环的主体 |
| GET | `/api/v1/sync/status` | 每轮开头的健康/一致性检查 |
| GET | `/api/v1/sync/counts` | 对账（抽样比对无漏采） |
| POST | `/api/v1/sync/ack` | 确认位点，解锁采集侧的保留期裁剪 |

> **前缀 `/api/v1` 是承重的，不要自己改写路径。**
> 采集侧存在 `GET /api/results/{asin}` 与 `GET /api/export/{batch_name}` 两条
> catch-all 路由，它们对不认识的名字回 **404**。把同步端点挂到那两个前缀下
> （或者请求时写错前缀）会得到一个 404，而 404 很容易被读成「暂无数据」，
> 于是游标永不推进、同步静默停摆。**本契约的四个端点永不用 404 表达「没有数据」。**

---

## 2. `GET /api/v1/sync/records`

### 参数

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `after_seq` | int ≥ 0 | **必填** | **独占**下界。返回 `seq > after_seq` 的记录。从头拉传 `0` |
| `limit` | int 1..1000 | 200 | 建议用 1000 |
| `outcomes` | csv | 全部 | 例 `ok,not_found`。**拉取循环里不要用**，见 §2.5 |

### 200 响应

```jsonc
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "min_available_seq": 39120041,
  "max_seq": 41872330,
  "relay_lag_seconds": 0.8,
  "outbox_depth": 12,
  "relay_state": "running",
  "server_time_utc": "2026-08-04T09:12:33.418922Z",

  "after_seq": 41208000,
  "next_after_seq": 41208500,
  "has_more": true,
  "count": 500,
  "limit": 500,
  "outcomes": null,
  "retention_forced": false,

  "records": [{
    "seq": 41208001,
    "source_id": "a3f19c2b7e04:evt-41208001",
    "gen": "a3f19c2b7e04",
    "asin": "B0CXXXXXXX",
    "marketplace": "amazon.com",
    "zip_requested": "10001",
    "zip_observed": "10001",
    "zip_verify": "confirmed",
    "collected_at": "2026-08-04T09:11:02.114000Z",
    "recorded_at":  "2026-08-04T09:11:07.902311Z",
    "outcome": "ok",
    "completeness": 0,
    "completeness_ok": false,
    "error_type": null,
    "error_detail": null,
    "batch_id": 8123,
    "task_id": 5512907,
    "worker_id": "w-hk-02",
    "attempt": 0,
    "parse_engine": "selectolax",
    "review_hash": "v1:77ab12…",
    "slow_hash": "v1:1f0c9a…",
    "hash_ver": 1,
    "payload": { /* 一次采集的完整结果，见 §6 */ }
  }]
}
```

### 2.1 顶层字段

| 字段 | 含义 |
|---|---|
| `gen` | 采集实例的**代号**。变化 = 硬停（§5 规则 3） |
| `instance_id` | 运维配置的部署标识。未配置时是 `"unconfigured"` |
| `min_available_seq` | **现在还能拉到的最小 seq**。空流时 = `max_seq + 1` |
| `max_seq` | 流头。**单调不减**，保留期裁剪不会让它回退 |
| `relay_lag_seconds` | outbox 里最老一条已经等了多久。持续 > 60 秒说明 relay 有问题 |
| `outbox_depth` | 还没进流的条数。单调增长 = relay 停摆 |
| `relay_state` | `running` / `stopped` / `starting` / `refused` / `failed` |
| `next_after_seq` | **下一轮的 `after_seq`**。见 §2.3 |
| `has_more` | 按**当前过滤条件**判定，精确（不是「seq < max_seq」那种估计） |
| `count` | `records` 的长度。恒等于 `len(records)` |
| `retention_forced` | 曾经发生过应急裁剪。为 true 时去 `/status` 读 `forced_prune_log` |

时间戳一律 **RFC 3339 UTC，带 `Z` 结尾**，可能带小数秒。

### 2.2 排序与分页

- 页内**严格按 `seq` 升序**，且所有 `seq > after_seq`。
- 页与页之间无重叠、无空洞（在没有 409 的前提下）。
- **`seq` 允许有空洞**，这是设计的一部分：底层序列非事务性，回滚的批次会烧号。
  连续性不是契约的一部分，**不要**用「seq 不连续」判断丢数据；
  要判断用 `/counts`（§4）。

### 2.3 游标推进规则（硬性规则）

```
X = r.next_after_seq
```

**不要**自己算 `max(rec.seq)`，也**不要**用 `after_seq + count`。
服务端保证：

- `records` 非空时，`next_after_seq == records[-1].seq`；
- `records` 为空时，`next_after_seq == after_seq`（**游标不推进**）。

游标只推进到**真正投递过的那一条**。这是唯一不会丢数据的方向。

### 2.4 空结果（硬性规则）

**本端点永不对空结果返回 404。** 空就是：

```
200  {"records": [], "has_more": false, "count": 0, "next_after_seq": <原样>}
```

这与采集侧其它导出端点（`/api/export/{batch_name}` 找不到批次回 404）**故意不同**。
若你在这四个端点上收到 404，那说明**路径写错了**或者请求被别的路由接走了，
**绝不是**「没有数据」——按 5xx 处理并告警。

### 2.5 `outcomes` 过滤（读前先读这一段）

`outcomes` 是**给对账和排查用的**，不要放进拉取循环。原因：

过滤是在 SQL 里做的，所以「空页不推进游标」这条规则在过滤下会让循环原地打转
（游标停在 `after_seq`，下一轮从同一个位置重扫）。
反过来，如果服务端替你把游标推到流头，你就会**跳过**那些被过滤掉、
但以后你可能需要的行。两害相权，服务端选了「不推进」。

**拉取循环一律不带 `outcomes`，全量收下，在你自己这一侧分流。**

### 2.6 状态码

| 码 | `error` | 条件 | 你要做什么 |
|---|---|---|---|
| 200 | — | 正常（含空结果） | 处理并推进游标 |
| **409** | `cursor_below_retention` | `after_seq + 1 < min_available_seq` | **告警 + 全量对账 + 停**。你要的下一条已经被裁掉了 |
| **409** | `cursor_ahead_of_stream` | `after_seq > max_seq` | **告警 + 全量对账 + 停**。采集侧疑似从备份恢复/回滚 |
| 422 | `invalid_parameter` / `range_too_wide` | 参数不合法 | 修请求，不要重试 |
| 503 | `event_stream_unavailable` | 采集侧跑在 SQLite 后端 / 库未就绪 / 事件流表未建 | 退避重试并告警 |
| 5xx | — | 服务端故障 | 退避重试并告警 |

> `429` 在计划里为背压预留，**当前不会发出**。你侧仍然应该实现它
> （收到就按 `Retry-After` 退避），这样将来加上时不需要改消费端。

422 有两种响应体：契约层面的错误带 `{"error": ..., "detail": "…"}`；
框架层面的类型错误（例如 `after_seq=abc`）只带 FastAPI 标准的
`{"detail": [ … ]}`（`detail` 是数组）。两者都不要重试。

两个 409 都是**服务端强制**的，没有任何参数能关掉。409 响应体形如：

```jsonc
{
  "error": "cursor_below_retention",
  "detail": "…人读的说明…",
  "after_seq": 41208000,
  "min_available_seq": 41300000,
  "max_seq": 41872330,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "server_time_utc": "…"
}
```

**409 永远不夹带半页数据** —— 响应体里没有 `records` 键。

> ⚠ **`cursor_below_retention` 有一类已知的假阳性，是有意保留的。**
> `seq` 允许有空洞。如果保留期边界正好落在一段被烧掉的号上，一个其实没掉窗的
> 游标也会拿到 409。代价是一次多余的全量对账；反方向（放宽判据）的代价是
> **静默丢数据**。采集侧 Phase 6 会持久记录实际裁掉的 floor，届时假阳性归零。
> 在那之前：**收到 409 就照章办事，不要试图自己判断它是不是假阳性。**

### 2.7 一致性保证

`min_available_seq`、`max_seq` 与页查询在**同一个 `REPEATABLE READ` 只读事务**
里完成，页查询之后还会用同一快照复核一次下界并取较大者。
因此不存在「保留期在 MIN 与页查询之间跑完 → 守卫用旧下界放行 → 你拿到一段
有洞的 200」这条竞态。这条保证由 `test_bounds_and_page_share_one_repeatable_read_snapshot`
守着（把隔离级别降成 READ COMMITTED 该用例立刻转红）。

---

## 3. `GET /api/v1/sync/status`

无参数。每轮拉取开始前调一次。

```jsonc
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "min_available_seq": 39120041,
  "max_seq": 41872330,
  "ack_seq": 41808200,          // 从没 ack 过就是 null，不是 0
  "ack_at": "2026-08-04T09:07:11.000123Z",
  "lag_records": 64130,          // max_seq - ack_seq；ack_seq 为 null 时也是 null
  "relay_lag_seconds": 0.8,
  "relay_state": "running",
  "outbox_depth": 12,
  "dead_letters": 0,             // 被隔离的毒丸行数。> 0 需要人工看
  "events_per_minute": 71.4,
  "oldest_recorded_at": "2026-06-19T02:00:11.881000Z",
  "newest_recorded_at": "2026-08-04T09:12:30.104000Z",
  "retention_forced": false,
  "forced_prune_log": [],
  "db_size_bytes": 18273615872,
  "free_disk_bytes": 41203105792,
  "free_disk_path": "/opt/amazon-scraper-v3",
  "observed_daily_insert_rate": 103882,
  "partitions": [{"name": "scrape_events_p1", "lo": 20000000, "hi": 40000000}],
  "future_partitions": 2,
  "server_time_utc": "2026-08-04T09:12:33.418922Z"
}
```

要点：

- **`ack_seq` 初值是 `null`，不是 `0`。** 你侧的解析必须区分这两者。
- `observed_daily_insert_rate` 是**上界估计**（`max_seq` 减去最近 24 小时内最早
  一行的 `seq`），因为 seq 会被回滚烧号。用来看数量级，别拿来对账。
- `free_disk_bytes` 量的是 `free_disk_path` 那块盘。PG 与采集服务同机部署。
- `forced_prune_log` 是**持久列表**，不是瞬时布尔。采集侧被迫应急裁剪时往里追加
  一条，**一直返回**直到你逐条确认。看到非空 = 有数据被强制丢弃，需要人处理。
- `dead_letters > 0` = 有行畸形到进不了事件流，被隔离了。那些行**不会**出现在
  `/records` 里，需要采集侧人工处理。

`min_available_seq` / `max_seq` 与 `/records` **逐字同源**，不会出现
「status 说还有、records 回 409」这种状态。

---

## 4. `GET /api/v1/sync/counts`

对账用。**闭区间** `[from_seq, to_seq]`。

| 参数 | 类型 | 说明 |
|---|---|---|
| `from_seq` | int ≥ 0 | 必填，含 |
| `to_seq` | int ≥ from_seq | 必填，含 |
| `bucket` | `hour` | 可选。带上就多返回按 UTC 整点分的桶 |

区间宽度 `to_seq - from_seq + 1` 上限 **8 000 000**，超出返回
`422 {"error": "range_too_wide"}`。

```jsonc
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "from_seq": 41000000, "to_seq": 41999999, "span": 1000000,
  "count": 812394,
  "min_seq": 41000003, "max_seq": 41999998,
  "min_recorded_at": "2026-07-28T11:04:00.100Z",
  "max_recorded_at": "2026-08-04T09:12:30.104Z",
  "by_outcome": {"ok": 790112, "not_found": 12033, "blocked": 8112, "parse_failed": 2137},
  "bucket": "hour",
  "buckets": [
    {"bucket_start": "2026-07-28T11:00:00Z", "count": 4021,
     "min_seq": 41000003, "max_seq": 41004024}
  ],
  "stream_min_available_seq": 39120041,
  "stream_max_seq": 41872330,
  "range_fully_retained": true,
  "server_time_utc": "…"
}
```

- `sum(by_outcome.values()) == count`，`sum(b.count for b in buckets) == count`。
- 桶边界是 **UTC 整点**，与服务端会话时区无关。
- 空区间是 `count: 0` + `min_seq: null`，**不是错误**。
- **先看 `range_fully_retained`。** 为 `false` 时 `count` 偏小是因为那段被裁剪了，
  不是漏采 —— 拿它去判「漏采」会得出错误结论。
  它**只看下界**（`max(from_seq, 1) >= stream_min_available_seq`）：保留期只从
  底部裁，所以「这段有没有被裁过」等价于「区间下界还在窗口里」。
  `to_seq` 超过 `stream_max_seq` 只是「还没采到那儿」，不是「没保留」——
  要判断这个自己比 `stream_max_seq`。
  （`seq` 由 bigserial 发号、永远 ≥ 1，所以 `from_seq=0` 与 `from_seq=1`
  是同一个区间。）

---

## 5. `POST /api/v1/sync/ack`

```jsonc
// 请求
{"gen": "a3f19c2b7e04", "ack_seq": 41808200}
```

`ack_seq` = 「这个位置及之前的数据，持久副本已经拿到了」。
采集侧不做本地备份，你侧的中心库就是持久副本 —— 所以 `ack_seq`
是保留期敢往下裁的**唯一**依据。**请老实 ack，也不要提前 ack。**

```jsonc
// 200
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "ack_seq": 41808200,        // 生效值（单调 max 之后的）
  "sent_ack_seq": 41808200,   // 你发来的
  "advanced": true,
  "ack_at": "2026-08-04T09:12:33.418922Z",
  "min_available_seq": 39120041,
  "max_seq": 41872330,
  "lag_records": 64130,
  "server_time_utc": "…"
}
```

规则：

- **单调取 max，永不后退。** 发一个比已存值小的 `ack_seq` 会返回 200，
  但 `ack_seq` 字段回的是**已存的那个更大值**，`advanced` 为 `false`。
- **并发安全。** 多个并发 ack 不会互相覆盖。
- `gen` 不符 → `409 {"error": "gen_mismatch"}`，**不写任何东西**。
- `ack_seq > max_seq` → `409 {"error": "ack_ahead_of_stream"}`，**不写任何东西**。
  确认一段本实例从未发出过的 seq，等于授权保留期裁掉你其实没拿到的数据。
- 形状不对（缺字段 / `ack_seq` 是字符串或布尔 / 负数 / `gen` 空串）→ `422`。

---

## 6. 记录字段

### 6.1 采集参数

| 字段 | 说明 |
|---|---|
| `asin` | Amazon ASIN |
| `marketplace` | **封闭集**，当前值域是单元素集 `{"amazon.com"}`。域名形式，由采集侧 CHECK 约束强制。加站点时会提前通知 |
| `zip_requested` | worker 实际请求的邮编，5 位补零 |
| `zip_observed` | 从页面抽出的邮编。**Phase 4 之前恒为 `null`** |
| `zip_verify` | `confirmed` / `assumed` / `mismatch` / `unverified`。**Phase 4 之前恒为 `unverified`** |

> **硬性规则：分组键是 `(asin, marketplace, zip_requested)`。**
> 只按 `asin` 分组会退化成「最近哪个邮编采的」，价格序列会在邮编之间振荡，
> 而且从数据上完全看不出异常。

### 6.2 时间

| 字段 | 权威性 |
|---|---|
| `collected_at` | **worker 时钟，仅供参考。** 不同 worker 之间可能不同步 |
| `recorded_at` | 服务端时钟，展示用 |
| `seq` | **唯一的排序权威** |

> **硬性规则：「同组最新值」一律按 `seq` 排序，不得用 `recorded_at`，
> 更不得用 `collected_at`。** NTP 前跳/后跳会让时间戳与 seq 非单调。
> products 的单调守卫写成 `WHERE excluded.seq > products.last_seq`。

### 6.3 质量 / 溯源

| 字段 | 说明 |
|---|---|
| `outcome` | `ok` / `not_found` / `blocked` / `parse_failed` / `stale`。封闭集 |
| `completeness` | 位图，见 §6.4 |
| `completeness_ok` | 服务端算好的合取结果，见 §6.4 |
| `error_type` / `error_detail` | `outcome != 'ok'` 时的原因 |
| `batch_id` / `task_id` / `worker_id` / `attempt` | 溯源 |
| `parse_engine` | `selectolax` / `lxml`。**Phase 4 之前可能为 `null`** |

> **硬性规则：`outcome != 'ok'` 的记录只入 `snapshots`，
> 不触发 `products` upsert，其哈希不参与复审判定。**
> 理由：`not_found` 的 payload 里有 30/40 个占位符，对它算哈希得到的是
> 「好页 → 404 → 好页」每次都翻转的值 —— 正是复审门要防的误复审模式。
> （采集侧已在两层各拦一次：`outcome != 'ok'` 时 `review_hash` 与 `slow_hash`
> **都**写 `null`。你侧这条规则是第三道。）

`stale` 的含义：那次采集是**完整真实**的，但提交时租约已经被回收了
（worker 只是慢，不是死了）。它进流是为了不丢失观测，但它**不代表当前状态**。

### 6.4 `completeness` 与 `completeness_ok`（**现在必读**）

位图定义：

| 位 | 值 | 含义 |
|---|---|---|
| 0 | 1 | 面包屑区块存在 |
| 1 | 2 | 详情表存在 |
| 2 | 4 | 主图集存在 |
| 3 | 8 | **MEASURED**：这次采集真的测量过上面三项 |

```
completeness_ok  ⟺  (completeness & 8) != 0  AND  (completeness & 7) == 7
```

服务端已经算好放在 `completeness_ok` 字段里，直接用，不要自己拼。

> ⚠ **采集侧 Phase 4 落地之前，`completeness` 恒为 `0`，
> 因此 `completeness_ok` 恒为 `false`。**
> `0` 的含义是「**未测量**」，不是「三项都缺」。
>
> 按 §7 的算法字面执行，这意味着 **Phase 4 之前没有任何一行会进
> `catalog.products`**，`snapshots` 会正常累积。这是**已知且预期**的。
>
> 试点期若需要提前打通 products 通路，请与采集侧约定一个**显式的、
> 有时限的**旁路（例如「Phase 4 前把 `completeness_ok` 视为 true」），
> 并在采集侧 `/status` 上能看到 `completeness` 开始出现非零值之后立刻撤掉。
> **不要**把「`completeness == 0` 当成 ok」写死进代码 —— Phase 4 之后
> `0` 会重新变成真正的「未测量」信号。

### 6.5 哈希与复审门

| 字段 | 说明 |
|---|---|
| `review_hash` | 复审门用。字段集见 `.agent/pg_migration_plan.md` §4.1 |
| `slow_hash` | 身份层变化检测，**不当门** |
| `hash_ver` | 整数，当前恒为 `1`。哈希串本身带 `"v1:"` 前缀 |

> **硬性规则：复审门是合取式。**
>
> ```
> 需要复审 ⟺ 本条 outcome == 'ok'
>          AND 本条 completeness_ok
>          AND products 中上一条 completeness_ok
>          AND review_hash 与存储值不同
>          AND hash_ver 与存储值相同
> ```
>
> **为什么必须是合取**：类目只有面包屑一个数据源，而面包屑正是软降级页会剥掉的
> 区块。好页 → 降级页 → 好页 = 两次哈希翻转 = 两次误复审。归一化解决不了，
> 因为 NULL 仍然 ≠ 真值，两个方向都翻。
> **占位符进/出永不触发复审。**

`hash_ver` 升级时采集侧会走「过渡期双输出」：同时输出 v1 和 v2，
你侧继续用 v1 把门、后台回填 v2，回填完再切。不会有一次性全语料复审风暴。

### 6.6 `payload`

一次采集的完整结果，JSON 对象（**不是**被引号包起来的字符串）。
消费侧自行解析，采集侧不保证它的键集跨版本稳定 —— 稳定的是本文列出的**顶层字段**。

已知的形状约束：

- **lxml 回退路径与全部早退路径上，`rating` / `review_count` / `seller_id` /
  `seller_name` 这 4 个字段在 `payload` 里是「缺席」** —— 不是 `null`，
  更不是旧值。用 `key in payload` 判断，不要用 `payload.get(k) is None`。
- **`screenshot_path` 是易失引用（硬性规则）。** 该键**可能缺席**（不是所有采集
  都出截图）。
  截图文件会被常规清理，采集侧**不做**独立持久化，且明确接受随机器丢失。
  所以这个路径**随时可能指向已删除的文件**。
  **消费侧不得依赖该路径可解引用，也不得据此判断截图是否曾经存在。**
  需要截图时走采集侧现有的导出接口现取，取不到就是没有了。

---

## 7. 消费侧拉取算法（契约的一部分，不是建议）

```python
OVERLAP = 200          # 重叠回拉的条数，宁可重复

st = GET /api/v1/sync/status

# --- 三道硬停检查，缺一不可 ---
if st.gen != stored_gen:
    ALARM("generation changed"); full_reconcile(); STOP
if st.max_seq < stored_max_seq_ever:
    ALARM("stream rewound"); full_reconcile(); STOP
if st.forced_prune_log:
    ALARM(st.forced_prune_log)        # 逐条处理并确认

stored_max_seq_ever = max(stored_max_seq_ever, st.max_seq)

X = max(0, stored_cursor - OVERLAP)   # 重叠回拉
while True:
    r = GET /api/v1/sync/records?after_seq=X&limit=1000

    if r.status == 409:
        ALARM(r.error); full_reconcile(); STOP
    if r.status == 404:
        ALARM("路径写错了或被别的路由接走了"); STOP     # 绝不是"没有数据"
    if r.status >= 500:
        backoff(); continue

    for rec in r.records:
        INSERT INTO catalog.snapshots (...)
          VALUES (...) ON CONFLICT (source_id) DO NOTHING     # 幂等锚点

        if rec.outcome == 'ok' and rec.completeness_ok:
            UPSERT catalog.products ...
              WHERE excluded.seq > products.last_seq          # 按 seq，不按时间

    X = r.next_after_seq                  # 只用服务端给的值
    stored_cursor = X
    POST /api/v1/sync/ack {"gen": st.gen, "ack_seq": X}

    if not r.has_more:
        break
```

### 硬性规则汇总（违反即数据错误）

1. **「同组最新值」一律按 `seq` 排序**，不得用 `recorded_at`，
   更不得用 `collected_at`。时钟前跳/后跳会让时间戳与 seq 非单调。
2. **分组键 = `(asin, marketplace, zip_requested)`。** 只按 asin 分组会退化成
   「最近哪个邮编采的」，价格序列在邮编间振荡且无法察觉。
3. **`gen` 变化是硬停**，不是「正常、无需动作」。它意味着采集侧是一套全新的
   实例（重装 / 从备份恢复 / 克隆部署），历史 `seq` 与新 `seq` 不可比。
4. **绝不把「没有新记录」读成下架/撤回**，也不要发 tombstone。
   采集侧有多条无鉴权删除端点，其中 `DELETE /api/results` 用
   `asin LIKE ? OR title LIKE ? OR brand LIKE ?` 模糊选目标 ——
   一次手滑会被复制成中心库里的大规模墓碑。
   商品下架必须由**独立的、显式的**信号驱动，不由本流的沉默驱动。
5. **`outcome != 'ok'` 的记录只入 snapshots，不触发 products upsert，
   其哈希不参与复审判定。**
6. **复审门是 §6.5 的合取式。占位符进/出永不触发复审。**
7. **`source_id` 是幂等锚点。** 形如 `{gen}:{uuid}`，在采集侧写入时铸造，
   重放不变。`ON CONFLICT (source_id) DO NOTHING` 是你侧唯一需要的去重。
8. **`screenshot_path` 不可解引用**（§6.6）。

### 重叠回拉为什么是必须的

`ack` 与写库不在同一个事务里，你侧崩溃可能落在两者之间。
`OVERLAP` 让你重复读回若干条已处理的记录，靠 `source_id` 的
`ON CONFLICT DO NOTHING` 吸收。**重复是安全的，空洞不是。**

`OVERLAP` 取多少：≥ 一次崩溃可能丢失的最大条数。建议 200，代价可以忽略。
把 `OVERLAP` 设成 0 等于赌「ack 之后一定写成功了」。

---

## 8. 不在本流里的东西（免得被读成数据丢失）

| 项 | 说明 |
|---|---|
| **卖家发现任务** | `accept_seller_discovery_result` 属于不同的域（`seller_discoveries` 表，无商品 payload），**不进本流**。需要的话是**第二条流**，不是混进这一条 |
| **截图文件** | 从不进中心库，也不可重建。见 §6.6 |
| `tasks` / `batches` / `asin_changes` | 采集侧内部状态，不进流。采集侧重装后这些归零，属于已接受的损失 |
| **被隔离的毒丸行** | 进 `scrape_outbox_dead`，不进流。`/status` 的 `dead_letters` 会显示条数 |

---

## 9. 故障排查速查

| 现象 | 多半是 |
|---|---|
| 四个端点全 404 | 路径写错，或者请求打到了别的服务。**不是没有数据** |
| 503 `event_stream_unavailable` | 采集侧跑在 SQLite 后端，或者还在启动。退避重试 |
| 409 `cursor_below_retention` | 你落后太多掉出保留窗口了（或者踩到了 §2.6 的已知假阳性）。全量对账 |
| 409 `cursor_ahead_of_stream` | 采集侧从备份恢复/回滚了。全量对账 |
| 200 但 `records` 一直是空的，`max_seq` 不涨 | 看 `/status` 的 `relay_state` 与 `outbox_depth`：`outbox_depth` 单调增长 = relay 停摆，找采集侧 |
| `products` 一行都没进 | 看 §6.4。Phase 4 之前 `completeness_ok` 恒为 false，这是预期 |
| `count` 比预期少 | 先看 `/counts` 的 `range_fully_retained`。为 false 说明是被裁剪，不是漏采 |
| 同一商品价格在两个值之间振荡 | 分组键漏了 `zip_requested`（硬性规则 2） |

---

## 10. 采集侧已知边界（诚实清单）

1. **整机快照回滚采集侧检测不到。** 只回滚数据库能被检出（会铸新 `gen`），
   但连同 meta 一起回滚的整机快照检不出来 —— 这是设计上的边界。
   **你侧的 `st.max_seq < stored_max_seq_ever` 单调性检查是唯一防线**，
   必须实现，且必须是告警而不是静默继续。
2. `cursor_below_retention` 的假阳性（§2.6），Phase 6 修。
3. Phase 4 之前：`zip_observed` 恒 `null`、`zip_verify` 恒 `unverified`、
   `completeness` 恒 `0`、`parse_engine` 可能为 `null`、`crawl_time` 时区待统一。
4. 采集侧的写路径今天是串行的（单写连接 + 真锁），所以「乱序提交」在今天的 API
   上还不可能发生。事件流的 outbox + 单 relay 是**提前建好的保险**，
   Phase 1.5 放开写并发时它就地生效，届时对你侧零改动。
