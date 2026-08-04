# amazon-scraper-v3 → PostgreSQL 迁移 + catalog_sync 事件流 改造计划

> 配套文档：`.agent/catalog_sync_audit.md`（迁移前现状审计，全部结论带 `file:line`）。
> 该审计中所有 SQLite 特有的加固项（哨兵行、`sqlite_sequence` 计数器修复、flock 单写者断言、
> ATTACH 跨库原子性、`VACUUM` 停机窗口、`incremental_vacuum` 与 `executescript` 隐式提交）
> **在本计划下全部作废**，不再实施。

---

## 0. 前提与边界

| 项 | 结论 | 来源 |
|---|---|---|
| 数据迁移 | **不需要**。切换后重新采集几小时即可 | 用户确认 |
| 旧系统 | SQLite 版原地继续运行，直到新系统验稳 | 用户确认 |
| 兼容约束 | 从「只加不改」降级为「**API 响应形状不变**」；存储层可重写 | 全新部署的推论 |
| erpAPI | 正在重构，端点清单待提供 | 用户确认 |
| 鉴权 | 暂不加，端点留 router 级依赖的空口子 | 用户确认 |
| 失败/降级采集 | **进流**，带 `outcome` 枚举 | 用户确认 |
| ack 契约 | 做，排后期，非阻塞 | 用户确认 |
| 日采集量 | 当前 10 万/天，未来可能几十万，绝大多数是重复采集 | 用户确认 |
| `marketplace` 表示法 | **域名形式**，当前值域是单元素集 `{'amazon.com'}` | 用户确认 |
| `crawl_time` 时区 | **改为带时区 UTC**，erpAPI 可接受 | 用户确认 |
| PG 部署 | **与 scraper 同机** | 用户确认 |
| 目标机器 | **2 核 / 4GB / 80GB SSD**（新机，非现有的 1C/2GB/20GB） | 用户确认 |
| 备份策略 | **不做本地备份**；沃尔玛侧中心库即持久副本，需要时从那边拉 | 用户确认 |
| 截图持久化 | **不需要**。截图是一次性的，取用后即可清理，接受随机器丢失 | 用户确认 |
| 拉取节奏 | **5 分钟** / 每页 1000 | 用户确认 |

**PG 版本要求**：≥ 14（推荐 16/17）。声明式分区、`FOR UPDATE SKIP LOCKED`、
`gen_random_uuid()` 内置均已具备。本方案不依赖 `xid8`/`pg_current_xact_id()`
（那是备选方案 B 才需要的，需 ≥ 13）。

**驱动**：`asyncpg` + 显式连接池。理由：当前是 `aiosqlite` 全异步，`asyncpg` 是同生态最快的选择；
`psycopg3` 的 async 也可以，但 `asyncpg` 的 `Record` 到 dict 的转换路径与现有 `aiosqlite.Row`
用法最接近，移植改动最小。**注意 `asyncpg` 用 `$1` 占位符，不是 `?`**——这是移植中最机械也最容易漏的一项。

**schema 布局**：采集侧自建 `scraper` schema，与沃尔玛侧的 `catalog` 完全无关，两边不共享数据库。

### 0.1 目标机器与资源规划

**目标机器：2 核 / 4GB / 80GB SSD**（新机，不是现有那台 1C/2GB/20GB 的 DMIT VPS，
README.md:8, 24, 546 记的是旧规格）。10 个 worker 仍分布在别处，本机只跑 server + PG。

相对旧机的两倍 CPU / 两倍内存 / 四倍磁盘，把之前按 1C/2GB/20GB 做的所有资源结论都翻掉了：

| 原结论（旧机） | 新机下 |
|---|---|
| 绑定约束是 1 vCPU，PG 与 uvicorn 抢同一个核 | **2 核，两者可真正并行。仍需在 Phase 5 实测，但不再是预判的瓶颈** |
| `shared_buffers` 只能给 256MB（12.5%） | **可以给到标准的 25% = 1GB** |
| 必须关并行查询（`max_parallel_workers_per_gather = 0`） | **可以开到 1**，让大导出用上第二个核 |
| 20GB 盘只够 7 天保留期（几十万/天时） | **80GB 盘下保留期不再是约束**，见 §0.2 |
| 建议开 `synchronous_commit = off` 缓解单核 fsync 压力 | **建议保持默认 `on`**，见下 |

建议的 `postgresql.conf`（4GB，与应用同机）：

```conf
shared_buffers = 1GB                      # 标准 25%
effective_cache_size = 2GB                # 扣掉应用常驻后的诚实估计，不是 75%
work_mem = 8MB                            # 峰值 ≈ max_connections × work_mem × 每查询排序数
maintenance_work_mem = 256MB              # 利于 autovacuum 与建索引
max_connections = 30
max_parallel_workers_per_gather = 1       # 2 核：留一个核给 uvicorn
max_parallel_workers = 2
autovacuum_max_workers = 3
random_page_cost = 1.1                    # SSD
checkpoint_completion_target = 0.9
default_toast_compression = lz4           # PG14+；比 pglz 快得多，压缩率略低。
                                          # 事件表的 jsonb payload 全部走 TOAST，这一项直接影响写入 CPU
```

`asyncpg` 连接池上限须 ≤ `max_connections` 并留维护余量（建议池 = 20）。

**`synchronous_commit` 保持默认 `on`。** 旧机上建议关掉是为了缓解单核 fsync 压力；
2 核 + 4GB 下这个压力不再突出，而关掉的代价（崩溃丢最后约 0.2 秒已提交事务）是实打实的。
**只有当 Phase 5 实测确认 fsync 是瓶颈时才考虑关**，并写进 runbook——不要默认开着。

### 0.2 保留期：80GB 下不再是约束

事件表按 jsonb payload 约 2KB/条估（TOAST + lz4 后）：

| 日采集量 | 日增长 | 14 天 | 30 天 | 90 天 |
|---|---|---|---|---|
| 10 万 | 200 MB | 2.8 GB | 6 GB | 18 GB |
| 50 万 | 1 GB | 14 GB | 30 GB | 90 GB ✗ |

80GB 盘扣掉系统、`scraper.products`/`tasks`、截图与导出，事件表可用空间约 50-60GB。

**结论：10 万/天下 90 天绰绰有余；涨到 50 万/天时 30 天仍然安全。**
既然中心库是持久副本（§0.3），**实际建议取 30 天**——足够覆盖任何合理的消费侧故障窗口，
又留了三倍以上的增长余量。

保留期仍然由「剩余磁盘下限 + 分区行数上限」驱动、天数只作观测结果这一点不变
（`/status` 必须暴露 `observed_daily_insert_rate` 与 `free_disk_bytes`），
但它从「随时可能咬人的硬约束」降级为「兜底护栏」。

> 2KB/条仍是估计值。审计里我在合成语料上测得 zlib-1 约 3.5x；PG 的 TOAST + lz4 压缩率略低。
> 80GB 下即使估错 2 倍也不影响结论，所以**不必为此阻塞开工**，Phase 5 实测后回填真实值即可。

### 0.3 「中心库即备份」恢复什么、不恢复什么

策略已定：采集侧不做本地备份，沃尔玛侧中心库是持久副本。
地理上是分离的（scraper 在 VPS，中心库在本机），这一点成立。

**重建路径是干净的**：VPS 丢失后重装 → 新 `gen` → catalog_sync 检出 `gen` 变化 →
硬停 + 全量对账 → 新事件在新 `gen` 下累积，与历史 `source_id` 不可能碰撞。
这正是 §5.5 设计的行为，不需要额外机制。

**中心库能恢复的**：所有 catalog_sync 已拉走的观测。`scrape_events.payload` 是完整采集结果，
足以重建 `scraper.products` 的内容。

**中心库恢复不了的**（丢了就是丢了，需要知情）：

| 项 | 说明 |
|---|---|
| `tasks` 队列 | 当前 113 万行，含在途租约。重装后所有排队与在途任务归零 |
| `batches` / `batch_asins` | 批次元数据、`external_id`、callback 状态。调用方的批次追踪断链 |
| **截图文件** | `server/static/screenshots/` 下的实体文件**从不进中心库**，也不可重建。**已决策：接受丢失**，不做独立持久化——截图是一次性的，取用后即清理 |
| `screenshots` 表 | 截图任务追踪状态 |
| `asin_changes` | 变动历史 |
| `seller_discoveries` | 卖家店铺发现结果 |
| 调度与运行时设置 | `data/schedules/`、`runtime_settings.json` |
| 最后 ≤5 分钟 + 未 relay 的窗口 | 尚未被拉走的部分 |

按用户「重采几小时就够」的口径，以上各项均可接受（重传 ASIN 表即可）。
截图已明确决策为接受丢失，不引入对象存储。

**由此产生一条必须写进契约的条款：`screenshot_path` 是易失引用。**
截图会被常规清理，所以中心库 `catalog.snapshots` 里存下来的路径**随时可能指向已删除的文件**。
它仍然放进 `payload`（那是「一次完整采集结果」的一部分），但契约里必须写明：
**消费侧不得依赖该路径可解引用，也不得据此判断截图是否曾经存在。**
需要截图时走采集侧的现有导出接口现取，取不到就是没有了。

（顺带：现有 `asin_data.screenshot_path` 今天就已经存在悬空路径的情况，
这不是新问题，只是首次被写进对外契约。）

**连带影响：`ack` 契约的语义变得明确。** `ack_seq` = 「持久副本已经拿到这个位置」，
保留期裁到 `ack_seq` 之下就是安全的。这让 Phase 6 从「nice to have」变成
「短保留期的正确性前提」。

---

## 1. 游标正确性方案（承重决策）

### 1.1 问题

Postgres 下 `seq > X` 轮询会**永久跳过行**，这是并发写入时的默认行为，不是边界情况：

```
事务 A: INSERT → nextval 拿到 seq=100   （未提交）
事务 B: INSERT → nextval 拿到 seq=101
事务 B 先提交 ─────► 消费者轮询：只看到 101
消费者游标推到 101
事务 A 后提交 ─────► seq=100 永远拉不到
```

SQLite 下不可能发生（单写连接串行提交），而去掉那把锁正是迁 PG 的目的。
业内称此问题为 **out-of-order sequence commit**。

### 1.2 选定方案：transactional outbox + 单 relay

核心：**游标会漏行是因为游标"跳过"；队列不会，因为它"认领"。**
未提交的行对认领者不可见，下一轮自然被捞走，不存在"推过去就回不来"。

```
worker 提交结果
  └─ 与结果写入【同一事务】INSERT INTO scraper.scrape_outbox
         （写侧全并发，id 乱序/空洞均无所谓）
              ↓
  单 relay 后台任务，每 1s 一轮，【一个事务内】：
     DELETE FROM scraper.scrape_outbox
       WHERE id IN (SELECT id FROM scraper.scrape_outbox
                    ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED)
       RETURNING *
     → INSERT INTO scraper.scrape_events (...)      seq BIGSERIAL
              ↓
  scrape_events 唯一写入者是 relay，串行提交
  ⇒ seq 顺序 == 提交顺序 == 可见顺序
```

**保证（可测试的一句话）**：

> 对固定的 `gen`，若消费者持久化 X = 已见过的最大 `seq` 且总是请求 `after_seq = X`，
> 则任何在后续请求时刻已提交且 `seq > X` 的行，必定在该次或之后某次请求中被返回，
> 且按 `seq` 严格递增。绝不跳过已提交行。重复返回可能发生，`source_id` 使其无害。
> `gen` 变化或 `X + 1 < min_available_seq` 时保证失效（服务端返回 409）。

**证明骨架（三环）**：

| 环 | 机制 | 如何钉死 |
|---|---|---|
| 1. `scrape_events` 只有一个写入者 | 只有 relay 写 | relay 启动时取 `pg_try_advisory_lock(<常量>)`，取不到就不启动。滚动部署/多进程都撞不了车 |
| 2. relay 串行提交 | 单任务、单事务、逐批 | 代码结构保证；加断言：relay 内不允许并发 `gather` |
| 3. 消费者看不到未提交行 | PG 默认 read committed | 天然成立；且 relay 的 DELETE+INSERT 同事务，崩溃即回滚，行留在 outbox |

**兜底（对应简报约束 #4「宁可重复」）**：消费侧从 `cursor - K` 重叠拉起，`source_id` 幂等吃掉重复。

### 1.3 为什么不选其他方案

| 方案 | 否决理由 |
|---|---|
| xid8 水位 `xact_id < pg_snapshot_xmin(...)` | 只要系统里有长事务，水位就被钉死。这是业内公认的该方案致命缺陷 |
| 时间窗口 `inserted_at < now() - 30s` | 只是概率性，不是证明。不满足简报约束 #4 |
| 序列表 + 行锁串行化写入 | 写吞吐塌陷，等于放弃迁 PG 的收益 |
| advisory lock 精确天花板 | 严格且写侧全并发，但要读 `pg_locks` 内部表，实现与维护复杂度显著高于 outbox，收益相同 |
| 逻辑复制 / CDC（Debezium 系） | 天然 commit 顺序，最成熟；但需 replication slot，**slot 卡住会撑爆 WAL 打满磁盘**，对「HTTP 拉取」场景过重 |

**一处诚实修正**：最初否决 xid8 时，我举的主要例子是「每天一次 `pg_dump` 会钉死水位」。
既然已确认不做本地备份（§0.3），这个具体理由不再成立，两者的差距因此收窄。
但结论不变——剩余的长事务来源（大批量导出、临时 psql 会话、将来任何一个忘了及时提交的
事务）依然会让水位停摆，而 outbox + relay 对此完全免疫，代价只是一张表加一个后台任务。

**备选方案 B（若将来 relay 成为瓶颈或运维负担）**：切到 xid8 水位。
接口契约设计成两者通用（响应里都有 `max_seq` / `min_available_seq` / 水位滞后指标），
**切换实现不需要改契约**。

---

## 2. 数据模型

### 2.1 事件流（新增）

```sql
CREATE SCHEMA IF NOT EXISTS scraper;

-- ---------- outbox：写侧入口，全并发，短命 ----------
CREATE TABLE scraper.scrape_outbox (
    id          bigserial PRIMARY KEY,
    enqueued_at timestamptz NOT NULL DEFAULT now(),
    body        jsonb       NOT NULL      -- relay 需要的全部原料
);
-- 常驻行数 ≈ 一秒的产出量，不需要分区，不需要额外索引（PK 即可）

-- ---------- 事件流：只追加，唯一写入者是 relay ----------
CREATE TABLE scraper.scrape_events (
    seq           bigserial   NOT NULL,
    source_id     text        NOT NULL,   -- '{gen}:{uuid}'
    gen           text        NOT NULL,   -- 实例代号，逐行落库
    asin          text        NOT NULL,

    -- 采集参数（需求 3）
    marketplace   text        NOT NULL
                  CHECK (marketplace IN ('amazon.com')),   -- 封闭集，加站点时改这一行
    zip_requested text        NOT NULL,   -- 5 位补零，worker 实际请求的邮编
    zip_observed  text,                   -- 页面 glow-ingress-line2 抽出，Phase 4 前为 NULL
    zip_verify    text        NOT NULL,   -- confirmed|assumed|mismatch|unverified

    -- 时间（需求 4）
    collected_at  timestamptz NOT NULL,   -- worker 时钟，仅供参考
    recorded_at   timestamptz NOT NULL,   -- 服务端时钟，展示用；排序权威仍是 seq

    -- 质量 / 溯源
    outcome       text        NOT NULL,   -- ok|not_found|blocked|parse_failed|stale
    completeness  int         NOT NULL DEFAULT 0,   -- 位图，见 §3.3
    error_type    text,
    error_detail  text,
    batch_id      bigint,
    task_id       bigint,
    worker_id     text,
    attempt       int         NOT NULL DEFAULT 0,
    parse_engine  text,                   -- selectolax|lxml

    -- 哈希（需求 5）
    review_hash   text,                   -- outcome<>'ok' 时为 NULL
    slow_hash     text,
    hash_ver      int         NOT NULL DEFAULT 1,

    payload       jsonb       NOT NULL,   -- 一次采集的完整结果
    PRIMARY KEY (seq)
) PARTITION BY RANGE (seq);

-- 按 seq range 分区（不是按时间）：游标查询天然分区裁剪，保留期 = DROP 最老分区。
-- 每 2000 万行一个分区；10 万/天下约 200 天一个分区，几十万/天下约 40 天。
CREATE TABLE scraper.scrape_events_p0 PARTITION OF scraper.scrape_events
    FOR VALUES FROM (MINVALUE) TO (20000000);
-- 后续分区由维护任务提前创建（永远保持至少 2 个未来分区）

CREATE UNIQUE INDEX ON scraper.scrape_events (source_id);   -- 幂等锚点，同时是坏数据的响铃
CREATE INDEX ON scraper.scrape_events (recorded_at);        -- /counts 按时间分桶

-- ---------- 同步元数据 ----------
CREATE TABLE scraper.sync_meta (k text PRIMARY KEY, v text NOT NULL);
-- 键：contract_version / gen / instance_id / ack_seq / ack_at / forced_prune_log
```

设计说明：

- **`source_id = '{gen}:{uuid}'`**。PG 的 sequence 回滚只留空洞、永不复用，
  所以不需要 SQLite 版那个 `{gen}:{seq}:{rid}` 的随机后缀。`gen` 每次启动新铸并**逐行落库**
  （只存 meta 表的话，一次从备份恢复会把全部历史重贴上恢复后的标签）。
- **`UNIQUE(source_id)` 建**。SQLite 版为省 WAL 放弃了它，PG 下这个成本不值一提，
  而它是「同一条记录被 relay 写了两遍」的唯一硬防线。
- **`payload` 用 `jsonb`**，PG 自动 TOAST + 压缩，不需要手动 zlib。
  代价是 `jsonb` 会重排键序、丢重复键——对我们无影响（payload 是消费侧解析的，不参与哈希）。
- **按 `seq` range 分区而非时间**：游标查询 `WHERE seq > X ORDER BY seq LIMIT n`
  直接分区裁剪，无需 MergeAppend。保留期变成 `DROP TABLE <最老分区>`——瞬时、无膨胀、无 VACUUM。
- **排序权威是 `seq`，不是任何时间戳**。时钟前跳/后跳会让时间戳与 seq 非单调。

### 2.2 采集侧主体表（移植 + 重构）

原则：**先移植到行为等价，再重构**。但纯 SQLite workaround 直接删除。

| 原表 | PG 下 | 变化 |
|---|---|---|
| `asin_data` | `scraper.products` | 列基本原样；~~`crawl_time`/`created_at`/`updated_at` 改 `timestamptz`~~ → **保持 `text`**（见下方修订说明与 D-1）；`content_hash`/`title_bullets_hash` 保留不动（`asin_changes` 依赖它） |
| `asin_changes` | `scraper.asin_changes` | 原样 |
| `tasks` | `scraper.tasks` | 原样移植；`lease_epoch` 机制**先保留**，Phase 1.5 再评估改 `FOR UPDATE SKIP LOCKED` |
| `batches` / `batch_asins` / `screenshots` / `seller_discoveries` | 同名 | 原样 |
| `asin_data_fts`（FTS5 trigram） | **删除**，改 `pg_trgm` + GIN | 见 §3.2 |

**SQLite workaround 直接删除的部分**：只读连接池 `_read_pool`（由 asyncpg.Pool 顶替）、
`maintenance_loop` 的 WAL checkpoint、全部 `PRAGMA`、`wal_checkpoint` 的实际动作。
四个公开方法（`_open_read_pool` / `run_startup_optimize` / `maintenance_loop` /
`start_maintenance` / `wal_checkpoint`）**签名与 sync/async 性质必须保留**——
app.py:171/174/306 与 harness 都按名字调用，`start_maintenance` 还是**同步**方法。

> ⚠ **修订（D-3）**：原文把 `_write_lock` / `TimedLock` 与 `/api/_debug/lock-stats`
> 列入"直接删除"，**这是错的**。黄金基线 step 56 把该端点的 key 集合钉死了：
> `waits`/`holds` = {accept_results_batch, other, pull_tasks}，
> `stage_timings` = {commit, save_result, total_in_lock, update_tasks_lease}，
> 且 `_summary` 对空样本返回形状不同的 `{"count": 0}`。删掉 = 七个 key 同时"字段消失"，
> 64 步校验必挂。因此 pgdb **从 `common.database` 共享同一个 `TimedLock` 与
> `LOCK_STATS` 对象**（不是拷贝——app.py:2625 是按模块全局对象 import 的），
> 命名调用点与五处 `record_stage()` 全部原地保留。

### 2.3 `marketplace` 的取值规则

**值域：域名形式的封闭集，当前只有 `'amazon.com'`。** 由 `CHECK` 约束在写入端强制。

现状是同一个概念有三个字面量：`worker/parser.py:1333` 写 `"site": "US"`、
`common/database.py:513` 列默认 `'amazon.com'`、`common/models.py:69` dataclass 默认
`'amazon.com'`。实际落库的永远是 `"US"`——`_default_result` 带了这个键且非空，写入时必定覆盖；
DDL 的默认值只在「插入时不带该列」时生效，而 worker 路径从不如此。

**实现要求：事件流的 `marketplace` 不得透传 parser 的 `site` 字段。**
那个 `"US"` 是硬编码常量，不从任何东西推导（`worker/session.py:45`
`AMAZON_BASE = "https://www.amazon.com"` 同样是无配置项的类常量——系统在结构上就是单站点的）。
应由 relay 从**实际抓取使用的域名**推导并规范化，落在封闭集内，集外的值直接拒绝并告警。

**为什么必须是封闭集而不是自由文本**：`marketplace` 是分组键
`(asin, marketplace, zip_requested)` 的一部分。一旦值发生漂移（一部分行 `US`、
一部分行 `amazon.com`），同一个商品会裂成两组，「取最新值」静默返回错误的价格序列，
且从数据上看不出异常。

沃尔玛侧若需 SP-API 的 marketplace ID，那是消费侧一张二十来行的映射表，不由采集侧承担。

---

## 3. 逐阶段计划

工作量按 1 个熟悉本仓库的工程师估。**旧 SQLite 系统全程不受影响**。

### Phase 0 — 骨架与验证基线（1d）

**黄金样本部分 ✅ 已完成**（PG 环境待 Phase 1 开始时一并起）。

| 项 | 状态 |
|---|---|
| **黄金样本夹具** | ✅ `tests/golden/`，**64 步**覆盖对外全部主要端点，基线在 `samples/sqlite_baseline.json` |
| 场景确定性自检 | ✅ 两次独立运行完全一致 |
| **变异测试**（夹具自身的验收） | ✅ 见下 |
| 依赖与入口 | ✅ `requirements-dev.txt` + `pytest.ini`；`pytest tests/` 45 passed / 4 skipped |
| PG 16 + `scraper` schema + `asyncpg` 池 | ⏳ Phase 1 |

用法与规范化原则见 `tests/golden/README.md`。

**变异测试结论**——抓不到回归的夹具等于没有，所以实测了两种：

| 变异 | 结果 |
|---|---|
| 给 `asin_data` 加一列 | ✅ 捕获 21 处差异。**实锤了审计 §2.17 的判断**：`SELECT d.*` 无 `response_model`，任何新加的列都会泄进 erpAPI 的响应。P1 里「不给 `asin_data` 加列」这条不变式因此是有牙齿的 |
| 静默删掉 `accept_success_result` 的 lease 校验 | ✅ 捕获 45 处差异，头三行直指根因 |

**第二个变异第一次没抓到，暴露的是场景自己的缺陷**（已修）：原来的 stale 测试拿一个
**已 done** 的任务去试，而 lease 校验的 WHERE 同时含 `lease_epoch=?` 与
`status='processing'`——status 条件本身就让 rowcount=0，**lease 校验被完全遮蔽，
那一步从来没测到 lease 门**。现改用一个仍处于 `processing` 的专用探针任务，双向断言：
过期 lease 必须被拒且该 ASIN 查不到（404）；正确 lease 必须被受理
（否则「一律拒绝」也能骗过第一条）。

**顺带修掉一个既有缺陷**：`tests/test_session_slot.py` 无条件把 `httpx`/`aiofiles`
桩进 `sys.modules` 且从不还原，污染整个测试进程，使结果变成收集顺序的函数
（`test_delivery_parse.py` 里那句 `sys.modules.pop("worker.parser")` 就是被它坑过一次
之后的手工绕行）。现改为「真依赖装了就用真的」+ `tearDownModule` 还原；
`unittest discover` 的原有跑法不受影响（47 tests OK）。

### Phase 1 — 存储层移植（5-8d）

`common/database.py` 2486 行 + `server/app.py` 里的直连 SQL。逐项：

> ⚠ **本表有四行已被黄金基线证伪，下面是修订后的版本。**
> 原表写于基线录制之前。以 `common/pgdb/OWNERSHIP.md` 的决策台账为准
> （D-1 / D-2 / D-3），Phase 1 已按修订版实现并通过 64/64 校验。

| 项 | SQLite | PG | 备注 |
|---|---|---|---|
| 占位符 | `?` | `$1, $2, ...` | pgdb 内部仍写 `?`，由 `translate_sql` 统一改写（D-6） |
| 自增主键 | `INTEGER PRIMARY KEY AUTOINCREMENT` | `bigint GENERATED ALWAYS AS IDENTITY` | 烧号行为一致，基线钉死（batch id 1/3、task id 1,3,7,8） |
| upsert | `INSERT OR IGNORE` | `ON CONFLICT DO NOTHING` | 绝不预过滤冲突行，否则烧号偏移 |
| 时间 | `TEXT` `'%Y-%m-%d %H:%M:%S'` | ~~`timestamptz`~~ → **保持 `text`** | **已修正（D-1）**：app.py:487/1168 对该值做 `strptime(x[:19], ...)`，datetime 对象不可下标，会 TypeError |
| 布尔 | `0/1` | ~~`boolean`~~ → **保持 `integer` 0/1** | **已修正（D-1）**：基线里 `/api/batches` 的 `needs_screenshot` 是 int `0`；asyncpg 会把 boolean 列返回成 `True/False`，FastAPI 序列化成 `true/false` |
| 全文搜索 | FTS5 trigram 虚拟表 + 触发器 | `pg_trgm` + GIN | 谓词用 `ascii_lower(x) LIKE ascii_lower(y)`，**不是 ILIKE**（D-5：ILIKE 折全 Unicode，实测 9 处与 SQLite 不一致） |
| 并发控制 | 全局 `_write_lock` + 单写连接 | ~~删除~~ → **Phase 1 保留原样** | **已修正（D-2）**：删掉就得给每个方法单独取连接，而 app.py 那 7 个 `async with db._write_lock: ... db._db.execute('BEGIN')` 块会立刻错乱。真正的写并发是 Phase 1.5（前提：先抽干净 app.py 的裸 SQL） |
| 读写隔离 | 独立只读连接池 | asyncpg 连接池 | 读侧已走池，"重读阻塞写"这个真正的痛点已解决 |
| 维护 | WAL checkpoint / optimize / VACUUM | autovacuum | 四个公开方法保留为 no-op/等价物（app.py 与 harness 都按名字调用） |

**语义必须逐字保持**，包括 lease 校验、变动检测的 baseline 逻辑、
`_is_parse_failure` 的判定（即使它有 bug——移植阶段不修 bug，Phase 4 再修）。

验收：Phase 0 的黄金样本对 PG 版重放**逐字节通过**。

### Phase 1.5 — 移植后立即做的简化（1-2d，可选）

只有在 Phase 1 验收通过后才动：

- `lease_epoch` + `reclaim_dead_worker_tasks` 那一整套 → `SELECT ... FOR UPDATE SKIP LOCKED`。
  这是 PG 原生的任务认领原语，能删掉大量状态机代码。
  **注意**：审计发现现有 reclaim 对「只是慢但还活着」的 worker 也 bump lease，
  导致完整采集结果被丢弃。改用 SKIP LOCKED 顺带修掉这个。
- `/api/results` 每页做全量 `COUNT(*)` → 改 `count(*) OVER ()` 或去掉精确总数。

### Phase 2 — 事件流（3-4d）

| 项 | 内容 |
|---|---|
| DDL | §2.1 全部 |
| 写钩子 | 在结果写入的同一事务内 `INSERT INTO scrape_outbox`。落点对应 SQLite 版的 `_save_result_inner_unlocked`，**必须在「跨时间合并」之前快照**——否则「每条记录 = 一次完整采集结果」不成立 |
| 失败/降级进流 | `outcome ∈ {ok, not_found, blocked, parse_failed, stale}`，全部入 outbox。`outcome<>'ok'` 的 `review_hash` 写 NULL |
| relay | 后台 asyncio 任务；`pg_try_advisory_lock` 单例保护；1s 轮询；批 500；`DELETE ... RETURNING` + `INSERT` 同事务 |
| 哈希 | §4 的完整规格，在 relay 里算（不占提交热路径） |
| 分区维护 | 提前创建未来分区的定时任务 |
| 指标 | outbox 深度、relay 滞后、每分钟事件数 |

验收：并发压测下 relay 输出的 `seq` 严格递增；杀掉 relay 中途重启，outbox 零丢失、`scrape_events` 零重复。

### Phase 3 — 导出 API（2-3d）

`server/api/sync.py`（该文件当前 0 字节、无人 import，是干净落点）：

```python
router = APIRouter(prefix="/api/v1/sync", tags=["sync"],
                   dependencies=[],            # 鉴权留口，暂空
                   include_in_schema=False)    # 保 /openapi.json 不变
```

- `/api/v1` 前缀是承重的：挂在 `/api/results/*` 或 `/api/export/*` 下会被现有 catch-all 吞成 404，
  而消费者会读成「暂无数据」永不推进。
- `db` 必须在调用时惰性解析，不得模块级 `from server.app import db`（循环导入，启动即崩）。

四个端点见 §5。

### Phase 4 — 数据质量（worker 侧，3-4d）

| 项 | 修什么 |
|---|---|
| `zip_observed` | 改用 `worker/ziputil.py` 的 `glow-ingress-line2` 抽取，不再用 `line1`（实测几乎恒返回 None） |
| `zip_verify` | 请求值 vs 观测值的判定结果 |
| `completeness` 位图 | **按 HTML 区块存在性判定**：面包屑区块 / 详情表 / 主图集 |
| 404 分支 | 改为 `outcome='not_found'`，**不写占位符覆盖慢变字段** |
| `manufacturer` 污染 | `_map_detail` 的子串匹配命中 "Manufacturer recommended age"，改精确匹配 |
| set 顺序不定 | `upc_list` / `variation_asins` 的 `set()+join` 全部改为排序输出 |
| `parse_engine` | 记录 selectolax / lxml |
| 四个结转字段 | `rating` / `review_count` / `seller_id` / `seller_name` 补进 `_default_result`，消除 lxml 路径的旧值结转 |
| 时区 | `crawl_time` 改带时区 UTC |
| `site` 值域 | 三处不一致（parser 写 `"US"`、列默认 `'amazon.com'`、dataclass 默认 `'amazon.com'`）统一 |

### Phase 5 — 并行验证 + 切换（3d + 观察）

1. 新 PG 系统与旧 SQLite 系统**同时**采同一批 ASIN（各自独立 worker），比对结果差异。
2. 黄金样本回归（T15）。
3. 事件流对账：`/counts` 的 `count` 与 `scrape_events` 直查一致。
4. 切换：停旧、起新、重采几小时把当前语料灌进去。
5. 观察 48h：relay 滞后、outbox 深度、accept 延迟、磁盘增长速率。

### Phase 6 — 保留期 + ack（2d）

```
floor = max(磁盘应急下界, min(分区时间下界, ack_seq))
```

- **`ack_seq` 初值必须是 NULL 而不是 0**。给 0 的话 `min(时间下界, 0) = 0`，
  保留期永远匹配不到行——看起来实现了，实际一行不裁，直到磁盘满。
- 保留期 = `DROP TABLE <分区>`，前提是该分区的 `max(seq) <= floor`。
- 触发应急裁剪时写持久的 `forced_prune_log`，在 `/status` 上一直返回直到消费者逐条确认。
  **不能只在响应里放瞬时布尔**——消费者宕机正是触发前提。
- `min_available_seq` 永远现算，绝不缓存。

---

## 4. 哈希规格（v1）

### 4.1 字段集

**`review_hash`（复审门）**
```
title, brand, product_type, root_category_id, category_tree,
bullet_points, variant_attributes
```

**`slow_hash`（身份层变化检测，不当门）** = review_hash 全部字段 +
```
manufacturer, model_number, part_number, country_of_origin, is_customized,
long_description, upc_list(排序), image_ids(排序), parent_asin,
package_dimensions, package_weight, item_dimensions, item_weight,
first_available_date
```

**排除，及理由**

| 字段 | 理由 |
|---|---|
| `best_sellers_rank` | Amazon 每小时重算 |
| `variation_asins` | `set()` 顺序跨进程不定；且兜底正则会捞进赞助位/推荐位 ASIN，每次都不同 |
| `rating` / `review_count` | 快变；lxml 路径不赋值，是结转旧值 |
| `seller_id` / `seller_name` | BuyBox 轮换，属快变 |
| 价格 / 库存 / 配送 / `is_fba` | 按定义属 snapshots 层 |
| `ean_list` | 实测 100% 为空 |
| `crawl_time` / `zip_code` / `site` | 采集参数，非商品属性 |
| `screenshot_path` | 内部字段 |

### 4.2 归一化

1. **哨兵值全等匹配**归一为 `null`：
   `"N/A"`、`""`、`"[页面为空]"`、`"[HTML解析失败]"`、`"[验证码拦截]"`、`"[API封锁]"`、`"[商品不存在]"`。
   **绝不能用 `startswith("[")`**——会误伤 `[2-Pack] Storage Bins` 这类真标题。
2. Unicode **NFKC** → 连续空白折叠为单空格 → `strip()` → `casefold()`。
3. **所有列表字段排序**：`upc_list`、`category_ids`、`image_ids`。
4. `image_urls` → **图片 ID**：`.../images/I/71ABC123._AC_SL1500_.jpg` → `71ABC123`（剥尺寸/格式后缀）。
5. `variant_attributes` 解析为 k→v，key `casefold` 后按 key 排序。
6. 序列化：`json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",",":"))`
   → **SHA-256** → 输出 `"v1:<hex>"`。

### 4.3 复审门（消费侧规则，写进契约）

```
需要复审 ⟺ 本条 outcome = 'ok'
        AND 本条 completeness_ok
        AND products 中上一条 completeness_ok
        AND review_hash 与存储值不同
        AND hash_ver 与存储值相同
```

`completeness_ok` = 面包屑区块 & 详情表 & 主图集三位齐全。

**为什么必须是合取**：类目只有面包屑一个数据源，而面包屑正是软降级页会剥掉的区块。
好页 → 降级页 → 好页 = 两次哈希翻转 = 两次误复审。归一化解决不了，
因为 NULL 仍然 ≠ 真值，两个方向都翻。

### 4.4 `hash_ver` 升级策略

**当前处于「还没有 v1」的位置，这是最便宜的时刻——直接上上述规格，不要导出现有的
`content_hash`**（实测跨进程不可复现）。`hash_ver` 留给将来预见不到的变更。

真到升级那天用「过渡期双输出」：采集侧同时输出 v1 和 v2，消费侧继续用 v1 把门、
后台回填 v2，回填完再切。避免全语料一次性复审风暴。

---

## 5. 接口契约

### 5.1 `GET /api/v1/sync/records`

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `after_seq` | int ≥0 | 必填 | **独占**下界 |
| `limit` | int 1..1000 | 200 | 独立上限，不动现有 `/api/results` 的 `le=200` |
| `outcomes` | csv | 全部 | 例 `ok,not_found` |

实现要求：`MIN(seq)` / `MAX(seq)` / 页查询必须在**同一个只读事务**内
（`BEGIN ISOLATION LEVEL REPEATABLE READ`），否则保留期竞态会让 409 守卫失效。
守卫在页查询之后用同一快照的 MIN 复核。

```jsonc
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "min_available_seq": 39120041,
  "max_seq": 41872330,
  "relay_lag_seconds": 0.8,
  "server_time_utc": "2026-08-04T09:12:33Z",
  "after_seq": 41208000,
  "next_after_seq": 41208500,
  "has_more": true,
  "count": 500,
  "retention_forced": false,
  "records": [{
    "source_id": "a3f19c2b7e04:7f3a1c9e-...",
    "seq": 41208001,
    "asin": "B0CXXXXXXX",
    "marketplace": "amazon.com",
    "zip_requested": "10001",
    "zip_observed": "10001",
    "zip_verify": "confirmed",
    "collected_at": "2026-08-04T09:11:02Z",
    "recorded_at":  "2026-08-04T09:11:07Z",
    "outcome": "ok",
    "completeness": 7,
    "error_type": null,
    "batch_id": 8123, "task_id": 5512907, "worker_id": "w-hk-02",
    "attempt": 0, "parse_engine": "selectolax",
    "review_hash": "v1:77ab12…", "slow_hash": "v1:1f0c9a…", "hash_ver": 1,
    "payload": { /* 一次采集的完整结果 */ }
  }]
}
```

| 状态码 | 条件 | 消费者动作 |
|---|---|---|
| 200 + `records: []` | 无新数据 | 正常等待。**本端点永不对空结果返回 404** |
| **409** `cursor_below_retention` | `after_seq + 1 < min_available_seq` | 掉出保留窗口 → 告警 + 全量对账 |
| **409** `cursor_ahead_of_stream` | `after_seq > max_seq` | 疑似恢复/回滚 → 告警 + 全量对账 |
| 422 / 429(+`Retry-After`) / 503 | 参数 / 背压 / relay 停摆 | — |

### 5.2 `GET /api/v1/sync/status`

`gen / instance_id / min_available_seq / max_seq / ack_seq / lag_records /
relay_lag_seconds / outbox_depth / oldest_recorded_at / newest_recorded_at /
retention_forced / forced_prune_log[] / db_size_bytes / free_disk_bytes /
observed_daily_insert_rate`

### 5.3 `GET /api/v1/sync/counts?from_seq=&to_seq=[&bucket=hour]`

用于对账（简报验收标准的「抽样比对无漏采」）。返回 `count / min_seq / max_seq /
min_recorded_at / max_recorded_at`。区间宽度上限 800 万，超出 422。

### 5.4 `POST /api/v1/sync/ack`  `{"gen":"…","ack_seq":41808200}`

单调取 max，永不后退；`gen` 不符返回 409。首次 ack 前保留期按纯时间 + 磁盘执行。

### 5.5 消费侧拉取算法（契约的一部分，不是建议）

```python
st = GET /status
if st.gen != stored_gen:              ALARM("generation changed"); full_reconcile(); STOP
if st.max_seq < stored_max_seq_ever:  ALARM("stream rewound");     full_reconcile(); STOP
if st.forced_prune_log:               ALARM(...)   # 逐条处理并确认
stored_max_seq_ever = max(stored_max_seq_ever, st.max_seq)

X = max(0, stored_cursor - OVERLAP)          # 重叠回拉，宁可重复
while True:
    r = GET /records?after_seq=X&limit=500
    if r.status == 409:  ALARM(r.error); full_reconcile(); STOP
    for rec in r.records:
        INSERT INTO catalog.snapshots (...) ON CONFLICT (source_id) DO NOTHING
        if rec.outcome == 'ok' and rec.completeness_ok:
            UPSERT catalog.products …
              WHERE excluded.seq > products.last_seq        # 单调守卫，按 seq 不按时间
    X = r.next_after_seq
    POST /ack {gen, X}
    if not r.has_more: break
```

**硬性规则（违反即数据错误）**

1. **「同组最新值」一律按 `seq` 排序**，不得用 `recorded_at`，更不得用 `collected_at`。
   时钟前跳/后跳会让时间戳与 seq 非单调。
2. **分组键 = `(asin, marketplace, zip_requested)`**。只按 asin 分组会退化成
   「最近哪个邮编采的」，价格序列在邮编间振荡且无法察觉。
3. **`gen` 变化是硬停**，不是「正常、无需动作」。
4. **绝不把「没有新记录」读成下架/撤回**，也不要发 tombstone。
   采集侧有多条无鉴权删除端点，其中 `DELETE /api/results` 用
   `asin LIKE ? OR title LIKE ? OR brand LIKE ?` 模糊选目标——
   一次手滑会复制成 Postgres 里的大规模墓碑。
5. `outcome != 'ok'` 的记录**只入 snapshots，不触发 products upsert，
   其哈希不参与复审判定**。
6. 复审门是 §4.3 的合取式。占位符进/出**永不触发复审**。

---

## 6. 边界测试清单

| # | 场景 | 期望 |
|---|---|---|
| T1 | 500 ASIN 单批提交，poller 200ms 一次 | 500 条 `recorded_at` 可能相同但 `seq` 各不相同；严格 `seq > X` 一条不漏。**对照组**用 `recorded_at > X` 跑同一测试，必须能观察到丢行 |
| T2 | 10 个并发提交者 + poller 1Hz | 每页 seq 严格升序；抽干后 poller 收到的 source_id 集合 == 表内全集 |
| T3 | **乱序提交专项**：人为让事务 A 拿到小 seq 后延迟提交，B 先提交 | 消费者不得跳过 A。这是本次迁移引入的核心风险，必须有专门用例 |
| T4 | relay 崩溃/重启 | outbox 零丢失，`scrape_events` 零重复（`UNIQUE(source_id)` 不报错） |
| T5 | 双 relay 启动 | 第二个 `pg_try_advisory_lock` 失败并拒绝启动 |
| T6 | 重跑/补采：(a) 同 ASIN 两个邮编；(b) 同名批次重传；(c) 批次 retry | (a) 产生 2 条独立记录；(b) `inserted == 0`、0 条新记录（**写进 runbook：重跑必须换批次名**）；(c) 新 seq + 新 source_id |
| T7 | 重启（提交前 / 提交后各 kill 一次） | 提交前：全回滚，worker 重投产生恰好 1 条；提交后：重投不产生第二条 |
| T8 | 保留期掉窗 | 落在被 DROP 分区里的 `after_seq` 返回 **409**，不得返回「跳过空洞后的下一批」 |
| T9 | ack 初值 | 全新库从不调 `/ack`，保留期必须正常按时间/磁盘裁剪 |
| T10 | 强制裁剪 | `forced_prune_log` 记录并在 `/status` 上持续返回直到确认 |
| T11 | 从备份恢复 | (a) 只回滚 DB：启动检出回退 → 铸新 gen；(b) 整机快照回滚：服务端检不出（预期），必须由消费端 `max_seq` 单调检查捕获。测试须显式验证 (b) 会告警而非静默继续 |
| T12 | 克隆部署 | 两边 `gen` 不同 ⇒ 无 source_id 碰撞 |
| T13 | **erpAPI 无回归** | Phase 0 的黄金样本逐字节通过；`/openapi.json` 单独 diff |
| T14 | 哈希跨进程稳定性 | 同一份 HTML 在 5 个独立 Python 进程里解析，`review_hash` 必须完全相同（现有 `content_hash` 在此测试下必失败） |
| T15 | 慢变字段完整性 | 构造「有 buybox 但无面包屑/无详情表」的降级页：`completeness` 必须标出，该条不得提升到 products。绝不允许好页→降级页→好页造成两次复审 |
| T16 | 时钟步进 | 服务端 NTP 前跳/后跳，「最新值」结论不变（因为按 seq 排序） |
| T17 | 写路径压测 | 峰值持续 1 小时，accept 延迟相对基线上浮 < 15%；outbox 深度不单调增长 |

---

## 7. 工作量汇总

| 阶段 | 工作量 | 阻塞沃尔玛侧？ |
|---|---|---|
| Phase 0 骨架 + 黄金样本 | 1d | — |
| Phase 1 存储层移植 | 5-8d | 是 |
| Phase 1.5 移植后简化（可选） | 1-2d | 否 |
| Phase 2 事件流 + relay | 3-4d | 是 |
| Phase 3 导出 API | 2-3d | 是 |
| Phase 4 数据质量（worker 侧） | 3-4d | 部分（决定复审门何时可用） |
| Phase 5 并行验证 + 切换 | 3d + 观察 | 是 |
| Phase 6 保留期 + ack | 2d | 否 |

**最快解锁沃尔玛侧**：Phase 0 → 1 → 2 → 3 → 5，约 **3 周**。
Phase 4 可与试点并行，Phase 6 上线后补。

---

## 8. 待确认

| # | 事项 | 影响 |
|---|---|---|
| 1 | ~~`marketplace` 值域~~ | **已定**：域名形式，`CHECK (marketplace IN ('amazon.com'))`。规则见 §2.3 |
| 2 | ~~`crawl_time` 改带时区 UTC~~ | **已定**：erpAPI 可接受 |
| 3 | ~~PG 部署形态 / 备份策略~~ | **已定**：与 scraper 同机；中心库即持久副本。资源与恢复边界见 §0.1-0.3 |
| 4 | ~~拉取节奏~~ | **已定**：5 分钟 / 每页 1000 |
| 5 | **`catalog.products` 的主键：`asin` 还是 `(marketplace, asin)`？（需转给沃尔玛侧）** | ASIN 是按站点分配的，同一 ASIN 字符串在不同站点可以是不同商品。单列主键只在单站点前提下安全。**现在做成复合主键成本是零**（值域只有一个值）；等表里堆了几十万行、挂上审核结论与上架资产之后再改主键会很痛 |
| 6 | ~~截图是否需要独立持久化~~ | **已定**：不需要，接受随机器丢失。由此产生「`screenshot_path` 是易失引用」的契约条款（§0.3） |
| 7 | ~~是否开 `synchronous_commit = off`~~ | **已定**：保持默认 `on`。2 核 / 4GB 下 fsync 压力不再突出，仅当 Phase 5 实测确认是瓶颈时再议（§0.1） |
| 8 | erpAPI 端点清单 | 决定 T13 黄金样本的覆盖面；在此之前「零影响」不可测量。**这是唯一还在阻塞验收标准定义的一项** |
