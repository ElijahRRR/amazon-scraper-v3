# amazon-scraper-v3 → WalmartAPI-Contral 增量导出契约：深度审计与落地方案

> 本报告的每一条技术断言都带 `file:line`。凡我实测过的 SQLite 行为，标注「**实测**（sqlite 3.45.1）」。凡无法从本仓库验证的，明确写出「未验证」和验证方法。
> 三份候选设计（同库追加表 / `scrape_events` / `scrape_observations`）我都做了对抗性验证，**其中有 6 条被实测证伪或击穿**，见第 2 节。第 3 节给出的是合并修正后的方案，不是三选一。

---

## 1. 已解决 / 未解决对账

对照简报的五项交付物。**总体完成度：约 15%**——存在的只是"零件"，没有一项是可交付的。

| # | 交付物 | 完成度 | 现状（有什么） | 缺口（缺什么，证据） |
|---|--------|--------|----------------|----------------------|
| 1 | 可靠增量导出：单调游标 + 全局唯一 source_id | **0%** | `iter_results` 已有 keyset 分页范式 `d.id > ? ORDER BY d.id ASC`（common/database.py:2344-2372），`/api/results` 已有 cursor 参数（server/app.py:1751-1766） | **没有任何 append-only 结果表**。`asin_data` 是 `asin TEXT NOT NULL UNIQUE`（common/database.py:477）的每 ASIN 一行 UPSERT：命中走 `UPDATE asin_data SET {fields} WHERE asin = ?`（common/database.py:1937-1940），未命中才 INSERT（common/database.py:1973-1976）。`asin_data.id` 在 UPDATE 分支永不变（`ASIN_DATA_FIELDS` common/database.py:276-290 不含 `id`），**`id > X` 只能拉到"首次见到的 ASIN"，第 N 次采集不产生任何新行**。`asin_changes`（common/database.py:534-543）只存 delta，且 `has_baseline` 为假时一行不写，不含 zip/site/crawl_time。source_id 概念在代码中完全不存在。 |
| 2 | 每条记录 = 一次完整采集结果 | **0%** | worker 提交的 payload 本身是完整的一次采集（server/app.py:1541-1583 直接透传） | 落库时被**跨时间合并**：common/database.py:1906-1912 `val = data.get(f); if val is not None:` —— parser 没产出的键保留上次的值。一行 `asin_data` 可以是「12:00 的 current_price + 三周前的 bullet_points」共用一个 `crawl_time`。从 `asin_data` 读出来的任何东西**在定义上都不是**"一次完整采集结果"。 |
| 3 | 采集参数结构化列 | **形式 30% / 实质 0%** | `site` / `zip_code` / `crawl_time` 确实是真实列（common/database.py:511-513），且在 `ASIN_DATA_FIELDS` 里（common/database.py:287） | (a) 每 ASIN 一行 ⇒ 同 ASIN 在 10001 和 90210 各采一次，**10001 那次直接消失**，不是被覆盖是被删除；(b) `site` 是硬编码常量 `"US"`（worker/parser.py:1333），与列默认值 `'amazon.com'`（common/database.py:513）和 dataclass 默认（common/models.py:67）三处不一致；(c) `zip_code` 存的是**请求值**不是观测值——`_slx_parse_zip_code` 读 `span#glow-ingress-line1`（worker/parser.py:263-273），而仓库自己的 worker/ziputil.py:12 写明邮编在 `glow-ingress-line2`，所以该函数几乎 100% 返回 None，`or zip_code` 兜底生效（worker/parser.py:111）。 |
| 4 | UTC / 带时区，秒精度 | **0%（唯一的采集时间字段是错的）** | 服务端时间戳确实是 UTC：`datetime.utcnow()`（common/database.py:1826、1711） | **唯一被导出的采集时间 `crawl_time` 是裸 UTC+8**：worker/parser.py:13 `_CN_TZ = timezone(timedelta(hours=8))`，worker/parser.py:1332 `datetime.now(_CN_TZ).strftime("%Y-%m-%d %H:%M:%S")` —— `%z` 被刻意丢掉。而 `updated_at` 在 `_INTERNAL_FIELDS` 里（common/models.py:118），**根本不导出**。消费侧拿到的是无标记的 CST，与自己的 UTC 混排就是系统性 8 小时错位。 |
| 5 | 慢变字段哈希 | **存在但不可用，且从未被读过** | `_HASH_FIELDS`（common/database.py:251-259）24 个字段，覆盖 title/brand/category/images/UPC/parent_asin，每次保存都算（common/database.py:1827） | (a) **全仓库无人读 `content_hash`**：`grep` 只有 4 处命中——定义(265)、DDL(517)、赋值(1827)、dataclass 字段(common/models.py:73)，无 SELECT 无比较；且在 `_INTERNAL_FIELDS`（common/models.py:118）里，**不导出**。(b) 含 `best_sellers_rank`（每小时变）和 `variation_asins`（见 §2.9，跨进程不可复现）。(c) 反向漏检：`variant_attributes` 这个真正的慢变身份字段**不在** `_HASH_FIELDS` 里，但在 `ASIN_DATA_FIELDS`(282) 和导出集里。 |

**加分项现状**：唯一被真正消费的哈希是 `title_bullets_hash`（common/database.py:262 只含 title+bullet_points），用于写 `asin_changes`，且 baseline 仅在 `is_auto` 批次刷新——不可作为复审门。

---

## 2. 深挖后新发现的问题

按严重度排序。标 **[实测击穿]** 的是我在对抗验证中真实复现、且**证伪了三份候选设计中至少一份的明文断言**的。

### 2.1 [实测击穿·P0] 脏读 + ROLLBACK ⇒ seq 复用 ⇒ source_id 别名，两道防线同向失效

两个原语，都实测确认（sqlite 3.45.1）：

```
A 插入 5 行后 sqlite_sequence: [('o', 5)]
  事务内 INSERT 拿到 seq=6，ROLLBACK
B ROLLBACK 后再 INSERT，max seq: 6      ← seq 值被【复用】，不是留空洞
```

`ROLLBACK` 会事务性地把 `sqlite_sequence` 回滚，**被中止事务分配的 seq 会被下一个事务重新使用**。三份设计里有两份把这写成"好消息"（"gaps are in fact rarer than one might assume"）——方向搞反了，这是危险放大器。

组合链条（可复现）：
1. 消费者游标 = 3。
2. `accept_results_batch` 持锁、`BEGIN IMMEDIATE`（common/database.py:1716），插入 5 条 observation（seq 4-8）。
3. 拉取查询若落到**写连接**上，就是脏读——实测：写连接看得见自己未提交的行，独立只读连接看不见。
4. 消费者持久化 `g:004..g:008`，游标推进到 8。
5. 事务 ROLLBACK（common/database.py:1801-1805，可由磁盘满 SQLITE_FULL / FTS5 触发器异常 / busy_timeout 5s 超时触发）。
6. 下一批**真实**的 5 条记录拿到 seq 4-8 → `seq > 8` 永远拉不到 → 且 source_id 与已消费的字节相同 → 消费侧 `ON CONFLICT (source_id) DO NOTHING` **静默丢弃真实数据**。

**为什么这条在本仓库是可达的，不是理论**：
- `read()` 有静默回退到写连接的分支：common/database.py:363-364 `if self._read_pool is None: yield self._db; return`；而 `close()` 在 common/database.py:419-421 先把 `_read_pool = None`，**之后**才 `await self._db.close()`——lifespan 里创建、关停时未 await 的后台任务能撞上。
- 仓库里已经有 6 处"在写连接上、锁外做 SELECT"的现成范式（server/app.py:1295, 2227, 2278, 2286, 2291, 2306），下一个维护者照抄的概率很高。
- 三份设计都刻意**不给 source_id 建 UNIQUE 索引**（"uniqueness is proved, not enforced"，为省 ~20% WAL）。而这个场景下 UNIQUE 索引**也救不了**——第一行从未提交，索引里没有它。

**后果**：静默、永久、无告警的丢数据 + 消费侧去重反向帮凶。

**修法见 §3**：(a) 同步读路径物理禁止触碰 `self._db`；(b) source_id 加入**每行随机后缀**，使 seq 复用产生不同 source_id，把"静默丢弃"降级为"可见重复"。

### 2.2 [实测击穿·P0] `min_available_seq` 与页查询不在同一快照 ⇒ 保留期守卫可被绕过

只读连接以 `isolation_level=None` 打开且从不 `BEGIN`（common/database.py:346-355），**每条语句是独立读事务**。一次拉取响应至少 3 条语句（MIN / MAX / page）。竞态：

```
handler: SELECT MIN(seq) → 1        （守卫 after_seq=100 通过）
retention: DELETE WHERE seq <= 200  （提交）
handler: SELECT ... WHERE seq > 100 LIMIT 200 → 返回 201 起
响应: min_available_seq=1，rows 从 201 开始，无 409
```
消费者看不到任何信号，游标推过去，100 条永久丢失。三份设计里，`min_available_seq` 被一致称为"non-negotiable"、"唯一能区分'没有新数据'和'数据被裁掉'的字段"——**这个竞态正好让它失效**。

补充实测：`PRAGMA query_only=ON` 的连接**可以** `BEGIN`，能跨语句保持稳定快照，WAL 下不阻塞写连接。所以修法成立且便宜（§3.5）。

### 2.3 [实测击穿·P0] `executescript` 会隐式提交——正好是保留期任务要用的那个调用

实测：

```
E in_transaction before executescript: True
E in_transaction after  executescript: False
E other conn sees rows: (1,)          ← 未提交的行被发布了
```

审计正确地发现 `PRAGMA incremental_vacuum` 走 `execute()` 是静默 no-op，必须 `executescript`。但**开出的药更毒**：一旦有人把「分块 DELETE + 更新 `min_available_seq` 记账」包进 BEGIN 保证原子（这是必须做的），中间那句 `executescript` 会静默提交，随后的 `COMMIT` 抛 "cannot commit - no transaction is active"，被 `maintenance_loop` 的 `except Exception` 吞掉（common/database.py:396-400）——**DELETE 落地了，记账没落地**，直接喂给 §2.2 的静默丢失。

更阴的一点：这是全进程唯一能解开「泄漏事务楔死」的语句，而它解开的方式是**发布半批数据**（10 条一批只处理了 k 条）。

### 2.4 [实测击穿·P0] AUTOINCREMENT 重置的真实触发条件——三份设计都写错了

三份设计都断言"实测 `DELETE FROM sqlite_sequence` 后下一个 seq = 1"。**错**。实测：

| 场景 | 下一个 seq |
|------|-----------|
| 表内 5 行，`DELETE FROM sqlite_sequence` | **7**（= max(rowid)+1，从表内重算） |
| 表清空 + `DELETE FROM sqlite_sequence` | **1** |
| 只 `DELETE FROM 表`（sqlite_sequence 保留） | 6（计数器存活） |

结论修正三点：
1. server/app.py:2654 的无 WHERE `DELETE FROM sqlite_sequence` 对**有行存活的**新表**不致命**——真正被打回 1 的是 loop 里被清空的那六张表。
2. **AUTOINCREMENT 在这里买不到设计声称的东西**。设计说"AUTOINCREMENT 保证 DELETE 后 id 不复用"——那只对尾部删除成立；保留期是**头部裁剪**，`max(rowid)` 不降，普通 `INTEGER PRIMARY KEY` 行为完全一致。真正在兜底的是 `sqlite_sequence` 那一行 + 表里的最大 rowid。
3. **真正的杀伤窗口是"表为空 + 计数器丢失"**：新部署、保留期扫空、磁盘应急裁剪之后。此时进程**不重启**，方案一的"每 boot 换 gen"不触发，新 seq=1 的行与已消费的 `g:000000000001` 字节相同 → 静默丢弃。

**方案三的"无条件修复"正是这个 bug 的现成触发器**：它写 `hw = COALESCE(MAX(seq),0)` 然后 `DELETE + INSERT INTO sqlite_sequence`。表被裁空时 `MAX(seq)=0`，计数器被**写低**到 0，而且它就跟在自己的 fast-forward 后面执行，把自己的安全措施抹掉。实测确认 SQLite 认这个降低后的值。

### 2.5 [P0] ack 门控的初值让保留期整体变成 no-op ⇒ 磁盘打满 ⇒ erpAPI 一起挂

三份设计一致：裁剪下界 = `min(age_floor, ack_seq)`，且 `ack_seq` 初始化为 `'0'`，且都明说"消费者可以只拉不 ack"。
⇒ 默认配置下 `floor = 0`，DELETE 匹配不到任何行，**保留期看起来实现了，实际一行都不裁**。
在 20GB 磁盘（README.md:24）+ 已有 2.4GB 库（README.md:568）+ 同卷的 `server/static/screenshots`、`data/exports` 上，这是确定性的磁盘打满。磁盘满 → `asin_data` 写不进 → 导出挂 → 仪表盘挂 → **erpAPI 挂**。这比裁数据严重得多，而且是安全机制自己造成的。

### 2.6 [P0] 容量假设差 4 倍——保留期天数是不可证伪的猜测

三份设计都按「290k 条/天」（= 现有语料全量重采一次，README.md:558）× ~2.1 KB/条 = 0.61 GB/天 定保留窗口。
但 README.md:551 记录的实测峰值是 **3000-5000 ASIN/min（60-83 ASIN/s）**，README.md:555 说 30k 批次 ~9 分钟跑完。**一天跑 4 小时峰值 = 1.2M 条/天 ≈ 2.4 GB/天**，是假设的 4 倍。方案一的 10-14 天窗口要 24-34 GB，方案三的 10 天要 24 GB —— **都超过整块 20GB 磁盘**。

⇒ 保留期**不能用天数表达**，必须用「剩余磁盘下限 + 行数/字节硬上限」驱动，并把观测到的日均写入速率暴露在 `/status` 上。

**上线前必须先测的一个数**（决定同库方案是否成立）：
```sql
SELECT COUNT(*) FROM tasks WHERE status='done' AND updated_at > datetime('now','-1 day');
```

### 2.7 [P0] 无鉴权、无 lease 的写入路径 = 游标注入 + 保留期放大器

两条路径接受**没有 task_id、因而不过 lease 门**的结果写入：
- `POST /api/tasks/result` 无 task_id → server/app.py:1536-1538 → `save_result`（common/database.py:1987-2006，直接 BEGIN/写/COMMIT，无任何校验）
- `POST /api/tasks/result/batch` 的 `if not task_id:` 分支（common/database.py:1727-1734）

全仓库无鉴权（`grep add_middleware|Depends(|Header(` 在 server/app.py 无命中，实测），`SERVER_HOST = "0.0.0.0"` 是硬编码字面量（common/config.py:34，**没有 env 覆盖**，与下一行可配的 `SERVER_PORT` 对比明显）。任何能打到 :8899 的对端可以：(a) 注入伪造记录，带合法 seq 和合法 source_id，下游无法区分；(b) 灌水把保留期下界推过 catalog_sync 还没拉的记录。新加的鉴权只在**拉取**路由上，喂游标的**写入侧按设计保持敞开**（加上就会打断所有 worker）。

### 2.8 [P1] 泄漏事务楔死，且 `except BaseException` 不是解药

所有写路径的 `BEGIN` 都在 `try` **外面**（common/database.py:1664、1716、1996、1339），handler 是 `except Exception:`（1695、1801、2001），接不住 `asyncio.CancelledError`（BaseException）。一次取消 → 锁释放但事务开着 → 后续每个写者在自己的 `BEGIN` 上死于 "cannot start a transaction within a transaction"，而该异常因为 `BEGIN` 在 try 外**永远到不了 ROLLBACK**，自我延续直到进程重启。

**但设计给的修法不成立**：我读了实际 handler，body 是 `try: await self._db.execute("ROLLBACK") except Exception: pass`，且都以裸 `raise` 结尾（所以 `BaseException` 不会吞掉取消，那个子担忧不成立）。问题在于**在 CancelledError 处理中 `await` 一个 aiosqlite 跨线程往返本身不可靠**——再次投递取消时 ROLLBACK 根本到不了连接。

正确修法是**在事务开始处防御**：`BEGIN` 前检查 `in_transaction`，为真则先 ROLLBACK，再 BEGIN，整体在 `try` 内。这把"永久楔死"降级为"单请求失败"，且不依赖取消后的清理能跑完。

对验收的影响：楔死期间游标完全不动，一周验收会读成"多小时数据缺失"，误判为丢数据。

### 2.9 [实测击穿·P1] `content_hash` 三重不稳定 —— 跨 worker 进程不可复现

实测：把 worker/parser.py:1563-1569 的 `set(re.findall(...))` + `",".join(list(...))` 管道在 5 个独立 Python 进程里跑**同一份输入**，得到 5 种不同顺序（`B00E,B00D,B00C,B00F` / `B00D,B00F,B00C,B00E` / ...）。CPython 逐进程随机化 str hash，**两个 worker 采同一个没变的页面会算出不同的 `content_hash`**。同样的 `set()+join` 模式还在 `_slx_parse_upc`（worker/parser.py:738-746）和 `_parse_ean`（1549-1554）—— UPC 是要复用到 Walmart 上架资产的字段。

叠加：
- `_parse_twister` 只在 `dimensionValuesDisplayData` 存在时返回真变体族（worker/parser.py:1610-1626），**绝大多数单变体页**落到全页正则 `"asin":"(\w+)"` 兜底，把赞助位、"猜你喜欢"轮播的 ASIN 全捞进来——这些是个性化/AB 测试驱动的，每次抓都不同。
- `best_sellers_rank` 在 `_HASH_FIELDS` 里（common/database.py:253），Amazon 每小时重算。

⇒ **v1 哈希作为复审门 = 100% 误判重审**。方案三"照原样导出 v1 + 文档里写不要用作门"等于不交付需求 5。

**反向漏检**：`variant_attributes` 不在 `_HASH_FIELDS`，颜色/尺码真变了不触发复审。方案二的窄 `review_hash {title,brand,category_tree,bullet_points,product_type}` 同样漏。

### 2.10 [P1] `manufacturer` 被 "Manufacturer recommended age" 污染，且是 last-write-wins

`_slx_parse_all_details` 迭代 `tree.css('tr')`——**全文档所有表格行**，含"Compare with similar items"和 A+ 对比表（worker/parser.py:770-820），`elif len(tds) >= 2` 分支接受任何 key ≤50 字符的两列行。`_map_detail`（worker/parser.py:1667-1697）用子串匹配 + 无条件覆盖：`elif 'manufacturer' in k_lower` 命中极常见的 **"Manufacturer recommended age"**，`manufacturer` 变成 "3 years and up"。谁赢取决于文档顺序，随页面模板/AB 变化 ⇒ 同一个没变的商品在 `Acme` ↔ `3 years and up` 之间来回翻，每翻一次两次误复审。

（`'part number'` 在 1673 先于 `'manufacturer'` 1679 判断，所以 "Manufacturer Part Number" 落位正确——年龄段这条才是活的。）

### 2.11 [P1] 归一化救不了哈希：占位符是结构性问题

`_slx_parse_categories` 只读 `div#wayfinding-breadcrumbs_feature_div`，任何异常返回 `("N/A","","")`（worker/parser.py:748-768），**没有第二数据源**。而面包屑正是 Amazon 软降级页会剥掉的个性化区块。worker 的降级门（worker/engine.py:1279-1292）要求 price/buybox/stock/brand **四个全空**才判降级，有价格就放行。
⇒ 好页 → 降级页（`category_tree=''`）→ 好页 = **两次哈希翻转 = 两次误复审**。方案一提出的"把占位符映射到统一 NULL token"证明无效：NULL 仍然 ≠ 真值，两个方向都翻。

**复审门必须是合取式，不是哈希比较**：`hash 变了 AND 本条通过完整性检查 AND 对比的上一条也通过`。⇒ 完整性信号（scrape_status / field_completeness，按 **HTML 是否存在该区块**判定，不按解析值判定）是需求 5 的**前置条件**，不是可选后续。

另：方案一提的"把任何 `[...]` 括号哨兵映射为 NULL"会误伤 `[2-Pack] Storage Bins` 这类真实标题。哨兵集合是**封闭可枚举**的：`[页面为空]`(worker/parser.py:70)、`[HTML解析失败]`(89/830)、`[验证码拦截]`(1378)、`[API封锁]`(1381)、`[商品不存在]`(worker/engine.py:1172)。必须全等匹配。顺带：`_is_parse_failure` 自己就用 `not title.startswith("[")`（common/database.py:186），已经会误判真括号标题。

### 2.12 [P1] lease 门丢弃的是**完整采集结果**，不是重复提交

`reclaim_dead_worker_tasks`（common/database.py:1246-1281）对**纯硬超时**也 bump `lease_epoch`（`updated_at < hard_cutoff`，不只是 worker 死掉）。一个仅仅**慢**但活着的 worker 因此丢租约；提交时 `UPDATE tasks ... WHERE ... lease_epoch=? AND status='processing'`（common/database.py:1739-1743）rowcount=0 → `stale += 1; continue` → **整个已解析的 payload 扔掉**。

设计把这描述成"免费的幂等性"，混淆了两件事：HTTP 层同 payload 重投（该抑制，抑制对了）vs **被顶替的 worker 的一次真实、不同的抓取**（被静默销毁）。这与简报"不要在采集侧去重"直接冲突。必须二选一并写进契约：要么给 stale 也写一条 `outcome='stale'` 的记录，要么把契约收窄成"记录的是**被受理的**结果，不是抓取"，并把 stale 率暴露出来。

### 2.13 [P1] 方案一的 hook 会导致**启动循环导入**（全站 500）

方案一写「`_build_sync_record` 是 sync.py 里的 helper，由 app.py 导入」，同时 router 从 app.py 底部 `include_router`。但它**没说 sync.py 怎么拿到 `db`**——`db` 是 server/app.py:40 的模块级全局，在 lifespan(157) 里赋值。最自然的写法 `from server.app import db` 是循环导入，启动即崩，且即便解析成功也永久捕获 `None`。这不是功能降级，是**所有 erpAPI 端点全挂**。
（方案三处理了：调用时 `from server import app as _srv; db = _srv.db`。方案二没说。）

### 2.14 [P1] `/openapi.json` 会变——"零个既有响应体变化"的断言是假的

`grep docs_url|openapi_url|redoc` 在 server/app.py **无命中**，server/app.py:181 `app = FastAPI(title=..., version="3.0.0", lifespan=lifespan)` 没关文档。`app.include_router(...)` 会把新路径写进生成的 schema ⇒ `GET /openapi.json`（一个既有的、无鉴权可达的端点）**响应体改变**。三份设计都断言"not one existing response body changes"，这句话按字面是错的。
严重性取决于 erpAPI 是否对 schema 做 codegen/校验——**本仓库无法判定**（无 client 代码、无 OpenAPI 快照、tests/ 只有 test_ziputil.py / test_delivery_parse.py / test_session_slot.py，无任何 HTTP 层测试）。
修法：新 router 加 `include_in_schema=False`，或明确记录并与 erpAPI 侧确认。

### 2.15 [P2] 一次瞬时 404 抹掉整行慢变字段

worker/engine.py:1167-1174：`session.is_404(resp)` 时构造 `_default_result` + `title="[商品不存在]"`，`success=True` 提交，**无重试无换 IP**。服务端守卫拦不住：`_is_parse_failure`（common/database.py:181-195）检查 `["current_price","buybox_price","stock_count","stock_status","brand"]` 是否全在 `_NA_VALUES`（common/database.py:168），而 `stock_count` 默认是字符串 `"0"`（worker/parser.py:1353），**`"0"` 不在 `_NA_VALUES`** ⇒ `all_empty=False` ⇒ 落库。然后 common/database.py:1906-1912 的 `is not None` 判断让每个 `"N/A"` 覆盖掉原来的好值。title/brand/category/images/UPC 全没，两个哈希全翻。因为没有历史，**采集侧不可恢复**。

### 2.16 [P2] 重跑同名批次是静默 no-op —— 而这正是运维发现缺口后第一反应

`create_batch` 用 `INSERT OR IGNORE INTO batches` 然后把已存在的 id 查回来（common/database.py:815-822）；`create_tasks` 用 `INSERT OR IGNORE INTO tasks`（common/database.py:1120-1123）对 `UNIQUE(batch_id, asin)`（common/database.py:565）。重传同名批次 = 插入 0 个任务 = 0 次采集 = 0 条记录，HTTP 200。三份设计都断言"有了追加表以后补采天然正确"——**前提是真的产生了采集**，而这个前提不成立。

### 2.17 [P2] 其余需在实施时一并处理的

| 项 | 证据 | 影响 |
|---|---|---|
| `read()` 静默回退写连接 | common/database.py:363-364；`close()` 在 419-421 先置 `_read_pool=None` 后关 `_db` | §2.1 的可达路径 |
| `/api/results` 每页做全量 `COUNT(*)` | common/database.py:2257-2259；`limit=Query(50, le=200)`（server/app.py:1754） | 不能复用为批量拉取；改 `le` 是既有端点行为变更，禁止 |
| `SELECT d.*` 无 response_model | common/database.py:2229-2235、2272-2276 | **任何加到 `asin_data` 的列都会泄进 erpAPI 响应**；`EXPORTABLE_FIELDS` 由 dataclass 推导（common/models.py:119），加 dataclass 字段会改 xlsx 列集和 `/api/export/fields` |
| 读池只有 3 条，`iter_results` 整场导出占 1 条 | common/database.py:313、2344-2372；`read()` 无超时（360-370） | 拉取会排在导出后面无限等；应给 sync 独立连接 + 超时 |
| 路由遮蔽 | server/app.py:1769 `/api/results/{asin}`、1952 `/api/export/{batch_name}` 都是抛 404 的 catch-all，Starlette 按注册顺序匹配 | 底部 `include_router` 若挂在这两个前缀下会被吞成 404，naive 消费者读成"暂无数据"永不推进 |
| `screenshot_path` 回填不 bump `updated_at` | common/database.py:2311-2314（`UPDATE asin_data SET screenshot_path=? WHERE asin=?`） | 截图后到的记录，导出里永久没有截图路径 |
| WAL 增长未被任何设计建模 | `wal_autocheckpoint=1000`(≈4MB, common/database.py:322)、`journal_size_limit=64MB`(333)；server/app.py:285-287 自述"固定 TRUNCATE 触发 ~200-400ms commit 抖动" | 三份设计只算持锁毫秒，**没算 WAL 字节和 checkpoint 频率**——而仓库已有 checkpoint 饥饿的历史 |
| 无任何备份机制 | 全仓 `grep backup\|\.dump\|rsync\|cron`：只有 deploy/setup.sh:18 的代码 rsync，且 `--exclude='data/*.db'` | 恢复危害只在有备份时可达；但没有备份的替代失败（全丢）更糟 |

---

## 3. 推荐方案

**同库单表 + 每 boot 轮换的 gen + 每行随机后缀的 source_id + 压缩 payload + 零二级索引（除时间桶索引外）+ 磁盘驱动的保留期。**
= 方案一的原子性论证 + 方案二的极简手术面 + 方案三的 409/digest 运维面 + 第 2 节六条实测修正。

### 3.1 为什么必须同库（不是 ATTACH，不是第二连接）

DB 无条件 WAL（common/database.py:319）。SQLite 明确规定：**WAL 模式下跨文件事务不是原子的**（对抗验证复现：rollback-journal + rollback-journal 会产生 super-journal `m.db-mj*`，WAL+WAL 不产生）。ATTACH 买到的是"一个事务"的语法，不是保证：崩溃可能让 `tasks.status='done'` 落地而 observation 丢失，**且没有任何队列条目可以重放**。同库是唯一能让"从游标 X 拉取绝不漏"成为可证命题的选择。

代价（诚实记账）：`auto_vacuum=INCREMENTAL` 只能在**建库时**决定，现在改需要一次完整 `VACUUM`（实测：WAL 库上 `PRAGMA auto_vacuum=INCREMENTAL; VACUUM;` 有效，auto_vacuum 0→2，journal_mode 保持 wal）。必须**趁库还是 2.4GB 时**在维护窗口做，且**必须记为计划停机**——`uvicorn.run` 只传了 host/port/log_level（server/app.py:2670-2675），没有任何超时参数，VACUUM 的独占锁期间 erpAPI 请求是无限排队而不是快速失败。方案一把它折进"纯追加、零影响"的变更里是错的。

### 3.2 DDL

追加到 `init_tables` 的 `executescript` 块（common/database.py:439-607）末尾，全部 `IF NOT EXISTS`，**不 ALTER 任何既有表**：

```sql
-- ============ 增量导出事件流（catalog_sync 消费）============
-- 只追加，永不 UPDATE，永不按内容去重。每行 = 一次完整采集结果。
-- 唯一的 DELETE 来自「按时间 + 磁盘 + ack」三重下界的保留期任务（头部裁剪）。
CREATE TABLE IF NOT EXISTS collection_records (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,  -- 游标
    gen            TEXT    NOT NULL,   -- 每次启动新铸的实例代号，逐行落库
    rid            TEXT    NOT NULL,   -- 每行 6 hex 随机后缀（见 §3.4，防 seq 复用别名）
    asin           TEXT    NOT NULL,   -- 写入时统一大写

    -- 采集参数（需求 3：结构化列）
    marketplace    TEXT    NOT NULL,   -- 规范化常量 'amazon.com'，不透传 site
    zip_requested  TEXT    NOT NULL,   -- 5 位补零，取 worker 真正 POST 的 target_zip
    zip_observed   TEXT,               -- 页面 glow-line2 抽出的邮编；Phase 5 前恒为 NULL
    zip_verify     TEXT    NOT NULL,   -- 'confirmed'|'assumed'|'mismatch'|'unverified'

    -- 时间（需求 4：UTC + 显式标记 + 秒精度）
    collected_at   TEXT    NOT NULL,   -- worker crawl_time(+08:00) 换算为 RFC3339 'Z'；仅供参考
    recorded_at    TEXT    NOT NULL,   -- 服务端时钟，RFC3339 'Z'；排序/分桶的唯一权威时间

    -- 质量 / 溯源
    outcome        TEXT    NOT NULL,   -- 'ok'|'not_found'|'rejected'|'failed'|'stale'
    error_type     TEXT,
    error_detail   TEXT,
    batch_id       INTEGER,
    task_id        INTEGER,
    worker_id      TEXT,
    attempt        INTEGER NOT NULL DEFAULT 0,
    parse_engine   TEXT,               -- 'selectolax'|'lxml'|NULL（Phase 5 前 NULL）

    -- 慢变哈希（需求 5）
    slow_hash      TEXT,               -- 'sha256:<hex>'；outcome!='ok' 时写 NULL
    review_hash    TEXT,               -- 窄哈希，复审门专用
    hash_ver       INTEGER NOT NULL DEFAULT 1,

    -- 对账：sha256(source_id || payload) 低 40 位。SUM() 顺序无关，int64 内精确
    row_digest     INTEGER NOT NULL,

    payload_enc    TEXT    NOT NULL,   -- 'zlib1+json'
    payload        BLOB    NOT NULL    -- 【必须声明在最后】见下
);

-- payload 声明在最后是有意的：SQLite 行是一个连续 cell，尾部溢出到 overflow page。
-- 把唯一的大列放最后，/counts 的范围扫描和 MIN/MAX 只读 local cell，不碰 overflow。

-- 唯一的二级索引，追加有序（每次插入只弄脏最右叶子 ≈1 页）。服务 /counts 与保留期扫描。
CREATE INDEX IF NOT EXISTS idx_cr_recorded ON collection_records(recorded_at);

-- 【故意不建】asin 索引（消费者只按 seq 过滤）；source_id 的 UNIQUE 索引
-- （它对 §2.1 的复用别名根本无效——未提交的行不在索引里；真正的防线是 rid + 只读连接）。

CREATE TABLE IF NOT EXISTS sync_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
-- 键：contract_version / gen / instance_id / hwm_seq / ack_seq / ack_at
--     / retention_forced / forced_prune_log

-- 【哨兵行】保留期永不裁 seq=1。作用见 §2.4：max(rowid) 永不塌陷，
-- 于是 sqlite_sequence 即使被 DELETE，AUTOINCREMENT 也能从表内重算出正确值（实测）。
INSERT OR IGNORE INTO collection_records
  (seq,gen,rid,asin,marketplace,zip_requested,zip_verify,
   collected_at,recorded_at,outcome,row_digest,payload_enc,payload)
VALUES (1,'sentinel','000000','SENTINEL','amazon.com','00000','unverified',
        '1970-01-01T00:00:00Z','1970-01-01T00:00:00Z','sentinel',0,'none',x'');
```

**一次性前置（维护窗口，趁库 2.4GB）**：`PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`

### 3.3 写入挂载点（精确到函数 + 行）

| # | 位置 | 做什么 |
|---|------|--------|
| H1 | **server/app.py:1541-1583 `api_submit_batch`** 的 item 循环内（1546-1563），`db.accept_results_batch` 调用（1566）**之前** | 在**锁外**做：`json.dumps` + `zlib.compress(...,1)`（实测 154 µs/行）、`slow_hash`/`review_hash`/`row_digest` 计算，结果塞进 `item["_rec_blob"]` / `_rec_task_id` 等下划线键。**必须在锁外**：zlib 释放 GIL 但 `_write_lock` 是 `asyncio.Lock`，锁内做 = 1 vCPU 上 +20% 持锁时间，`pull_tasks` 排在后面 |
| H2 | **server/app.py:1516-1538 `api_submit_result`**，1526 与 1536 之前 | 同上 |
| H3 | **common/database.py:1816 `_save_result_inner_unlocked`**，在 1828（两个 hash 赋值）之后、1840（`_get_done_screenshot_path`）与 1842-1849（`SELECT ... FROM asin_data`）**之前**插入一条 `INSERT INTO collection_records`，外加 `record_stage("save_record", ...)` | 位置是硬要求：从 1904 起是 `if val is not None` 的跨时间合并（1906-1912）。**在合并前快照**才让"每条记录 = 一次完整采集结果"字面成立。放在函数内部（而非各调用点）还保证了 1821-1822 的 `asin` 空值早退同时跳过记录写入 |
| H4 | 调用点透传 `task_id`：common/database.py:1691（`accept_success_result`）、1730（batch 无 task_id 分支）、1761（batch 主路径）、1998（`save_result`） | 全部已在 `BEGIN..COMMIT` 内 |
| H5 | `outcome != 'ok'` 的记录：common/database.py:1682-1688 与 1750-1757（`server_reject`）、1783-1789 与 1356-1360（终态 failed）、1745-1747（`stale`，见 §2.12） | 只在**终态**写，requeue 分支不写 ⇒ 每次终态尝试恰好一条 |

`_`-前缀键不会落进 `asin_data`：写入侧遍历 `ASIN_DATA_FIELDS`（common/database.py:1906、1944），未知键天然丢弃——`batch_name`、`worker_id`、`_page_asin` 今天就是这么走的（worker/parser.py:1336 注释已写明这个约定）。

### 3.4 游标保证（可测试的一句话 + 证明骨架）

> **保证**：对固定的 `gen`，若消费者持久化 X = 它见过的最大 `seq` 且总是请求 `after_seq = X`，则任何在后续请求时刻**已提交**且 `seq > X` 的行，必定在该次或之后某次请求中被返回，且按 `seq` 严格递增。绝不跳过已提交行。重复返回可能发生，`source_id` 使其无害。`gen` 变化或 `X + 1 < min_available_seq` 时保证失效（服务端返回 409）。

**证明骨架（四环，缺一即断）：**

| 环 | 证据 | 状态 |
|---|------|------|
| 1 单写连接 | `self._db` 仅在 common/database.py:318 创建一次；全仓另一处 `aiosqlite.connect` 是读池 common/database.py:348，每条都 `PRAGMA query_only=ON`（349），**物理上分配不了 rowid** | 实测 grep 确认只有这 2 处 |
| 2 单全局互斥 | `TimedLock.__init__` 只持一把 `asyncio.Lock`（common/database.py:132-135）；命名 `__call__`(138-139) 与裸 `__aenter__`(143) 都汇入 `_do_enter`(149-156) 取同一把锁——"命名"只是指标标签。每 Database 一个实例（306） | 我逐条枚举了 common/database.py 全部 17 处 BEGIN（813, 963, 1016, 1071, 1117, 1167, 1258, 1306, 1339, 1400, 1415, 1469, 1519, 1664, 1716, 1996, 2303），**每一处都在 `async with ... _write_lock` 内** |
| 3 单进程 | server/app.py:2670-2675 `uvicorn.run` 无 `workers=`；deploy/server.service 单 ExecStart；server/app.py:157 `db = Database()` 仅一处 | 需转为**断言**：`connect()` 里对 `f"{db_path}.writer.lock"` 取 `fcntl.flock(LOCK_EX|LOCK_NB)`，失败拒绝启动 |
| 4 读者不脏读 | 读连接独立、WAL、`isolation_level=None` 且从不 BEGIN（common/database.py:346-355），每条语句自带已提交快照 | **必须补一刀**：删掉/禁用 common/database.py:363-364 的写连接回退；sync 路径直接从 `db._read_pool` 借，借不到返回 503 |

1+2+3 ⇒ 任意时刻至多一个写事务打开 ⇒ 分配 seq=M 的事务只能在分配 M-1 的事务提交后才开始 ⇒ **rowid 顺序即提交顺序**，观测到 M 就证明 M-1 已持久。

**必须同时写进契约的两条注意**（三份设计都写反了其中一条）：
- **seq 空洞无害**：`seq > X ORDER BY seq` 不依赖连续性。绝不可把空洞读成丢数据。
- **被中止事务的 seq 会被复用**（§2.1 实测）。所以 **seq 在其事务提交前毫无意义，任何组件不得发布、记录、ack 或导出未提交的 seq**。这是可测试的不变式，不是一句叮嘱。

**关于 flock 的诚实边界**：它拦得住 `uvicorn --workers 2`、拦得住第二个 `run_server.py`（实测 flock 对同进程的第二个 fd 也生效），**拦不住** `sqlite3 data/scraper.db` 或备份/迁移脚本。补充说明：对抗验证显示，多进程写**不会**造成乱序提交——WAL 下 SQLite 自己把写事务串行化（实测：慢写者持事务 1.5s，另一进程 `BEGIN IMMEDIATE` 阻塞 1.23s 后才拿到 seq=2；先读后写的 deferred 事务则抛 `database is locked`/BUSY_SNAPSHOT）。所以 flock 保护的是**可用性**和保留期/gen/计数器修复这三件真正单写依赖的事，**不是**顺序性证明。这个区分很重要：如果把证明基础写成"单进程"，第一个想做水平扩展的人会去掉它，然后打爆的是保留期机制。

### 3.5 source_id 定义

```
source_id = f"{gen}:{seq:012d}:{rid}"
例：a3f19c2b7e04:000000041208001:9f2c1b
```

| 组件 | 生成 | 作用 |
|---|---|---|
| `gen` | 每次**启动**新铸 `uuid4().hex[:12]`，与必填环境变量 `SYNC_INSTANCE_ID` 混合，**逐行落库**（不在读取时派生） | 隔离克隆部署、恢复、重置。逐行存储是关键：只存在 `sync_meta` 的话，一次恢复会把全部历史重贴上恢复后的标签 |
| `seq` | AUTOINCREMENT PK | 游标 |
| `rid` | 插入时 `os.urandom(3).hex()`，落库 | **§2.1 的解药**：即使 seq 因 ROLLBACK 被复用，新行的 source_id 也不同 ⇒ 消费侧不会静默丢弃，而是插入一条新记录（它确实是新记录）；异常变成"可见的重复"而非"不可见的丢失" |

**为什么不用其他候选**（都在本仓库验证过）：
- `tasks.id`：重试是 UPDATE 同一行（common/database.py:1364-1369、1308-1318、server/app.py:1262-1276），`UNIQUE(batch_id, asin)`（common/database.py:565）保证一个工作项一行 ⇒ 一个 id 覆盖 N 次采集。
- `asin_data.id`：首次 INSERT 分配后永不变（UPDATE 分支 common/database.py:1937-1940 的字段来自 `ASIN_DATA_FIELDS`，不含 id）。
- 纯 uuid4 + UNIQUE 索引：随机值散布索引，实测 WAL 从 3409 → 8611 B/行，热路径代价翻倍，且对 §2.1 无效。

**`source_id` 承诺的边界（必须写进契约）**：它保证**传输幂等**（重拉任意 seq 区间免费），**不保证采集身份**——`save_result` 无 lease 路径（common/database.py:1987-2006）上，worker 的 HTTP 重投会产生两条不同 seq 的记录（§2.7）。若确实需要采集身份，需 worker 侧生成 `fetch_id`（每次页面抓取一个 uuid4）作为结构化列。

### 3.6 保留期 / 重置 / 恢复

**保留期（修正 §2.5 的 no-op 与 §2.6 的天数猜测）**

```python
# maintenance_loop（common/database.py:385-400）内，每 10 tick（20 min）一次
age_floor  = SELECT MAX(seq) FROM collection_records WHERE recorded_at < :horizon
ack_floor  = int(ack_seq) if ack_seq is not None else +inf   # ← 从未 ack 过时【不设下界】
disk_floor = 若 free_disk < SYNC_DISK_FLOOR_BYTES: 强制裁到剩余磁盘达标为止（记 forced_prune_log）
floor = max(disk_floor, min(age_floor, ack_floor))

DELETE FROM collection_records
 WHERE seq IN (SELECT seq FROM collection_records WHERE seq > 1 AND seq <= :floor
               ORDER BY seq LIMIT 2000)          -- seq > 1 保住哨兵行
```
- `ack_seq` 初值必须是 **NULL 而不是 '0'**（§2.5）。
- 不能用 `DELETE ... LIMIT`（需 `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`，发行版构建不保证）。
- **`incremental_vacuum` 单独跑**：另开一个短生命周期连接，或在确认 `in_transaction is False` 后单独一次 `_write_lock` 内执行，**绝不与 DELETE/记账放进同一事务**（§2.3 实测）。前后读 `PRAGMA freelist_count` 断言有进展，为 0 且有空闲页时告警。
- `min_available_seq` **永远现算** `SELECT MIN(seq) WHERE seq > 1`（PK 最左下降，O(log n)），**绝不缓存进 `sync_meta`**（§2.2 的方案三破法）。
- 强制裁剪必须是**持久化的闩锁**：写进 `forced_prune_log`（from_seq, to_seq, ack_seq_at_time, ts），在 `/status` 上一直返回直到消费者逐条确认。只在响应上放一个瞬时布尔是无效的——消费者宕机正是触发前提。

**重置（`DELETE /api/database`，server/app.py:2646-2663）**
```python
# 1) 收窄（对既有六表行为等价：五张 AUTOINCREMENT 表 batches:443 / asin_data:476 /
#    asin_changes:535 / tasks:550 / screenshots:578 全在 loop 里；
#    batch_asins:466 与 seller_discoveries:594 是复合主键，本无 sqlite_sequence 行）
await db._db.execute("DELETE FROM sqlite_sequence WHERE name IN "
                     "('asin_changes','asin_data','batch_asins','tasks','screenshots','batches')")
# 2) 在这个 handler 里【重铸 gen】——进程不重启，per-boot 轮换不会触发（§2.4）
# 3) 给整个 handler 补 try/rollback：它现在是裸 BEGIN 无 except（2650-2655）
```
`collection_records` 与 `sync_meta` 不在 2652 的表列表里，行天然存活；加上哨兵行，计数器即使被清也能从 `max(rowid)` 重算（实测：有行存活时 → 7，不是 1）。

**恢复（三层，缺一层就有静默窗口）**

| 层 | 机制 | 覆盖 |
|---|---|---|
| L1 服务端 409 | `after_seq > MAX(seq)` 直接 409 | 恢复后第一次轮询即命中（最常见情形） |
| L2 单调计数器修复 | 启动时 `new_counter = max(现 sqlite_sequence 值, MAX(seq), sidecar_hwm, sync_meta.hwm_seq)`，**只升不降**，降则 ERROR 告警。sidecar 写 `max(旧值, MAX(seq))` 并 fsync | 修掉方案三"无条件修复把 fast-forward 抹掉"的自伤（§2.4）与 sidecar 自擦除 |
| L3 消费端锚定 | 每个响应带 `max_seq` + `gen`；catalog_sync 持久化历史最大 `max_seq`，**下降即硬停 + 全量对账**；`gen` 变化即硬停 | **唯一能在整机快照回滚下存活的探测器**（其状态在 scraper 磁盘之外）。方案一说"gen 变化是正常的、无需消费侧动作"——那等于把唯一有效信号关掉 |
| L4 内容摘要 | 对**已消费过的**区间定期跑 `/counts`，比 `SUM(row_digest)` | 唯一非竞态的探测器：恢复后重新生成，占据同一 seq 区间的是**不同的行**。方案一的 `seq_checksum = SUM(seq)` 对内容完全盲（实测：删掉 10 行换 10 行完全不同的内容，`(count, SUM(seq))` 仍是 `(10, 55)`） |

---

## 4. 接口契约草案（交给沃尔玛侧）

**落点**：新文件 `server/api/sync.py`（`server/api/__init__.py` 实测 0 字节且无人 import）。app.py 底部加两行：
```python
from server.api.sync import router as sync_router
app.include_router(sync_router)
```
```python
router = APIRouter(prefix="/api/v1/sync", tags=["sync"],
                   dependencies=[Depends(require_sync_key)],
                   include_in_schema=False)      # ← §2.14
```
- **前缀 `/api/v1` 是承重的**：现无任何路径以 `/api/v1` 开头；挂在 `/api/results/*` 或 `/api/export/*` 下会被 server/app.py:1769 / 1952 的 catch-all 吞成 404，而消费者会读成"暂无数据"永不推进。
- **`db` 必须在调用时惰性解析**（`from server import app as _srv; db = _srv.db`），不得模块级 `from server.app import db`（§2.13）。
- 鉴权：`X-Sync-Key` + `hmac.compare_digest` 比 `SYNC_API_KEY`；不匹配 401，未配置 503（**fail closed**）。**只能是 router 级依赖**——全局中间件会瞬间打断所有 worker 和 erpAPI（现全站零鉴权）。
- 每页借一条读连接、跑完即还（对比 `iter_results` 整场占用，common/database.py:2344-2372）。读池只有 3 条（313），建议给 sync 一条专用连接 + `asyncio.wait_for` 超时（现在 `read()` 无超时，池空即无限挂）。

### 4.1 `GET /api/v1/sync/records` —— 拉取

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `after_seq` | int ≥0 | 必填 | **独占**下界 |
| `limit` | int 1..1000 | 200 | 独立上限，不动 `/api/results` 的 `Query(50, le=200)` |
| `format` | `packed`\|`json` | packed | packed 返回 base64 压缩块；json 返回解压对象，limit>200 时 422 |
| `outcomes` | csv | 全部 | 例 `ok,not_found` |

**实现要求（承重）**：在借来的只读连接上 `BEGIN` → `SELECT MIN(seq)` / `SELECT MAX(seq)` / 页查询 → `COMMIT`，三条语句同一快照（实测 `query_only=ON` 连接可以 BEGIN，WAL 下不阻塞写）。守卫在**页查询之后**用同一快照的 MIN 复核（§2.2）。

```jsonc
{
  "contract_version": 1,
  "gen": "a3f19c2b7e04",
  "instance_id": "prod-hk-1",
  "min_available_seq": 39120041,
  "max_seq": 41872330,
  "server_time_utc": "2026-08-04T09:12:33Z",
  "after_seq": 41208000,
  "next_after_seq": 41208500,
  "has_more": true,
  "count": 500,
  "retention_forced": false,
  "records": [{
    "source_id": "a3f19c2b7e04:000041208001:9f2c1b",
    "seq": 41208001,
    "asin": "B0CXXXXXXX",
    "marketplace": "amazon.com",
    "zip_requested": "10001",
    "zip_observed": null,
    "zip_verify": "assumed",
    "collected_at": "2026-08-04T09:11:02Z",
    "recorded_at":  "2026-08-04T09:11:07Z",
    "outcome": "ok",
    "error_type": null,
    "batch_id": 8123, "task_id": 5512907, "worker_id": "w-hk-02",
    "attempt": 0, "parse_engine": "selectolax",
    "slow_hash": "sha256:1f0c9a…", "review_hash": "sha256:77ab12…", "hash_ver": 1,
    "row_digest": 734829183742,
    "payload_enc": "zlib1+json",
    "payload_b64": "eJyNVE1v…"        // format=json 时替换为 "result": {...}
  }]
}
```

| 状态码 | 条件 | 消费者动作 |
|---|---|---|
| 200 + `records: []` + `has_more:false` | 无新数据 | 正常等待。**本端点永不对空结果返回 404**（与 server/app.py:2020 的既有导出行为刻意相反） |
| **409** `cursor_below_retention` | `after_seq + 1 < min_available_seq` | **掉出保留窗口** → 告警 + 全量对账。服务端强制，不作为消费侧可选检查 |
| **409** `cursor_ahead_of_stream` | `after_seq > max_seq` | 疑似恢复/回滚 → 告警 + 全量对账（§3.6 L1） |
| 401 / 422 / 429(+`Retry-After`) / 503 | 鉴权 / 参数 / 背压 / 未配 key | — |

### 4.2 `GET /api/v1/sync/status`
返回 `gen / instance_id / min_available_seq / max_seq / ack_seq / lag_records / oldest_recorded_at / newest_recorded_at / retention_horizon_utc / retention_forced / forced_prune_log[] / db_size_bytes / free_disk_bytes / observed_daily_insert_rate`。
`forced_prune_log` 是**未确认的强制裁剪事件列表**，逐条确认前一直返回（§2.5/§3.6）。

### 4.3 `GET /api/v1/sync/counts?from_seq=&to_seq=[&bucket=hour]`
```sql
SELECT COUNT(*), MIN(seq), MAX(seq), SUM(row_digest),
       MIN(recorded_at), MAX(recorded_at)
  FROM collection_records WHERE seq > ? AND seq <= ?
-- bucket=hour 时 GROUP BY substr(recorded_at,1,13)，走 idx_cr_recorded
```
区间宽度 > 8,000,000 返回 422（保 int64 精确）。**对账必须比 `SUM(row_digest)`，不能比 `SUM(seq)`**（§3.6 L4 实测）。

### 4.4 `POST /api/v1/sync/ack` `{"gen":"…","ack_seq":41808200}`
单调取 max，永不后退；`gen` 不符返回 409。首次 ack 之前保留期按**纯时间 + 磁盘**执行（§2.5）。

### 4.5 消费侧拉取算法（写进契约，不是建议）

```python
st = GET /status
if st.gen != stored_gen:                 ALARM("generation changed"); full_reconcile(); STOP
if st.max_seq < stored_max_seq_ever:     ALARM("stream rewound");     full_reconcile(); STOP
if st.forced_prune_log:                  ALARM(...)   # 逐条处理并确认
stored_max_seq_ever = max(stored_max_seq_ever, st.max_seq)

while True:
    r = GET /records?after_seq=X&limit=500
    if r.status == 409:  ALARM(r.error); full_reconcile(); STOP
    for rec in r.records:
        INSERT INTO catalog.snapshots (...) ON CONFLICT (source_id) DO NOTHING
        if rec.outcome == 'ok' and passes_quality_guard(rec):
            UPSERT catalog.products …
              WHERE excluded.recorded_at > products.last_recorded_at   # 单调守卫
    X = r.next_after_seq
    POST /ack {gen, X}
    if not r.has_more: break
```

**硬性规则（违反即数据错误，不是风格问题）：**
1. **"同组最新值"一律按 `seq` 排序**，不得用 `recorded_at`，更不得用 `collected_at`。时钟前/后跳会让时间戳与 seq 非单调（服务端时间戳来自 `datetime.utcnow()`，无单调守卫，common/database.py:1826/1711；server/app.py:1499 还有一处 `datetime.now()` 写本地时间进 `tasks.updated_at`）。三份设计给的 `(recorded_at, seq)` 复合键看着更稳，实际更危险。
2. **分组键 = `(asin, marketplace, zip_requested)`**。按 `asin` 单独分组会退化成"最近哪个邮编采的"，价格序列在市场间振荡且无法察觉。
3. **`gen` 变化是硬停**，不是"正常、无需动作"。
4. **绝不把"没有新记录"读成下架/撤回**。采集侧有 5 条无鉴权删除端点（server/app.py:1300, 1345, 2233, 2312, 2650），其中 `DELETE /api/results` 用 `d.asin LIKE ? OR d.title LIKE ? OR d.brand LIKE ?` 模糊选目标（server/app.py:2264-2280）——**不要发 tombstone**，一次手滑的模糊删除会复制成 Postgres 里的大规模墓碑。
5. `outcome != 'ok'` 的记录**只入 snapshots，不得触发 products upsert，其 `slow_hash` 不参与复审判定**。
6. 复审门是合取：`review_hash 变了 AND 本条通过完整性守卫 AND 被比较的上一条也通过`（§2.11）。占位符进/出**永不触发复审**。
7. `hash_ver` 变化时：**更新存储的 hash 与版本，但不入复审队列**（§7 待定）。

---

## 5. 边界测试清单

对应简报约束 #4。每条给出可执行场景与期望。

| # | 场景 | 具体做法 | 期望 |
|---|---|---|---|
| **T1** 同游标值多条 | 用 500 个 ASIN 单批提交（`accept_results_batch` 一个事务写 500 条，共用 common/database.py:1711 的同一个 `now`），poller 每 200ms 拉一次 | 500 条 `recorded_at` 相同但 `seq` 各不相同；严格 `seq > X` 一条不漏。**对照组**：用 `recorded_at > X` 跑同一测试，必须能观察到丢行——这是证明"不能用时间戳做游标"的回归证据 |
| **T2** 乱序写入 | 10 个并发提交者打 `/api/tasks/result/batch`，poller 1Hz。断言：(a) 每页 seq 严格升序；(b) `min(page) > cursor`；(c) 抽干后 poller 收到的 source_id 集合 == `SELECT gen\|\|':'\|\|printf('%012d',seq)\|\|':'\|\|rid FROM collection_records WHERE seq>1` | 三条全通过。**(c) 是哨兵断言**：任何人把 uvicorn 设成 `workers=2`、或引入第二个写连接、或让 sync 查询落到写连接，(c) 会在数秒内失败 |
| **T3** 脏读 + 回滚复用（§2.1 专项） | 在 `accept_results_batch` 事务内注入 sleep，期间发一次拉取请求；随后强制事务 ROLLBACK；再提交一批真实结果 | 拉取**必须返回 0 条未提交行**。回滚后重新分配的 seq 因 `rid` 不同产生不同 source_id。断言消费侧没有任何 source_id 冲突被静默丢弃 |
| **T4** 重跑/补采 | (a) 同 ASIN 在 zip 10001 与 90210 各采一次；(b) 用**同名批次**重传同一份 ASIN 表；(c) `POST /api/batches/{name}/retry`；(d) 用**新批次名**重传 | (a) 产生 **2 条**独立记录、`zip_requested` 各异（今天只剩 1 条）；(b) `inserted == 0`，**0 条新记录**——把这条断言写进 runbook：重跑必须换批次名；(c)(d) 每次受理产生新 seq + 新 source_id |
| **T5** 重启（提交前后各一次） | `kill -9` 服务端：(a) `COMMIT` 之前；(b) `COMMIT` 之后、HTTP 响应之前 | (a) 全回滚，租约仍 `processing`，worker 重投（worker/engine.py:1715-1746，3 次，~27.5s 后降级为逐条 POST）成功 ⇒ **恰好 1 条**记录；(b) 重投撞 lease 门（common/database.py:1739-1743）rowcount=0 计 stale ⇒ **不再写第二条**。两种情况下 seq 均严格递增无复用 |
| **T6** 泄漏事务楔死 | 在 `DELETE /api/results` 大集合请求中途断开客户端，然后发任意写请求 | 加固前：`cannot start a transaction within a transaction`，游标停摆。加固后（BEGIN 前 `in_transaction` 检查 + ROLLBACK）：单请求失败，下一个写请求正常。**同时验证**：uvicorn 是否真的在客户端断连时取消 handler（requirements.txt 只钉 `uvicorn[standard]>=0.24.0`，**本仓库无法判定**）——这决定该路径是罕见还是常态 |
| **T7** 保留期与掉窗 | 灌 N 条 → 裁掉前 M 条 → 用落在空洞里的 `after_seq` 请求 | 返回 **409 `cursor_below_retention`**，不得返回"跳过空洞后的下一批行"。**竞态版**：在 `SELECT MIN` 与页查询之间注入裁剪提交，仍必须 409（§2.2） |
| **T8** ack 门初值 | 全新库，从不调 `/ack`，跑到超过保留期天数 | 保留期**必须正常裁剪**（按时间/磁盘）。若一行未裁 ⇒ §2.5 未修 |
| **T9** 强制裁剪 | 把 `SYNC_DISK_FLOOR_BYTES` 设到当前剩余磁盘之上，触发一次维护 tick | 裁到下界；`forced_prune_log` 记录一条并在 `/status` 上**持续返回直到确认** |
| **T10** 重置 | 调 `DELETE /api/database` | 六张既有表被清且计数器归零；`collection_records` 行**存活**；`gen` **在 handler 内**被重铸；随后新记录的 source_id 与任何已消费值都不同 |
| **T11** 重置 + 空表（§2.4 专项） | 先让保留期把 `collection_records` 裁到只剩哨兵行，再调 `DELETE /api/database`，再写新记录 | 新 seq **必须 > 历史 max**（哨兵行 + 单调计数器修复共同保证）。移除哨兵行重跑一次，确认能复现 seq=1 —— 证明该防护确实在起作用 |
| **T12** 恢复 | (a) 只回滚 `.db` 文件；(b) 整机快照回滚（sidecar 一起回滚） | (a) 启动时 sidecar/`sync_meta` 检出回退 → 铸新 gen + 计数器 fast-forward；(b) 服务端**检不出**（预期），必须由消费端 `max_seq` 单调检查 + `/counts` 内容摘要复核捕获。测试须显式验证 (b) 会告警，而不是静默继续 |
| **T13** 克隆部署 | 把 `.db` 复制到第二台机器同时起服务 | 两边 `gen` 不同（per-boot + `SYNC_INSTANCE_ID`）⇒ 无 source_id 碰撞。**若 gen 存在库里就会碰撞**——这是方案二/三的破法，务必回归 |
| **T14** `executescript` 提交（§2.3 专项） | 在一个打开的事务中调用维护任务的 vacuum 步骤 | 加固后：断言 `in_transaction is False` 后才执行，或走独立连接。绝不允许出现 "cannot commit - no transaction is active" 被吞掉 |
| **T15** erpAPI 无回归 | 变更前后各跑一遍：upload → status → export xlsx/csv → screenshots zip → `/api/results` 翻页 → `batch.completed` webhook；逐字节 diff | 全部字节相同。**另外单独 diff `/openapi.json`**——若用 `include_in_schema=False` 应无变化，否则记录并与 erpAPI 侧确认（§2.14） |
| **T16** 时钟步进 | 服务端 NTP 后跳 1 小时 / 前跳 > `TASK_TIMEOUT_MINUTES` | 后跳：`seq` 顺序不受影响，"最新值"结论不变（因为按 seq 排序）。前跳：`reclaim_dead_worker_tasks`（common/database.py:1246-1281）会一次性回收所有在途任务、bump 全部 lease_epoch ⇒ 流量断崖 + stale 尖峰 —— **必须能与真正的丢数据区分**，所以 reclaim 数与 stale 数要作为时间序列暴露 |
| **T17** 哈希稳定性 | 同一份 HTML，在 **5 个独立 Python 进程**里各解析一次，比 `review_hash` | 5 个必须完全相同。今天的 `content_hash` 在此测试下必失败（§2.9 实测） |
| **T18** 慢变字段完整性 | 构造一个「有 buybox 但无面包屑/无详情表/无 colorImages」的降级页 | `outcome` 或完整性位必须标出降级；`review_hash` 写 NULL 或该条被守卫拒绝提升到 products。绝不允许"好页→降级页→好页"造成两次复审 |
| **T19** 写路径压测 | 峰值 83 ASIN/s 持续 1 小时，采 `/api/_debug/lock-stats`（server/app.py:2622）与 **WAL 文件大小时间序列**（server/app.py:296-299 已在采样） | `accept_results_batch` p50 相对 README.md:552 的 7.5ms 上浮 < 15%；**WAL 不得单调增长**。三份设计都没建模 WAL，这是最可能被忽略的回归（§2.17） |

---

## 6. 分阶段落地计划

工作量按 1 个熟悉本仓库的工程师估。**除 P0 与 P1 的两处一行修正外，全部为纯新增**。

| 阶段 | 内容 | 工作量 | 加性 | erpAPI 如何保持不变 |
|---|---|---|---|---|
| **P0 前置**（必须先做，计划停机） | 备份 `scraper.db`（**今天完全没有备份机制**）；`PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`；跑一次 `SELECT COUNT(*) FROM tasks WHERE status='done' AND updated_at > datetime('now','-1 day')` 定容量基线（§2.6） | 0.5d + 停机窗口 | N/A | **明确记为停机**，不是"零影响变更"。VACUUM 独占锁期间 uvicorn 无超时参数（server/app.py:2670-2675），请求会无限排队 |
| **P1 写入侧**（无端点） | DDL + 哨兵行 + `_init_sync_state`（gen/instance_id/计数器单调修复/sidecar）+ flock + H1..H5 写钩子 + `record_stage("save_record")` | 3-4d | ✅ 纯新增 | 不给 `asin_data` 加列、不给 `AsinData` dataclass 加字段 —— 这是**必须写进 CI 的硬不变式**：快照 `PRAGMA table_info(asin_data)` 与 `EXPORTABLE_FIELDS`，任何 diff 即失败（§2.17） |
| **P1b 安全修正**（与 P1 同批） | (a) server/app.py:2654 收窄 sqlite_sequence + handler 内重铸 gen + 补 try/rollback；(b) 四条结果路径 `BEGIN` 前的 `in_transaction` 防御（common/database.py:1663, 1716, 1996, 1339）；(c) 禁用/移除 `read()` 的写连接回退（363-364） | 1d | ⚠️ **改 3 处既有代码** | (a) 对既有六表行为逐字等价（实测：五张 AUTOINCREMENT 表全在 loop 内，另两张是复合主键无 sequence 行），响应体仍是 `{"ok": True}`；(b) 只在异常路径生效；(c) 只影响池未初始化的降级路径 |
| **P1c 观测 48h** | 跑 P1，不开端点。盯 `/api/_debug/lock-stats` 的 `save_record` 阶段 + `accept_results_batch` p50/p99 对 README.md:552/415 基线（7.5 / 93.81 ms），**外加 WAL 大小时间序列** | 2d 等待 | — | p50 上浮 >15% 或 WAL 单调增长 ⇒ 回滚钩子（表留着无害） |
| **P2 读取侧** | `server/api/sync.py`：4 个端点 + 只读连接显式 `BEGIN` 快照 + 服务端 409 + 独立连接/信号量/超时；`SYNC_API_KEY` 进 `.env.example` 与 `server.service` 的 `EnvironmentFile=` | 3d | ✅ 纯新增 | `/api/v1` 前缀避开 1769/1952 的 catch-all；router 级依赖，不加全局中间件；`include_in_schema=False` 保 `/openapi.json` 不变；T15 全量回归 |
| **P3 试点（=一周验收）** | catalog_sync 只读拉取，不 ack，写 staging schema；每晚跑 `/counts` 对账 | 1w（沃尔玛侧主导） | ✅ | 拉取用独立读连接，与导出/仪表盘不争 |
| **P4 保留期 + ack** | 保留期任务（按剩余磁盘 + 行数上限驱动，**不是天数**）、`forced_prune_log`、`/ack` 接线、`incremental_vacuum` 独立连接 | 2d | ✅ 纯新增 | 只 DELETE 新表；T8/T9/T11 覆盖 |
| **P5 质量字段（worker 侧）** | `_zip_observed`（改用 `zip_effective_in_html` 抽出的值而非 line1 正则）、`_zip_verify`、`_parse_engine`、404 分支改成 `_outcome='not_found'` 且**不写占位符**、`field_completeness` 位（按 HTML 区块存在性判定） | 3-4d | ✅ 纯新增（`_` 键被 common/database.py:1906/1944 的字段白名单自动丢弃，与今天的 `_page_asin` 同理） | 不改 `asin_data` 写入语义 ⇒ 既有端点返回值不变。**注意**：若同时把 404 分支改成不覆盖 `asin_data`，那**会**改变既有端点对降级 ASIN 的返回内容——需单独决策 |
| **P6 慢变哈希 v2** | 规范化 `review_hash`（NFKC / 空白折叠 / 哨兵全等匹配 / 列字段排序 / image URL 归约到 `/images/I/<ID>` / 排序键 JSON / SHA-256）；排除 `best_sellers_rank`、`variation_asins`、`ean_list`；**加入 `variant_attributes`**（并先规范化其带标签/不带标签两种格式）；`variation_asins` 仅从 `_parse_twister` 取值 | 3d | ✅ 只在新表上 | `content_hash` 原样保留，不动 common/database.py:251-267 ⇒ `asin_changes` 行为不变 |

**最快解锁沃尔玛侧的路径**：P0 → P1 → P1b → P1c → P2，约 **2 周 + 一个停机窗口**，之后 catalog_sync 就能拉到带完整参数与 UTC 时间戳的追加流。P5/P6 决定复审门何时可用，可与试点并行。

---

## 7. 需要沃尔玛侧拍板的问题

按阻塞程度排序。前四条不定就无法开工。

| # | 决策 | 为什么必须你们定 | 不定的后果 |
|---|---|---|---|
| **1** | **真实日采集量与拉取节奏**：catalog_sync 每 N 分钟拉一次的 N？采集侧每天重采多少 ASIN？ | 这一个数决定保留窗口、决定同库方案是否成立。我的 0.61 GB/天假设的是"每天全量重采 29 万"；README.md:551 的实测峰值 60-83 ASIN/s 意味着峰值 4 小时就是 2.4 GB/天，是假设的 4 倍（§2.6）。20GB 磁盘、已用 2.4GB | 保留窗口只能是猜的，磁盘打满会连带 erpAPI 一起挂 |
| **2** | **失败/降级采集要不要进流**：`outcome ∈ {not_found, rejected, failed, stale}` 的记录，是导出还是丢弃？ | 导出 ⇒ 你们能区分"下架/被封"和"没人采"，能算每 zip 覆盖率、能给陈旧价格打折；但重试会放大行数，直接吃保留窗口。丢弃 ⇒ 缺失记录与从未尝试不可区分。**特别地** §2.12 的 `stale`：那是一次真实的完整抓取被 lease 门丢掉，与简报"不要在采集侧去重"冲突 | 我不能替你们权衡"可观测性 vs 保留天数" |
| **3** | **ack 契约要不要**：catalog_sync 能不能实现 `POST /ack`？ | 有 ack ⇒ 保留期在消费者存活期间**可证无损**。纯拉取 ⇒ 保留期是"尽力而为 + 可检测失败"（靠 409），且 ack 门必须默认关掉否则退化成不裁剪（§2.5） | 决定 P4 的形态，也决定磁盘应急裁剪的触发概率 |
| **4** | **网络路径与鉴权形态**：catalog_sync 跑在哪台机器？与 scraper 之间有没有既有私网（VPN/VPC peering）? | `SERVER_HOST = "0.0.0.0"` 是硬编码字面量（common/config.py:34，与下一行可配的 `SERVER_PORT` 对比），deploy/ 里没有反代也没有 TLS。有私网 ⇒ 共享密钥头够；没有 ⇒ 还得立 TLS 终结。另外 §2.7 的**写入侧**（无鉴权、无 lease）如果也在公网可达，是一键游标注入原语 | 决定 P2 的范围与上线风险 |
| **5** | **`hash_ver` 升级时的复审策略**：v1→v2 那天，全语料（~29 万）的哈希会同时不匹配 | 三份设计都以为加个版本列就解决了——**不解决**。消费侧规则是"hash 不同就复审"，版本一变就是一次全量复审风暴，这是真金白银。可选：(a) 版本变化时只更新存储值不入队（接受一次盲区）；(b) 过渡期采集侧**同时输出 v1 和 v2**，消费侧用 v1 把门、后台回填 v2，再切换 | 这是复审服务的成本决策，必须上线前定，不能事后定 |
| **6** | **复审门的完整性守卫策略**：`scrape_status != 'ok'` 或某字段组缺失时，是"跳过 upsert 保留旧值"还是"照写"？ | §2.11 证明了单靠哈希+归一化无法避免占位符抖动，必须是合取式守卫。但"保留旧值"意味着 catalog.products 可能长期停在旧数据上而无人察觉 —— 需要你们定 SLA（多久没有 `ok` 记录就告警） | 决定 P5 交付什么字段、以及它们的语义 |
| **7** | **`marketplace` 值域**：是否要区分多个站点？规范化成 `amazon.com` 还是 `US` 还是 marketplace ID `ATVPDKIKX0DER`？ | 今天代码写死 `"US"`（worker/parser.py:1333），列默认是 `'amazon.com'`（common/database.py:513），dataclass 默认又是 `'amazon.com'`（common/models.py:67）。这是分组键的一部分，值域必须先钉死 | 值域不定就无法安全 GROUP BY |
| **8** | **erpAPI 到底调哪些端点**（需你们或 erpAPI owner 提供） | 本仓库**无法判定**：没有 client 代码、没有 OpenAPI 快照、tests/ 只有 3 个单元测试无 HTTP 层测试。我只能**结构性**论证加性（一行既有 decorator/SQL 都没改），**无法度量**。验证方法：生产 uvicorn 访问日志按 path+method 分组一天，或直接要 erpAPI 源码。另需确认它是否消费 `/openapi.json`（§2.14）和 `batch.completed` webhook（README.md:157-172 记的载荷与 server/app.py:508-535 的实现在 4 处不符——README 错、实现对，**不要改实现**） | "零影响 erpAPI"这条验收标准在拿到调用清单前**无法测量** |

---

### 本次审计中未能验证的事项（及验证方法）

| 未验证项 | 验证方法 |
|---|---|
| 生产库真实大小、剩余磁盘 | 本 checkout 无 `data/` 目录（.gitignore:6-8 排除）。上机跑 `ls -la data/` + `df -h` |
| 部署的 uvicorn 是否在客户端断连时取消 handler | requirements.txt 只钉 `uvicorn[standard]>=0.24.0`。跑 T6：发大集合 `DELETE /api/results`，中途断开，再发写请求看是否报 "cannot start a transaction within a transaction" |
| :8899 是否公网可达 | 上机 `ss -tlnp` + 云安全组规则。决定 §2.7 是互联网暴露还是内网 |
| 生产 worker 是否统一装了 selectolax | 每台 worker `python -c 'import selectolax'`。若有 lxml 回退机，`rating/review_count/seller_id/seller_name` 今天就是陈旧的结转值（lxml 路径 worker/parser.py:824-936 从不赋这四个键，`_default_result` 也没有，于是 common/database.py:1906 的 `is not None` 跳过它们） |
| 生产库里 `site` / `zip_code` 的实际取值分布 | `SELECT site, COUNT(*) FROM asin_data GROUP BY site;` 和 `SELECT length(zip_code), COUNT(*) FROM asin_data GROUP BY 1;` 决定规范化是回填还是仅向前修 |
| 真实 A+ 文案的压缩率 | 我在合成语料上测得 zlib-1 约 3.5x（5738 B → 1645 B）。拉 1000 行生产 `asin_data` 重新序列化后压一遍，这个数直接决定保留窗口 |
| 面包屑/详情表/colorImages 的实际缺失率 | `DUMP_DEGRADED_HTML=1`（common/config.py:69，默认关）跑 500 个 ASIN，统计 `root_category_id='N/A'`、`model_number='N/A'`、`image_urls=''` 的行数 |
| `_extract_page_asin` 返回 None（无变体偏移防护）的频率 | worker/parser.py:1438 的 None 分支加计数器，跑一批观察 |