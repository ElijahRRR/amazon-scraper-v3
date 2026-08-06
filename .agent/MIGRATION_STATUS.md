# 迁移状态：一页诚实版

> SQLite → PostgreSQL + catalog_sync 事件流。
> **本文是切换前唯一需要通读的文件。** 其余文档的定位：
> `common/pgdb/OWNERSHIP.md` = 决策台账（D-1..D-62，冲突时以它为准）；
> `.agent/pg_migration_plan.md` = 计划书 + 「计划错在哪里」；
> `docs/sync_contract.md` = 交付沃尔玛侧的契约；
> `.agent/phase{1,2,3}/` = 各阶段验证报告（都带 file:line 与实测输出）。
>
> 最后更新：**真机验证第一轮**（macOS 本机，PG 17.9）。分支 `claude/walmart-api-db-refactor-7oergd`。

---

## 0. 三十秒版

| 阶段 | 状态 |
|---|---|
| Phase 0 骨架与黄金基线（64 步） | ✅ |
| Phase 1 存储层移植 `common/pgdb/` | ✅ |
| Phase 2 事件流 outbox + 单 relay | ✅ |
| Phase 3 同步 API `/api/v1/sync/*` + 契约 | ✅ |
| Phase 4 采集质量（worker + 服务端接线） | ✅ |
| Phase 6 保留期 + ack | ✅ |
| **Phase 5 S1-S3 真机跑起来** | ✅ 本机 PG 17.9 已验（见 §5.5） |
| **Phase 5 S4 新旧内容比对** | ✅ 44 个 ASIN、两边同批，`UNEXPECTED = 0`（见 §5.6） |
| **Phase 5 S6 切换** | ⛔ 未做。顺序是承重的，见 §7.1 |
| Phase 1.5 写并发放开 | ⛔ 有意推迟（见 §3） |

**当前门（六道，全绿）**——命令与实测输出见 §6。

**一句话风险判断**：存储层与事件流被黄金 64 步 + 每个后端约 700 条 pytest 用例
逐条钉住；新旧两套系统采同一批 ASIN 的**内容**比对也已在真机做完（§5.6，
`UNEXPECTED = 0`）。剩下的风险集中在**切换动作本身**——部署顺序是承重的
（§7.1），以及三个**样本没覆盖到**的有意变更（Amazon 自营页的 seller_*、
邮编未生效时的 D-55、多值 upc_list 的排序），它们在真机第一轮里无从显现，
只能靠灰度期的真实流量兜住。

---

## 1. 已完成的（附「凭什么这么说」）

### Phase 1 存储层
- `common/pgdb/` 50 个公开方法，导入期自检公开面完整性与单一归属。
- **`common/database.py` 一个字节没改**（标准规矩第 1 条），SQLite 路径完全保留。
- 修掉 3 个并发阻断（D-13 / D-15 / D-17）+ 4 处语义漂移（D-16 / D-18 / D-19 / D-20）。
  每一条都有配对反事实，不是「看起来不对就改」。
- 证据：`.agent/phase1/verify{1,2,3}.md`。

### Phase 2 事件流
- `scraper.scrape_outbox` → **单** relay → 按 seq 分区的 `scraper.scrape_events`。
- 游标保证有**配对对照**：同一批事务下，朴素游标丢 **24.88%** 的行，outbox+relay 丢 **0**。
- 慢字段哈希 `common/slowhash.py`（纯 stdlib，零 `common.*` 依赖）。
- 证据：`.agent/phase2/verify{1,2,3,4}.md`。

### Phase 3 同步 API
- `/api/v1/sync/{records,status,counts,ack}` + `docs/sync_contract.md`。
- SQLite 后端上**挂路由但回 503**，不回 404（D-32）——404 会被消费者读成
  「暂无数据」，游标永不推进、静默停摆。

### Phase 4 采集质量
- worker 侧：D-55..D-62（zip 三分、completeness 位图、404 分支、manufacturer、
  set 排序、四个结转字段、crawl_time、parse_engine）。
- 服务端：D-39..D-43（四个信号接进事件流、404 写入保护）。
- **接缝已实测**：真 HTML → 真 parser → 真 engine → 真 HTTP → 真 relay →
  `/records` 原始响应，五个信号逐一相等；「好页→软降级→好页」
  只比哈希 = **2 次误复审**，契约 §6.5 合取门 = **0 次**。

### Phase 6 保留期 + ack
- 下界 = `max(硬下界, min(时间下界, ack_seq − slack))`，**整分区 DROP，只裁前缀**。
- `ack_seq` 永不为 0 做成**三层**：唯一读取入口 + 库上 CHECK + 公式断言（D-46）。
- `forced_prune_log` 是持久闩锁 + 第五个端点 `POST /ack-prune`（D-49 / D-50）。

---

## 2. 两个后端之间的**残留差异**（完整清单）

> 这一节是本文的重点。**每一条都是有意保留或有意引入的**，
> 都有 D 号，且都在切换后随 SQLite 路径退役而消失。
> 「黄金 64/64」对下面**任何一条都不构成反证**——原因见每条的「黄金为什么看不见」。

### 2.1 会改变 HTTP 响应的

| # | 差异 | SQLite | PostgreSQL | 出处 | 黄金为什么看不见 |
|---|---|---|---|---|---|
| R1 | **整数列的类型亲和**。`POST /api/tasks/release` 传 `task_ids: ["1"]`（JSON 字符串） | `200 {"ok":true,"released":1}` | **`500`**（asyncpg `'str' object cannot be interpreted as an integer`） | Phase 1 verify3 | 基线里这个端点只用整数调用过 |
| R2 | 载荷里掺一条 SQLite 会拒收的值（越界 int / list / dict / bytes） | 整批 500 + 回滚 | 同左（D-20 已对齐） | D-20 | — |
| R3 | 标题里带 NUL 字节的上传 | 收下 | **整批 500**（`PG_STRIP_NUL=1` 可改成剔除，但那会改变落库数据进而改哈希） | D-12 | 基线里没有 NUL |
| R4 | `/api/_debug/lock-stats` 的 caller key | 有 `checkpoint` | **没有**（PG 里不存在对应操作；为了让指标"看起来一样"去空转一把锁等于伪造观测） | D-14 | 基线只钉了 `waits`/`holds`/`stage_timings` 的既有 key |

**R1 的完整实测**（今天重跑，`scratchpad/residual_release.py`）：

```
===== backend=sqlite =====
  [sqlite] POST /api/tasks/release task_ids=['1'] -> 200 {"ok":true,"released":1}
  [sqlite] 之后 create_batch -> OK id=2
  [sqlite] POST /api/tasks/release task_ids=[1]   -> 200 {"ok":true,"released":0}
===== backend=postgres =====
  [postgres] POST /api/tasks/release task_ids=['1'] -> 500 Internal Server Error
  [postgres] 之后 create_batch -> OK id=2
  [postgres] POST /api/tasks/release task_ids=[1]   -> 200 {"ok":true,"released":1}
```

> 注意第 3 行两边的 `released` 一个是 `0` 一个是 `1`：SQLite 把字符串 `"1"`
> 匹配上了整数 id，**第一次调用就已经把那个任务放掉了**；PG 第一次 500 什么都没放。
> 也就是说 R1 不只是「错误码不同」，它会让**同一串调用序列的最终库状态不同**。
>
> `之后 create_batch -> OK` 两边都成立，说明 **D-17 的修复今天仍然有效**：
> 500 之后写路径没有被焊死（修复前 PG 侧此处是
> `RuntimeError: 嵌套 BEGIN：上一个事务还没结束`，且此后**每一次**写都是它）。

**R1 谁来管**：调用方是 worker（`worker/engine.py` 一直传整数）。风险来自
手工 curl / 运维脚本 / 未来的第三方。**切换前不修**——修它要动 `app.py` 的
参数强转，而那正是 D-4 说的「分叉 app.py 会破坏 SQLite 路径逐字节不变」。
切换**之后**它自然消失（SQLite 路径退役），届时可以顺手把
`as_int` 加到那个端点上。

### 2.2 只影响落库数据、不影响响应的

| # | 差异 | 说明 | 出处 |
|---|---|---|---|
| R5 | **404 的写入保护是 PG-only** | PG 侧：404 不写目录列、不重算两个哈希、不更新 `baseline_*`。SQLite 侧：**照旧污染**（一次 404 = 两次误报变动）。**这是被迫的分叉**——同样的缺陷在 `common/database.py` 里一字不差存在，而那个文件禁止修改 | D-43 |
| R6 | 事件流 / 保留期 / 同步 API 的**数据面**在 SQLite 上完全不存在 | 四张 `scraper.*` 表、relay、保留期都是 PG 独有；SQLite 上是零字节代码 + `/api/v1/sync/*` 回 503 | D-32 / D-45 |

### 2.3 有意的**双向**行为改变（两个后端都变了）

> 这几条不是「PG 与 SQLite 不同」，而是「今天与迁移前不同」。列在这里是因为
> 它们同样会让「SQLite 路径完全没变」这句话不成立。

| # | 改变 | 出处 |
|---|---|---|
| R7 | 三条并列不稳定的 `ORDER BY` 补了全序 tiebreaker。SQLite 侧并列时原来的顺序是**任意的**，现在被钉死 | D-18 |
| R8 | `get_pending_screenshots` 补 `ORDER BY id`（补的就是 SQLite 今天实际产出的那个序） | D-19 |
| R9 | **`worker/parser.py` 是两个后端共用的**，Phase 4 的 D-55..D-62 与 Phase 2 的 D-27 全部同时改变 SQLite 部署的产品数据 | D-27 / D-55..D-62 |

> **R9 是最容易被误读的一条。** 「黄金 64/64」只证明存储层与 HTTP 行为不变；
> 黄金夹具喂的是**合成** result dict，实测（MetaPathFinder）确认 64 步里
> `worker.parser` **从未被 import**。所以解析器的每一处改动对黄金结构性不可见。

---

## 3. 有意**不做**的（不是遗漏）

| # | 项 | 为什么不做 | 何时做 |
|---|---|---|---|
| N1 | **`site` 值域不统一**（parser 写 `"US"`，列默认 `'amazon.com'`） | 它是**导出列**，改值即改已交付给 erpAPI 的数据，而只有 `crawl_time` 有用户确认；信号另有权威来源（事件流 `marketplace`）；它在 `SLOW_HASH_FIELDS` 之外，改它修不好任何误复审；`asin_data` 每 ASIN 一行无版本，**回填不可逆** | Phase 5 并行期（D-44） |
| N2 | `get_results` 的 COUNT 崩溃（`?search=<≥3字符>&cursor=<id>` → 500） | **刻意复现**的既有缺陷。等价优先 | Phase 1.5（D-8） |
| N3 | 真正的写并发（单写连接 + 真锁保持不变） | 换掉要先把 `app.py` 的 24 处裸 SQL 抽干净，否则 7 个 `async with db._write_lock: BEGIN` 块会立刻错乱 | Phase 1.5（D-2） |
| N4 | `except Exception` → `except BaseException` 的统一（10 处） | 取消场景照旧泄漏；影响面小于 N3，但同属清理 | Phase 1.5（D-17 末段） |
| N5 | `app.py:1499` 的 `datetime.now()`（本地时间写进 UTC 列） | **照抄，不修**。等价优先 | 未定 |
| N6 | 404 之外的 **captcha / blocked** 提交体同样能穿过 `_is_parse_failure` | 同一个根因（`stock_count="0"` 不在 `_NA_VALUES` 里）。修它要动 `common/database.py`（禁止），或在 PG 侧再开一处分叉 —— 而 404 是**发生频率最高**的那一档，先修它收益最大 | 切换后（见 §7） |
| N7 | worker 的 404 **不重试、不轮换 IP** | 四条可核查的理由见 D-57。确认成本确定，收益小而推测 | 不做 |

---

## 4. 无法消除的边界（设计上的，不是欠账）

| # | 边界 | 后果 | 唯一防线 |
|---|---|---|---|
| B1 | **整机快照回滚采集侧检测不到** | 只回滚数据库能被检出（`_seq_high_water()` 判回退 → 铸新 `gen`），但连同 `sync_meta` 一起回滚的**整机快照**检不出来 —— 服务端所有状态都在那块盘上 | **消费侧**持久化历史最大 `max_seq`，每轮断言 `st.max_seq >= stored_max_seq_ever`，下降即**告警 + 全量对账**。契约 §7 硬停检查之一、§10.1 明写。**这条必须由沃尔玛侧实现，采集侧无法代劳** |
| B2 | 跨分区的重复 `source_id` 抓不到 | `UNIQUE(source_id)` 只能建在分区上（PG 16 拒绝建在分区父表上，缺分区键列） | relay 认领→落库是单事务，一行不可能被处理两次；「第二个 relay」由单例 advisory lock 挡住（D-21） |
| B3 | `stale` 事件在 `ROLLBACK` 与 `COMMIT` 之间硬崩会丢一条 | 那个窗口里租约本来就没了 | 已写进契约（Phase 2 计划表） |
| B4 | `cursor_below_retention` 在保留期边界的 seq 空洞上仍可能假阳性 | 判据没有放宽——放宽等于把守卫关掉 | Phase 6 从**另一头**收窄了它：常规裁剪先算「裁完之后 `min_available_seq` 落在哪」（D-47），守规矩的消费者实践中不该再撞到 |
| B5 | 截图文件从不进中心库、也不可重建 | `screenshot_path` **随时可能指向已删除的文件** | 契约 §6.6 硬性规则：消费侧不得解引用、不得据此判断截图是否存在过 |

---

## 5. 收口阶段新发现并修掉的（四个 builder 各自的门都是绿的）

| # | 问题 | 怎么发现的 | 修法 |
|---|---|---|---|
| C1 | **测试结果是收集顺序的函数**：`pytest tests/test_session_slot.py tests/test_engine_not_found.py` → **25 failed**，反序 → **75 passed** | 把四份改动放在一起跑才现形。默认字母序恰好是安全的那一种，所以**六道门全绿、缺陷完全不可见** | 五个模块级 `_stub` → `_stub_if_missing`；并加 AST 看守 `ModuleStubLeakTests`（D-53） |
| C2 | `zip_requested` 的仲裁**写了两份**，第二份还少一级 | 读 `outbox.py` 时发现它在「tasks 行不权威」这一格里自己退到了第 3 级，把 `_zip_requested`（第 2 级）跳过去了 | 删掉重复的那一份，仲裁只留 `relay._emit_outbox` 一处（D-54） |

**C1 的变异验证**：把任意一行改回 `_stub`，看守立刻报
`['test_session_slot.py:77 -> worker.parser']`。

**C2 的变异验证**：恢复抢答写法，新用例
`test_blank_task_zip_consults_the_meta_before_the_payload_zip` 报
`assert '10001' == '90210'` —— **落库的是错的邮编**，而且被记成了另一种故障
（`zip_requested_mismatch`）。既有用例发现不了：它的两个邮编恰好相同。

### 顺带复核、结论为「不是缺陷」的

| 项 | 交付说明里的说法 | 实测结论 |
|---|---|---|
| `/ack-prune` 要求 `gen` 匹配，「gen 变过之后运维无法清掉旧闩锁」 | 被列为「你可能想推翻的判断」 | **不是缺陷。** 闩锁跨 gen 变化存活，用 `/status` 上**当前**的 gen 就能清掉（消费者本来就是从那里读 gen 的）；只有拿**过期** gen 才 409，那正是预期的守卫 |
| `SYNC_ACK_SLACK_SEQ` 默认 1000（5× 契约的 `OVERLAP=200`） | 同上 | **保留 1000。** 调大是**对消费者友好**的方向（他们可以把 OVERLAP 开更大），代价只是保留期在游标附近懒一点；而调小会直接制造假 409。硬下限已经钉成 `max(配置值, 200)` |

`/ack-prune` 的实测（`scratchpad/genlatch.py`）：

```
3) 换 gen 之后 /status.gen = 'ffffdeadbeef'  retention_forced=True 未确认条数=2
4A) 用【旧】gen 确认 -> 409 'gen_mismatch'
4B) 用【当前】gen 确认 -> 200 acknowledged=[1, 2]
5) 确认之后 retention_forced=False 未确认条数=0
6) 库里条目仍在（事后可查）: 2 条，acknowledged_at=[True, True]
```

---

## 5.5 真机验证第一轮（macOS 本机，PG 17.9）

沙箱里的一切都是 Linux + PG 16 + Python 3.11。本轮换成 **macOS + PG 17.9 +
Python 3.12**，跑真代理、真 Amazon、真商品页。**四个缺陷是这一轮才现形的**，
每一个都有「为什么既有用例看不见」的解释——那比修复本身更值得记。

### 验过的

| 项 | 结果 |
|---|---|
| PG 17.9 建 schema/表/索引/分区、relay 常驻 | ✅ 无 DDL 报错 |
| 两个后端 golden | ✅ 各 64/64 |
| 两个后端 pytest | ✅ sqlite 684 passed、postgres 705 passed |
| 完整采集链路 + 契约不变量（`tools/smoke_local.py`） | ✅ 25 项全过 |
| 真实采集 50+ 个 ASIN | ✅ `parse_engine=selectolax`、`zip_verify=confirmed` |
| 后台「清空数据」在 PG 上的行为 | ✅ 与旧系统一致（`DELETE FROM sqlite_sequence` 被翻成五条 `RESTART WITH 1`） |

### 查出并修掉的

| # | 缺陷 | 为什么既有用例看不见 |
|---|---|---|
| V1 | **黄金基线是「跑测试那台机器」的函数**：开发机 `.env` 配了 `PROXY_URL`，同一份 64 步录制在两个后端上都报 4 处 `settings.proxy_url` 差异 | 夹具隔离了 settings **文件**，没隔离**环境变量喂进来的默认值**（`_default_settings()` 直接读 `config.PROXY_URL`）。沙箱没配代理所以永远绿 |
| V2 | **`slow.images` / `slow.bullet_points` 整块塞进单个元素**：6 条图片 URL 成了一个数组元素 | 分隔符有两份且已分叉。parser 四条路径全是 `"\n".join(...)`，slowhash 记的是对的（所以 `slow_hash` 一直对），只有导出适配器自带一份错的（images 按 `,`、bullets 按 `\|`）。夹具**只喂了一条** image URL，单元素下任何分隔符都"对" |
| V3 | **`<br>` 不产生分隔符**，A+ 描述里的词被粘住（`corrosion.The` / `alloythat` / `pushand` / `PullDrawer`） | 两个引擎取叶子文本都是「把子树文本首尾相接」，而 `<br>` 自身不含文本。既有用例的 HTML 里没有 `<br>` |
| V4 | **被裁空的事件流永远发不出 409**，落后的消费者会永远等下去 | 导出端点调 `_window()` 前把两个下界压成一个数（`max(... or 0, ... or 0)`），空表的 `None` 被压成 `0`，而 `_window` 判空靠的正是 `min_seq is None`。`test_incremental_export.py` 里**一条 409 用例都没有** |

四条的变异验证都做了（把修复改回去，对应用例当场红）。V1/V4 各自新增了守卫文件
（`tests/test_golden_env_isolation.py` 用 AST 扫描 config 与 `_default_settings()`
求交集；`tests/pgdb/test_export_retention_window.py` 四条覆盖被裁空/全新空库/
底部被裁/已追平消费者）。

### 一条共同的教训

V2 和 V4 是同一种错法：**抄了一份该共用的东西，然后两份各自演化**。
V2 抄的是分隔符表，V4 抄的是窗口计算。两处的"正确版本"都一直在
（`common/slowhash.py` 的 `_SPLIT_PATTERNS`、`sync.py:512` 的两次 `_window`），
只是没被复用。现在 V2 已经收敛成单一事实源（`split_multivalue()`），
V4 与 `sync.py` 对齐。

### 顺带记下的环境事实

* 目标机器要装 **PG 16**，但本机实际连的是先前就存在的 **PG 17.9**——
  `brew services start postgresql@16` 报的 `Bootstrap failed: 5` 就是端口冲突。
  17 满足全部要求（分区 + `SKIP LOCKED`），但**别去"修好"那个 16 的服务**：
  真启起来是另一个空集群。
* venv 用 **Python 3.12**。3.13/3.14 下 `curl_cffi`/`lxml`/`selectolax`/`asyncpg`
  常常还没有 wheel，pip 会掉进源码编译。
* `common/config.py:50` 的 `SERVER_HOST` 是**写死的 `0.0.0.0`**，不读环境变量。
  `.env` 里的 `SERVER_HOST=127.0.0.1` 不生效。
* `SERVER_PUBLIC_BASE` / `SERVER_FORWARDED_PREFIX` 在本仓库里**没有任何代码读**。

### 还没验的（Phase 5 S4）

`<br>` 修复只在构造的 HTML 上验过，**没在真实 Amazon 页面上做前后对比**。
工具已就绪：`tools/desc_glue_check.py --compare`，判据是精确的（修复只在 `<br>`
处插入换行，所以同一 ASIN 前后两轮「去掉换行后必须逐字节相等」，换行数之差就是
修好的处数）。重采一轮即可补上。

---

## 5.6 Phase 5 S4：新旧内容比对（真机，44 个 ASIN）

两套系统采**同一批 ASIN**，`tools/phase5_compare.py` 比对。最终 **`UNEXPECTED = 0`**。

### 结论

| 有意变更 | 真机表现 |
|---|---|
| D-61 `crawl_time` → RFC3339 | ✅ 44/44 全部变更 |
| D-58 `manufacturer` 精确匹配 | ✅ `B0D978Z8N4`：`'1 year.'` → `'SOUNDVALUE'`；新系统里**零条**仍被年龄段污染 |
| D-63 `<br>` 产出换行 | ✅ 7 条，去掉全部空白后与旧值逐字节相等 |
| B5/D-60 去容器外文本 + 去重 | ✅ 11 条变短 |
| `content_hash` | ✅ 18 条变更（输入字段变了） |

### 三个**样本没覆盖到**的有意变更（不是没生效）

这一格比"全绿"重要：它们在真机第一轮里**无从显现**，所以这一轮**没有**为它们提供证据。

| 变更 | 为什么没显现 |
|---|---|
| D-60 Amazon 自营页 `seller_*` 不再恒 `N/A` | 44 个 ASIN 里**一个自营页都没有**（卖家全是第三方，`N/A` 仅 1 条） |
| D-55 `zip_code` 恒为请求值 | 44 个邮编**全部 `verify=confirmed`**（请求值 == 观测值），两者不同时才现形 |
| D-59 `upc_list` 排序 | 非空仅 4 条，且**没有一条是多值** |

灰度期要专门盯这三项 —— 见 §7.2。

### 顺带修正的一处错误说法

比对工具原本断言 `variation_asins` 应出现排序差异。**错的**：它有两条产出路径，
兜底的正则路径（`_parse_variation_asins`）确实 `sorted(...)`，但优先走的**变体矩阵
路径**是 `",".join(dvdd.keys())` —— 文档顺序。`dict` 保插入序、本来就确定，
不受 D-59 要消的 `set` 哈希种子影响。所以"完全没变"是正常的。

### 比对工具本身在这一轮被修了四次

每一次都是真机才现形的：

1. **只取「前 N 行」** → 生产库上必然交集为空。`/api/results` 是 `ORDER BY d.id DESC`，
   而 `asin_data` 一 ASIN 一行、重采走 UPDATE，行 id 在**首次**见到时分配 ——
   于是"前 N 行" = "最近**首次收录**的 N 个"，不是"最近采集的"。测试用的 ASIN
   在旧生产库里 id 很小，永远排不进前 600。→ `--asins` / `--batch` 定向捞。
2. **没有进度输出** → 跨公网翻 60 页看起来像卡死。
3. **`/api/batches` 的键是 `name` 不是 `batch_name`** → 候选列表打出一串 `None`。
4. **`long_description` 只认「应变短」** → 把 `<br>` 修复本身报成 7 条回归。

### 已知盲点（有意不修）

**「变短」一律放行。** 变短有两个合法来源（去容器外文本、去重），而一次真的
截断也是纯删除，形态重合。试过两种自动判据——要求删除内容带 D-60 标志、
要求删除内容仍出现在新值里——在这 44 条真实数据上**都产生假阳性**。
所以不收紧：宁可这一支宽，也不要一个会误报的规则让人习惯性忽略整份报告。
人工核对靠报告末尾的抽样。

---

## 6. 门（实测输出，见 §9 复现命令）

```
### GATE 1) golden sqlite
✅ 64 步与基线完全一致
### GATE 2) golden postgres
✅ 64 步与基线完全一致
### GATE 3) pytest sqlite
684 passed, 22 skipped, 1 warning in 188.73s (0:03:08)
### GATE 4) pytest postgres
705 passed, 1 skipped, 1 warning in 239.74s (0:03:59)
### GATE 5) unittest sqlite
Ran 157 tests in 1.720s

OK (skipped=14)
### GATE 6) unittest postgres
Ran 157 tests in 4.205s

OK (skipped=12)
```

> 计数相对任务书给的基线（503/505 passed，`unittest` 未报数）高出一截，
> 因为 Phase 4 与 Phase 6 新增了 5 个测试文件：
> `test_parser_quality.py`(33) + `test_engine_not_found.py`(44) +
> `test_phase4_fields.py`(26) + `test_retention.py`(31)，
> 加上收口新增的 1 条（D-54 的邮编仲裁）与 1 条看守（D-53）。
> Phase 4 交付期一度存在的那条 `xfailed`（relay 接线哨兵）**已经消失** ——
> 接线做完之后它按设计换岗成了两条正向回归断言，见
> `tests/test_engine_not_found.py::RelayContractSentinelTests`。
>
> 相对上一版（635/637）的增量来自真机验证第一轮（§5.5）新增的守卫：
> `test_golden_env_isolation.py`(4) + `test_export_multivalue_split.py`(10) +
> `test_export_retention_window.py`(4) + `test_long_description.py` 的
> `BrSeparatorParity`(5)。两列 skip 数差得多是因为**跑这一版的机器装齐了
> `selectolax`/`lxml`**，PG 那列几乎不再 skip。

> **门的覆盖面，说清楚**：`unittest discover` 的加载器只认 `TestCase` 子类，
> 所以 `tests/pgdb/`、`tests/golden/`、`tests/test_slowhash.py` 下的函数式用例
> 它**一条都收不到**（`tests/test_runner_parity.py` 显式 skip 并报数，不让它们
> 无声消失）。**「unittest OK」不等于「仓库是绿的」**，Postgres 存储层整层靠 pytest。
>
> **黄金那两行对 Phase 4 / Phase 6 是零信息量**，理由见 §2.3 R9 与
> `OWNERSHIP.md` Phase 4 节的抬头。两个阶段各自带独立取证。

---

## 7. 切换运行手册（Phase 5）

### 7.0 工具与手册

工具与手册已交付（本文只留摘要，操作以手册为准）：

| 交付物 | 用途 |
|---|---|
| `docs/phase5_runbook.md` | 完整运行手册：前置体检 → 并行比对 → 事件流对账 → 切换 → 回滚 → 上机后盯的指标 |
| `tools/phase5_preflight.py` | 目标机器体检。**实测**而非读配置：PG 版本/扩展/编码/排序规则、能否建分区表、advisory lock、路由顺序、依赖、磁盘 |
| `tools/phase5_compare.py` | 新旧两套系统的内容比对，把差异分成 EXPECTED / VOLATILE / UNEXPECTED |
| `tests/test_phase5_compare.py` | 比对工具分类逻辑的 13 个用例 |

并行比对**已经跑完并通过**（§5.6，44 个 ASIN，`UNEXPECTED = 0`）。
Phase 5 要做的（`.agent/pg_migration_plan.md` §Phase 5）：

1. 新 PG 系统与旧 SQLite 系统**同时**采同一批 ASIN（各自独立 worker），比对**内容**差异。
   —— 这是唯一还没有任何证据覆盖的环节。黄金证明不了它。
2. 黄金样本回归（T15）。
3. 事件流对账：`/counts` 的 `count` 与 `scrape_events` 直查一致。
4. 顺带把 N1（`site` 值域）在并行期定掉：两套系统同时在跑，回滚成本最低。

### 7.1 两条部署路径 —— 先确认你走的是哪一条

| 路径 | 场景 | 步骤 |
|---|---|---|
| **A 全新部署**（本项目实际采用） | 另起一台机器 + 全新 PG 库 + 全新 worker；旧系统在原机器上继续跑不动。**同一套系统内不存在新旧混跑** | §7.1a，四步准备 + 三项验收 |
| **B 原地灰度** | 在现有生产部署上就地换代码，worker 与 server 分开部署、分批替换 | §7.1b，九步 |

**为什么路径 A 用不着那九步**：那九步的承重约束只有一条 —— worker 与 server
独立部署时，灰度期两种提交体必然同时在线，所以 D-61（worker 写 RFC3339）必须在
D-41（relay 认双格式）**之后**上线。全新部署里两边都是新代码，这条自动满足；
relay 的双格式支持退化成无害的保险，不再是排序约束。

---

### 7.1a 路径 A：全新部署

**准备（顺序无所谓，但都得做）**

| # | 动作 | 为什么不能省 |
|---|---|---|
| 1 | 建库时带 `LC_COLLATE='C' LC_CTYPE='C' TEMPLATE template0` | **只能建库时定**，事后改要重建库。PG 默认排序规则与 SQLite 的 `BINARY` 不同，`TEXT` 列的 `ORDER BY` 会给出不同顺序 —— 分页、导出、搜索全会跟旧系统对不上 |
| 2 | 配 `SCRAPER_INSTANCE_ID` | **永不自动铸造**（T12）。它是人用来区分两个克隆部署的，不配则两套部署的事件流无法区分 |
| 3 | 装齐 `selectolax`（以及 `lxml`） | `selectolax` 是**生产解析引擎**。缺了 worker 走 lxml 回退，采出来的数据与 §5.6 比对过的**不是同一套** |
| 4 | 跑 `python tools/phase5_preflight.py` | 实测而非读配置。硬失败必须修；特别看「排序规则」那行 |

**验收（起服务之后，三项）**

| # | 看什么 | 正常 |
|---|---|---|
| 1 | 启动日志 | 出现 `事件流就绪：gen=… instance_id=<你配的>`、`PostgreSQL 连接就绪`、`relay 启动`。若出现 `FTS5` / `WAL 维护协程` / `只读连接池就绪：3 条连接`，说明**跑在 SQLite 上**，`DB_BACKEND` 没生效 |
| 2 | `tools/smoke_local.py` | 25 项全过（上传→拉任务→提交→查结果→增量导出契约） |
| 3 | 真跑几小时后看 `/api/v1/sync/status` | `relay_state=running`、`outbox_depth` 有涨有落不单调增长、`collected_at_fallback == 0`、`zip_requested_mismatch == 0`、`dead_letters == 0` |

**接 catalog_sync（这一步仍然要分两段，理由是实的）**

| # | 动作 | 为什么 |
|---|---|---|
| 1 | 先让沃尔玛侧**只读拉取，不 ack**，连续跑一段时间 | 确认 `source_id` 无冲突、游标严格推进、不出 409 |
| 2 | 确认无误后再接 `/ack` | **ack 驱动保留期裁剪**。裁掉的分区拿不回来，所以必须先证明消费侧真的消费到了，再让它去推保留期下界 |

**灰度期专项确认三项**（§5.6 的真机比对样本没覆盖到，见 §7.2 末尾的表）。

---

### 7.1b 路径 B：原地灰度（**顺序是承重的**）

> 两条耦合约束决定了这个顺序，不要重排：
> **(a)** worker 与 server 独立部署，灰度期两种提交体必然同时在线；
> **(b)** D-61（worker 写 RFC3339）必须在 D-41（relay 认双格式）**之后**上线，
> 否则每一条记录的 `collected_at` 都会退回 `recorded_at` 兜底。

| # | 动作 | 校验点（不过就停下） |
|---|---|---|
| 1 | **先建库**：新 PG 库用 `LC_COLLATE=C` 创建（D-10） | `SHOW lc_collate;` → `C` 或 `C.UTF-8` |
| 2 | 配 `SCRAPER_INSTANCE_ID`（**永不自动铸造**，它是人用来区分两个克隆的，T12） | 起服后 `/api/v1/sync/status` 的 `instance_id` **不是** `unconfigured` |
| 3 | **部署 server**（含 D-39..D-43 + Phase 6），`DB_BACKEND=postgres` | `/api/_debug/event-stream` 的 `relay_state == 'running'`；`/api/v1/sync/status` 回 200 且 `gen` 非空 |
| 4 | 观察 30 分钟，**worker 先不动** | `outbox_depth` 不单调增长；`dead_letters == 0` |
| 5 | **再灰度 worker**（含 D-55..D-62），先放一台 | `/records` 里出现 `parse_engine != null` 的记录；`collected_at_fallback` **仍为 0** |
| 6 | 单台观察 30 分钟 | `collected_at_legacy_cst` 与 `collected_at_naive_utc` 都在动 = 双格式并存正常；`zip_requested_mismatch` **应为 0** |
| 7 | 全量放 worker | `collected_at_legacy_cst` 停止增长 = 老 worker 全部下线 |
| 8 | **通知沃尔玛侧撤掉 `completeness_ok` 旁路**（契约 §0.1 第 4 行） | 对面 `products` 入库量**下降一截**——那是**正确的**，少掉的是没测量过的记录 |
| 9 | 首次 ack 之后再放开保留期观察 | `/status.retention.ack_floor_seq` 不再是 `null`；`forced_prune_log` 为空 |

### 7.2 要盯的指标（前 48 小时）

| 指标 | 位置 | 正常 | 不正常意味着 |
|---|---|---|---|
| `relay_state` | `/api/_debug/event-stream` | `running` | `failed`/`refused` = 事件流停摆，而 HTTP 全绿 |
| `outbox_depth` | 同上 | 有涨有落 | **单调增长** = relay 没在消费 |
| `consec_tick_fail` | 同上 | 0 | 状态翻转前的先兆 |
| `dead_letters` | 同上 / `/status` | 0 | 有毒丸行被隔离，body 逐字节留着，去看 ERROR 日志 |
| `collected_at_fallback` | 同上 | **恒 0** | 有 worker 交的 `crawl_time` 两种格式都不是 |
| `zip_requested_mismatch` | 同上 | **恒 0** | per-ASIN 邮编没生效（worker 没读到 tasks 行的值） |
| `min_available_seq` / `max_seq` | `/status` | 都在涨 | — |
| `retention.effective_floor_seq` | `/status` | 落后 `ack_seq` ≥ 1000 | 追平了 = 消费侧要开始吃假 409 |
| `forced_prune_log` | `/status` | `[]` | **非空 = 真的丢了数据**，按契约 §5.1 处理，不是假阳性 |
| `lock_timeouts` | `/status.retention.counters` | 偶发可接受 | 持续增长 = 保留期一直抢不到锁，分区会堆积 |
| 磁盘 | `free_disk_bytes` | 远高于 `disk_floor_bytes` | 逼近 = 会触发**越过 ack 的强制裁剪** |

**另外三项要专门盯**，因为 §5.6 的真机比对**样本没覆盖到**它们 —— 那一轮
`UNEXPECTED = 0`，但对这三条来说是"无从显现"，不是"已验证"：

| 有意变更 | 灰度期怎么确认 |
|---|---|
| D-60 Amazon 自营页 `seller_*` 不再恒 `N/A` | 抽一条自营商品（`seller_name` 应为 `Amazon.com`、`seller_id` 为 `AMAZON`）。真机第一轮 44 个 ASIN 里一个自营页都没有 |
| D-55 `zip_code` 恒为请求值 | 找一条 `zip_verify != 'confirmed'` 的记录，确认 `zip_code` 仍是**请求的**那个邮编。第一轮 44 条全部 `confirmed`，两者相同故无差异 |
| D-59 `upc_list` 排序 | 找一条多值 `upc_list`，确认升序。第一轮非空仅 4 条且全是单值 |

### 7.3 回滚

> **回滚的难点不是把服务切回去，是别让沃尔玛侧把回滚读成数据丢失。**

| 场景 | 做法 |
|---|---|
| **步骤 3-4 失败**（server 起不来 / relay 不转） | 直接把 `DB_BACKEND` 切回 `sqlite` 重启。worker 还没换，`common/database.py` 一个字节没改，**无损**。SQLite 上 `/api/v1/sync/*` 回 503，消费侧按契约退避重试 |
| **步骤 5-7 失败**（worker 侧数据不对） | 只回滚 worker（server 保持 PG）。relay 认双格式（D-41）、认哨兵标题（D-40），**老 worker 的提交体全程被支持**——这正是那两条决策存在的理由。⚠ **D-57 与 D-60 必须一起回滚**，只回滚 D-57 会让四列开始在每次 404 上被刷成 N/A |
| **路径 A 出问题** | 旧系统全程没被碰过，直接停用新系统即可 —— 这正是全新部署相对原地灰度的**主要优势**。代价是两套系统并行期间的采集配额 |
| **切换后想退回 SQLite**（路径 B） | ⚠ **PG 期间的数据不会回到 SQLite 库里。** 两个库是独立的，没有反向同步。退回 = 丢掉 PG 期间采集的全部数据（`asin_data` 每 ASIN 一行、无版本）。**只在切换后极短时间内可行**，超过一个采集周期就应该向前修而不是回滚 |
| **库回滚 / 从备份恢复** | 起服时 `_seq_high_water()` 会检出回退并**铸新 `gen`**，同时把序列 `setval` 推过历史高水位。消费侧按契约 §5.5 硬停 + 全量对账。**这是预期行为，不是故障** |
| **整机快照回滚** | 服务端**检不出**（B1）。唯一防线是消费侧的 `max_seq` 单调检查。回滚后必须**主动通知**沃尔玛侧做全量对账，不能指望自动检出 |

### 7.4 切换后第一件事

按优先级：

1. **N6**（captcha / blocked 同样穿过 `_is_parse_failure`）——SQLite 路径退役后
   `common/database.py` 的冻结约束解除，可以从根上修。
2. **R1**（`/api/tasks/release` 的整数强转）——同上，`as_int` 加到端点上即可。
3. **R5** 随 SQLite 路径退役自然消失。
4. Phase 1.5 的 N2 / N3 / N4。

---

## 8. 文件落点速查

| 关注点 | 文件 |
|---|---|
| 决策台账（**冲突时以它为准**） | `common/pgdb/OWNERSHIP.md` |
| 交付沃尔玛侧的契约 | `docs/sync_contract.md` |
| 计划 + 「计划错在哪里」 | `.agent/pg_migration_plan.md` |
| 原始审计（Phase 4 大部分条目的来源） | `.agent/catalog_sync_audit.md` |
| 存储层 | `common/pgdb/`（`pool` / `schema` / `tasks` / `results_*` / `batches` / `media`） |
| 事件流 | `common/pgdb/relay.py` + `outbox.py` + `common/slowhash.py` |
| 保留期 | `common/pgdb/retention.py` |
| 同步 API | `server/api/sync.py` |
| worker 侧质量 | `worker/parser.py` + `worker/engine.py` |
| 黄金基线（**永不重录**） | `tests/golden/samples/sqlite_baseline.json` |

---

## 9. 复现命令

```bash
V=/home/user/amazon-scraper-v3/.venv/bin/python

# 六道门
$V -m tests.golden.run verify                          # -> ✅ 64 步与基线完全一致
DB_BACKEND=postgres $V -m tests.golden.run verify      # -> ✅ 64 步与基线完全一致
$V -m pytest tests/ -q
DB_BACKEND=postgres $V -m pytest tests/ -q
$V -m unittest discover -s tests
DB_BACKEND=postgres $V -m unittest discover -s tests

# 收口阶段的两个缺陷（C1 收集顺序 / C2 邮编仲裁）
$V -m pytest tests/test_session_slot.py tests/test_engine_not_found.py -q   # 两个顺序都必须 75 passed
$V -m pytest tests/test_engine_not_found.py tests/test_session_slot.py -q
$V -m pytest tests/test_runner_parity.py -q
$V -m pytest tests/pgdb/test_phase4_fields.py -q -k zip

# PG 起不来
pg_ctlcluster 16 main start
```

**标准规矩（还在生效）**：不改 `common/database.py`；
不重录 `tests/golden/samples/sqlite_baseline.json`；
每一条主张都要有实测输出。
