# 改造计划（五份计划 + 五份对抗验证的合成版）

> 输入：5 份计划共 **50 条 proposal**（性能 6 / 错误规范 8 / 分层拆分 15 / 去重 13 / SQLite 退役 8），
> 对抗验证给出 51 条 verdict（去重 P3 被拆成 a/b 两条判）。
> 本文只执行 **verdict=SAFE** 的 30 条；其中 2 条是「明确不做」、2 条是「只登记不执行」，
> **本轮真正落地 26 条**。被驳回的 20 条在 §5，逐条写明「错在哪、要重提得先修什么」。
>
> 体例照 `.agent/MIGRATION_STATUS.md`：带 file:line、带实测、明确写出刻意没做什么。
> 文中凡标「（已复核）」的行号是我在本仓库重新 grep 过的；其余沿用各份 verify 的实测结论。

---

## 0. 三十秒版

**这轮要做什么**

1. **先把验证成本和验证网整好**（Phase 0）：CI 的 sqlite 那一列砍掉重复的 PG 用例（省 184s）；
   趁 SQLite 还在，把 `tests/pgdb/test_results_read.py` 那 88 条「以 SQLite 为唯一裁判」的差分用例
   **录一份快照**——它同时是本轮所有「可证明等价」的查询改写的第二道网。
2. **性能三条**（Phase 1）：全是纯索引 / 可证明等价的改写，无一条动响应形状。
   `pull_tasks` 164.6ms→0.075ms（在全局写锁内）、`_hydrate` 62.7ms→0.207ms、`change_filter` 736ms→0.349ms。
3. **错误规范四条**（Phase 2）：8 处裸 `pass` 补日志、全局 500 换成结构化 JSON、错误码注册表 + 漂移用例、
   最后**有意扩黄金基线**（15+ 个错误路径站点，纯 append）。
4. **分层拆分十条**（Phase 3）：`server/app.py` 2838 行（已复核）拆成 8 个 `server/api/*.py`，
   **openapi.json 逐字节不变、黄金零重录**；末尾把 49 处 `db._db` 分五批收进 db 方法。
5. **去重九条**（Phase 4）：先建 `common/core/`（唯一真源的家），再把 ASIN 正则 / 时间戳 / RFC3339 /
   completeness 位 / 邮编 / 批次名收成一份。

**这轮不做什么**

- **不退役 SQLite。** 退役计划的可执行核心（第 1/3/4/5 步）四条全部没通过验证：
  第 1 步的 `from common.domain import *` 拿不到 15 个下划线符号；第 3/5 步违反 C4（只给 PG 一侧）；
  第 4 步的删除清单漏了 `tests/golden/harness.py:234-243`——**照它做会把黄金回归网自己删掉**。详见 §5 与 §7。
- **不碰 `worker/parser.py` 一个字节**（C3 / D-27）。
- **不删、不改名任何既有端点**（erpAPI 清单未到）。所有删除候选集中登记在 §6。
- **不做「为兼容旧调用方而设的可选参数」**——性能计划的 P4/P5 就是因为这个被驳回（C1 第一句）。
- **不做仪表盘默认查询（batch_id 翻页 262ms）的修复**——唯一的方案 P6 事实有误被驳回，见 §5 与 §6。

**为什么是这个顺序**

用户给的顺序（性能 → 错误规范 → 拆分 + 去重）**基本正确，保留**。三处调整：

- **前面加一个 Phase 0**。不加它，后面每道门慢 40%，而且「diff blob 就是评审物」这句话有三处是空头支票（见 §6 盲点 B2）。
- **SQLite 退役不排进本轮**，理由不是「太晚」而是「它的步骤没通过验证」。
  它的两条 SAFE 分量（CI 分列、录裁判快照）提前到 Phase 0；
  它真正的前置（把共享符号搬出 `common/database.py`）由去重 P9 完成——做完之后退役是一次 `git rm`。
- **去重 P9（`common/core/`）可以随时提前**，它与其余每一步零文本冲突，且是新建模块的落点裁决（否则 P1/P4/P5/P6 建的四个新模块要搬第二次）。

---

## 1. 全局约定：怎么验、什么算通过

每一步的**基础门**（下文简称 `GATES`，照 `.agent/MIGRATION_STATUS.md` §6 的六道）：

```bash
# GATE 1/2  黄金：record 只在 sqlite 侧跑，PG 侧只跑 verify
python -m tests.golden.run selfcheck            # 连跑两遍，专测「夹具补丁是否打空」
python -m tests.golden.run verify               # 非 strict，一次给出全部差异
DB_BACKEND=postgres python -m tests.golden.run verify
# GATE 3/4  pytest 两列
pytest tests/ --ignore=tests/pgdb -q            # sqlite 列（Phase 0 之后）
DB_BACKEND=postgres pytest tests/ -q            # postgres 列，跑全树
# GATE 5/6  unittest 两列（加载器只认 TestCase 子类，不等于仓库全绿）
python -m unittest discover -s tests
DB_BACKEND=postgres python -m unittest discover -s tests
```

三条硬规矩，写在这里，下文不再重复：

1. **`tests/pgdb/` 才是 C4 的真执行机制，不是黄金。** 五份计划里只有一份提到它。
   `tests/pgdb/test_results_read.py:267 test_get_results_matches_sqlite`（50 条参数化）与
   `:318 test_iter_results_matches_sqlite`（25 条）逐字段对比两个后端，参数矩阵覆盖 `batch_id × change_filter` 全组合。
   **Phase 1 的每一条都撞在它上面。**「先跑一遍黄金全绿」是不够的，必须 `pytest tests/pgdb -q` 也全绿。
2. **黄金只有一份基线**：`tests/golden/run.py:28 BASELINE = .../samples/sqlite_baseline.json`（已复核），
   `samples/` 下只有这一个文件，两个后端 verify 比的是同一份。
   推论一：`record` 在 `DB_BACKEND=postgres` 下会**直接覆盖 sqlite 基线**——任何重录必须在 sqlite 侧做。
   推论二：**任何新增步骤，两个后端的响应必须逐字节相同，否则根本加不进去**（这是选步的硬筛，不是加完再调）。
3. **breaks_golden=yes 的步骤流程**：先在提交信息/PR 描述里写明「改了什么行为、为什么」→ 再 `record` →
   基线 blob 的 diff 就是评审物。**未经声明的飘红一律先查错，不许顺手重录。**

**回退的通用形态**：每一步一个独立提交，回退 = `git revert <sha>`；纯 DDL 的步骤回退 = `DROP INDEX`（下文给具体语句）。
凡是「一个提交里同时改生产代码和夹具/守卫用例」的步骤，回退必须整提交回，不能只回一半。

---

## 2. 执行顺序总览

| # | 步骤 | 来源 | breaks_golden | 依赖 | 规模 |
|---|---|---|---|---|---|
| **0.1** | CI sqlite 列改 `--ignore=tests/pgdb` | sqlite-exit 第0步 | no | — | S |
| **0.2** | 录 SQLite 裁判快照（**只录不拆**） | sqlite-exit 第2步（改造） | no | — | M |
| **0.3** | 把「单一基线 / record 只在 sqlite 侧」写进 `tests/golden/run.py:28` 注释 | verify missed | no | — | S |
| **1.1** | `screenshots` 部分索引 | 性能 P2 | no | — | S |
| **1.2** | `pull_tasks` 拆排序 + `idx_tasks_pull` | 性能 P1 | no | — | S |
| **1.3** | `change_filter` 改 EXISTS + 索引 | 性能 P3 | no | — | M |
| **2.1** | 8 处裸 `pass` 补日志 | 错误 P7 | no | — | S |
| **2.2** | 全局 500 处理器 | 错误 P3 | no | — | S |
| **2.3** | 错误码注册表 + 漂移用例 | 错误 P4（改造） | no | — | S |
| **2.4** | **扩黄金：错误路径 15+ 站点** | 错误 P6 | **yes** | 2.1–2.3 之后 | M |
| **3.1** | `server/api/pages.py` | 拆分 S2 | no | 2.4 | S |
| **3.2** | `server/api/debug.py` | 拆分 S3 | no | 3.1 | S |
| **3.3** | `server/api/fleet.py` | 拆分 S7 | no | 3.1 | S |
| **3.4** | `server/api/sellers.py` | 拆分 S6 | no | 3.1 | M |
| **3.5** | `server/api/worker_queue.py` | 拆分 S8 | no | 3.4 | M |
| **3.6** | `server/api/results.py` | 拆分 S9 | no | 3.1 | M |
| **3.7** | `server/api/export.py` + 顺序守卫升级 | 拆分 S11 + S11b | no | 3.1–3.6 全绿 | L |
| **3.8** | 裸 SQL 五批收口 | 拆分 S12 | **unsure**（分批判） | 3.6 / 3.7 | L |
| **4.1** | `common/core/`（真源搬家） | 去重 P9 | no | — （可提前） | M |
| **4.2** | `common/core/idents.py` ASIN 正则 | 去重 P1 | no | 4.1 | S |
| **4.3** | `common/core/timeutil.py` + 修 3 处本地时钟 | 去重 P4 | no | 4.1 | M |
| **4.4** | RFC3339 `_iso` 去重 | 去重 P5 | no | 4.3 | S |
| **4.5** | completeness 位常量收一份 | 去重 P6 | no | 4.1 | S |
| **4.6** | `app.py:1591` 邮编改调 `_normalize_zip` | 去重 P7 | no | — | S |
| **4.7** | 批次名收一份 + 统一秒精度 | 去重 P12 | no | 4.3 | S |
| **4.8** | 字段表守卫测试 + 补 `title_bullets_hash` | 去重 P10 | no | — | S |
| **X.1** | 删除候选登记（不执行） | 拆分 S13 + 去重 P11 | — | — | S |

依赖关系，用一句话说清：
**0.x 是所有后面步骤的验证前提；2.4 必须在 Phase 2 的其余三条之后（否则每加一个 handler 重录一次，评审物被噪声淹掉）、在 Phase 3 之前（它是拆分的网）；3.7 必须最后拆（承重的路由顺序）；3.8 必须在 3.6/3.7 之后（收口的边界由那两个模块划出）；4.1 必须在 4.2–4.5 之前（否则四个新模块要搬第二次）。**

---

## 3. 分阶段详述

### Phase 0 — 先把网和成本弄对（3 步）

#### 0.1 CI 的 sqlite 列改成 `pytest tests/ --ignore=tests/pgdb`

- **改什么**：CI 配置里 `DB_BACKEND=sqlite` 那一列的 pytest 命令。
  依据：`tests/pgdb/conftest.py:41-49` 的 `pgdb` 夹具**不读 `DB_BACKEND`**（只 `importorskip('asyncpg')` + 建临时库），
  所有在意后端的用例都自己 `monkeypatch.setenv`（`tests/pgdb/test_sync_api.py:37,938,965`、
  `test_retention.py:54,489,907`、`test_export_retention_window.py:38`）。
  所以这 411 条在两列里逐条相同。
- **怎么验**：`pytest tests/ --ignore=tests/pgdb -q` → 期望 `285 passed, 22 skipped`（verify 本机实测 4.61s / 我这侧 4.77s）；
  `tests/golden/test_golden.py` 在 `tests/` 根下不受 `--ignore` 影响，GATE 1 照跑。
- **失败怎么回退**：改回原命令，零代码改动。
- **必须写进 CI 注释的一句**：这一列从此**不再实际执行任何 SQLite 存储层代码**
  （`test_admin.py:400` 的 `run_sqlite()` 与 `test_results_read.py:174` 的 `seeded_sqlite` 都在 `tests/pgdb` 下），
  SQLite 的存储层信号只剩 GATE 1 的黄金 64 步。免得后来人误以为那一列还在守 SQLite。
- **收益**：六道门总时长省约 184s（GATE 3 从 188.73s 降到约 5s）。
  **顺带的作用**：这一步做完，「退役 SQLite 能省 190s CI」这条理由自己蒸发——让 §7 的算账建立在真数字上。

#### 0.2 录 SQLite 裁判快照（**改造：只录，不拆**）

- **改什么**：趁 SQLite 还在，把 `tests/pgdb/test_results_read.py` 里依赖 `seeded_sqlite` 夹具（`:174`）的
  **93 条**用例（`test_get_results_matches_sqlite` 50 + `test_iter_results_matches_sqlite` 25 +
  `test_simple_reads_match_sqlite` 13 + `test_count_bug_is_reproduced` 3 + `test_short_term_search_with_cursor_still_works` 2，
  其中纯 diff **88 条**）的 SQLite 侧返回序列化成一份 JSON 期望文件；
  `tests/pgdb/test_admin.py:400-414 run_sqlite()` 的输出同样录一份。
  做法照抄 `tests/golden/run.py:38-46` 的 record。
- **与原提案的唯一偏差（必须声明）**：原提案要求「之后这些用例改成对着录制文件断言，SQLite 侧夹具删除」。
  **本轮不删夹具。** 理由：SQLite 去留未决（C2），删掉夹具等于提前替 C2 做决定；
  而差分用例正是 Phase 1 三条查询改写的主要回归网（见 §1 规矩 1）。
  本轮只产出快照文件 + 一条「PG 侧对快照」的新用例，两套并存。
  退役真被批准时，删夹具是一行操作。
- **顺带的收益（原计划没算到）**：这份快照是**改动前的答案**，因此它同时是 Phase 1「可证明等价」的第二道网——
  等价性推理错了，它会红，而黄金不会（黄金的搜索样本全是单一大小写 ASCII，`brand` 恒为 `GoldenBrand`）。
- **文件头必须写明**：期望值来源是 SQLite，不是「我们认为对的」；任何修改要重新论证。
  并且必须点名夹具里这批数据各自守的是什么：
  `B0BSLASH05 'back\\slash item'` 守 D-16（`ESCAPE ''`）；
  `B0ACCENT06 'ÉCLAIR CAFÉ'` / `B0ACCENT07 'éclair café'` 守 D-5（`ascii_lower` 而非 `ILIKE`）；
  `B0PCT08 'Gol%rand pct'` / `'Pct_Brand'` 守 LIKE 通配符字面量。
  （原计划只提了反斜杠和大小写，把后两组的意义漏了一半。）
- **怎么验**：`DB_BACKEND=postgres pytest tests/pgdb/test_results_read.py tests/pgdb/test_admin.py -q` 全绿；
  故意把快照里某一行改一个字符，确认新用例会红（证明它真的在比）。
- **失败怎么回退**：删快照文件与新用例，原差分用例一行没动。
- **顺序性**：这是全案唯一一处「做晚了不可逆」的地方——不过既然本轮不退役，它的紧迫性来自
  「它是 Phase 1 的第二道网」，所以仍然排在 Phase 1 之前。

#### 0.3 基线护栏（来自各份 verify 的 missed，不是任何一条 proposal）

- **改什么**：`tests/golden/run.py:28` 上方加注释，写死三件事：
  (a) 只有一份基线，两后端共用；(b) `record` 只允许在 `DB_BACKEND=sqlite` 下跑，PG 侧只跑 `verify`；
  (c) 新增步骤的两后端响应必须逐字节相同，否则不许加。
  同时把全仓「openapi 是黄金第 5 步」的错误说法改对——**真值是第 11 步**（已复核：
  `tests/golden/samples/sqlite_baseline.json` 第 5 步是 `results_empty`，第 11 步才是 `openapi_schema`）。
  错误副本至少在 `server/app.py:264`、`server/api/export_incremental.py:95` 附近的注释里。
- **怎么验**：`GATES` 全绿（纯注释）。
- **失败怎么回退**：`git revert`。

---

### Phase 1 — 性能（3 条，全部纯索引或可证明等价）

三条互相独立，可以三个独立提交，按风险升序排。**每一条都必须跑 `pytest tests/pgdb -q`**，
因为黄金对它们几乎是瞎的（`tests/golden/scenario.py:212-233` 的六次 `/api/results` 一次都没传 `batch_id`，
`change_filter` 只测了 `new`；`scenario.py:249-257` 的导出没有一条带 `change_filter`）。

#### 1.1 `screenshots` 补 `(asin) WHERE status='done'` 部分索引

- **改什么**：`common/pgdb/schema.py:264-265`（`DDL_INDEXES` 里 screenshots 那两行旁边）与
  `common/database.py:589-590` 各加一行
  `CREATE INDEX idx_screenshots_asin_done ON screenshots(asin) WHERE status='done'`。
  `_get_done_screenshot_paths` 的 SQL（`common/pgdb/media.py:445-449`）**一个字不改**。
- **为什么**：`screenshots` 上今天只有 `idx_screenshots_status(status)`、`idx_screenshots_batch(batch_id)`
  和建表里的 `UNIQUE(batch_id, asin)`，**没有任何以 asin 打头的索引**。
  `media.py:467-469` 的第二次 `await load()` 不带 batch 过滤、无条件执行 → PG 16 无 skip scan → 全表扫。
  实测 62.7ms → 0.207ms（Parallel Seq Scan 3847 buffers → Index Scan 154 buffers）。
- **收益比原计划算的更大（verify 补的）**：`common/pgdb/results_read.py:294-304` 的 `get_result_by_asin`
  调 `_hydrate_screenshot_paths([item])` 时**不传 batch_id**，所以 `/api/results/{asin}`（很可能正是 erpAPI 在用的端点）
  每次也吃这条全表扫描。
- **怎么验**：
  ```bash
  # 改前改后各一次
  psql "$PG_DSN" -c "EXPLAIN (ANALYZE,BUFFERS) SELECT asin,file_path FROM screenshots WHERE status='done' AND asin = ANY(ARRAY['B0X']::text[])"
  DB_BACKEND=postgres pytest tests/pgdb -q && pytest tests/ --ignore=tests/pgdb -q
  # 全套 GATES
  ```
  期望：计划从 `Parallel Seq Scan` 变成 `Index Scan using idx_screenshots_asin_done`。
- **失败怎么回退**：`DROP INDEX idx_screenshots_asin_done;` + `git revert`。
- **C4**：两侧都加。SQLite 侧本来靠 `UNIQUE(batch_id,asin)` 的跳跃扫描（0.32ms→0.06ms），
  但**跳跃扫描依赖 `sqlite_stat1`**（`common/database.py:379` 的 `PRAGMA analysis_limit=400` + 启动期 ANALYZE），
  加了这个索引就不再依赖统计信息，顺带把这处依赖去掉。

#### 1.2 `pull_tasks` 拆排序 + `idx_tasks_pull`

- **改什么**：
  - 索引：`common/pgdb/schema.py:256-262` 加
    `CREATE INDEX idx_tasks_pull ON tasks(status, priority, zip_code NULLS FIRST, id)`；
    `common/database.py:567-573` 加同名索引（**SQLite 不写 NULLS 修饰符**，默认即 NULLS FIRST；
    PG 的 ASC 默认是 NULLS LAST，这处不对称 `common/pgdb/tasks.py:57-58` 已经踩过一次）。
  - 查询：`common/pgdb/tasks.py:255-262` 的 `order_clause` + `:266-278` 的候选 SELECT，
    `prefer_zip` 非空时拆成两条：
    Q1 `... AND t.zip_code = ? ORDER BY t.id ASC LIMIT count`；
    不足再发 Q2 `... AND t.zip_code IS DISTINCT FROM ? ORDER BY t.zip_code ASC NULLS FIRST, t.id ASC LIMIT (count-len(Q1))`。
    `prefer_zip` 为空时不拆，原 SQL 直接命中新索引。
    SQLite 侧同构，在 `common/database.py:1185-1206`。
- **两处原计划没写的 C4 不对称（verify 补，必须照办）**：
  (a) `IS DISTINCT FROM` 需要 SQLite ≥3.39，**SQLite 侧必须写 `t.zip_code IS NOT ?`**；
  (b) 「两条 `FOR UPDATE OF t SKIP LOCKED` 照旧」只对 PG 成立，SQLite 侧没有这东西。
- **等价性**：`CASE WHEN NULL='x'` 落 ELSE → 组 1，正好被 Q2 的 `IS DISTINCT FROM` 收走；组内 zip 恒定，故 `id ASC` 等价。
- **怎么验**：
  ```bash
  DB_BACKEND=postgres pytest tests/pgdb/test_tasks.py -q        # test_pull_tasks_prefer_zip_wins:135 是唯一覆盖被拆分支的用例
  psql "$PG_DSN" -c "EXPLAIN (ANALYZE,BUFFERS) <Q1>"            # 期望 external merge Disk 消失
  curl -s localhost:PORT/api/_debug/lock-stats                  # 改前改后各取一次，比 pull_tasks 的持锁分位
  ```
  **收益必须用 `/api/_debug/lock-stats` 取数验收**（它被 `tests/golden/scenario.py:238` 记进基线，是现成的度量入口）。
  原计划「每天从写锁里拿回 27 分钟」是拿 10 万任务/天 × count=10 推的，仓库里没有 `pull_tasks` 调用次数的度量来源——
  不用这个端点取数，这个数字无法验收。
- **失败怎么回退**：`git revert`（查询与索引同一提交）+ `DROP INDEX idx_tasks_pull;`。
- **黄金**：不会红。`tests/golden/scenario.py:158-160` 调 `/api/tasks/pull` 不传 `prefer_zip`
  （`server/app.py:1569` 是 `Query(None)`，`:1590-1592` 校验后 `pz=None`），被拆的那条分支根本不进基线。
  基线里的行序（10001→id 1,3 / 60601→id 7,8 / 90210→id 2）是无偏好分支产出的，形状不变。

#### 1.3 `change_filter` 的 `JOIN (SELECT DISTINCT ...)` 改 EXISTS + `asin_changes(change_type, asin)` 索引

- **改什么**：六处 join 改写 + 一个索引。
  翻页：`common/pgdb/results_read.py:169`、`:177`、`:191`；`common/database.py:2144`、`:2152`、`:2166`。
  批次导出：`results_read.py:352-353`、`:358-359`；`database.py:2381-2382`、`:2387-2388`。
  全量导出：`results_read.py:409-416`（SQL 在 `:422`）；`database.py:2438-2445`。
  索引：`common/pgdb/schema.py:252-254` + `common/database.py` 的 `asin_changes` 索引块。
- **三处必须照办的修正（verify 提供）**：
  1. **别照抄提案给的 SQL 形状**：提案正文写 `ac ON ac.asin = d.asin`，但导出那四处
     （`results_read.py:352-353/:358-359`、`database.py:2381-2382/:2387-2388`）实际是 `ac ON ac.asin = ba.asin`
     ——驱动表是 `batch_asins` 不是 `asin_data`。
  2. **参数绑定顺序会变**：谓词从 `join_parts` 挪进 `where_parts`，而 `params = join_params + where_params`
     （`results_read.py:236` / `database.py:2217`）。
  3. **D-8 的 count 过滤必须复查**：`results_read.py:276` / `database.py:2254` 的
     `[p for p in where_parts if "d.id" not in p]` 是**刻意保留**的缺陷复现。
     EXISTS 文本里是 `d.asin` 不含 `"d.id"`，所以不会被误剔、D-8 行为得以保留——
     但这是推理不是测量，**必须实测 `tests/pgdb/test_results_read.py:278 test_count_bug_is_reproduced` 仍绿**。
- **注意**：只加索引不改写只能从 736ms 降到 678ms，两件事必须一起做。
- **怎么验**：
  ```bash
  DB_BACKEND=postgres pytest tests/pgdb/test_results_read.py -q   # 含 test_count_bug_is_reproduced
  DB_BACKEND=postgres pytest tests/pgdb -q
  # 0.2 录的快照用例也必须全绿 —— 这是本条唯一真正的等价性证据
  ```
  加一条 EXPLAIN 对比：期望从 `Parallel Seq Scan asin_data + Hash 15.6万行 + 16,760 页 temp 溢出`
  变成 `Nested Loop Semi Join`。
- **失败怎么回退**：`git revert` + `DROP INDEX idx_changes_type_asin;`。
- **黄金**：`scenario.py:227-228` 只覆盖 `change_filter=new`，`price_stock` / `title_bullets` 一次没有。
  **黄金全绿不构成任何证据**，证据来自 `tests/pgdb` 与 0.2 的快照。

---

### Phase 2 — 错误规范（4 条）

#### 2.1 8 处裸 `except Exception: pass` 补日志（不改控制流）

- **改什么**：`server/app.py:351`（导出临时文件清理）、`:371` / `:373`（openpyxl 泄漏目录内外层）、
  `:391`（WAL getsize）、`:588` / `:1268`（批次耗时 strptime）、`:1317`（`_callback_send_queue.put_nowait`）、
  `:2116`（`wb.close()`）。
  清理类（351/371/373/391）与 1317 用 `logger.warning(..., exc_info=True)`；计算类（588/1268）与 2116 用 `debug`。
  **控制流一行不改。**
- **怎么验**：`GATES` 全绿。`grep -A1 'except Exception' server/app.py | grep -c 'pass'` 应从 8 变 0。
- **失败怎么回退**：`git revert`。
- **注意**：这 8 处与 `server/app.py` 的 8 处 `except BaseException`
  （`:151, 1375, 1410, 1457, 1621, 2358, 2441, 2812`）不重叠，后者是有意的回滚模式（`_rollback_quietly`，
  `app.py:132-152` 注释写明理由），**一个字都不要碰**。

#### 2.2 全局 500 处理器

- **改什么**：`server/app.py:245` 之后（app 定义后、`include_router` 之前）注册
  `@app.exception_handler(Exception)`，返回结构化 JSON（`error="internal_error"` + `request_id`），
  并 `logger.exception` 打完整 traceback 带同一个 `request_id`。body 不泄漏任何异常细节。
- **两个实施陷阱（verify 提供，必须照办）**：
  1. **`request_id` 不能用 `BaseHTTPMiddleware` + contextvar 生成**——Starlette 的 `ServerErrorMiddleware` 在更外层，
     contextvar 的下游赋值上游看不见，结果恰恰在 500 那一次 `request_id` 是 None。
     必须用纯 ASGI middleware 写进 scope，handler 里从 `request.state` 取。
  2. **body 里不要放 `server_time_utc`**。见 §6 盲点 B4：任何逐次不同的字段进了基线就录不出来。
     `request_id` 之所以可以放，是因为**黄金 64 步里没有任何一步返回 500**，这条响应永远不进基线。
- **怎么验**：挂一个必崩的探针路由，`curl -i` 确认 500 从 `text/plain 'Internal Server Error'`
  变成 `application/json`；然后删掉探针。`GATES` 全绿（`harness.py:301` 的 TestClient 默认
  `raise_server_exceptions=True`，`ServerErrorMiddleware` 调完 handler 后仍 raise，测试期行为不变）。
- **失败怎么回退**：`git revert`（单个注册块）。
- **文档**：`docs/sync_contract.md:218` 的 5xx 行 error 列今天是「—」，加 `error="internal_error"` 是**加法**，
  不冲突，但建议在对账段落里声明。

#### 2.3 错误码注册表 + 漂移用例（**改造**）

- **与原提案的偏差**：原提案把注册表放在新建的 `server/api/errors.py` 里，
  但 `errors.py` 那条整体被驳回（§5-E1）。**注册表落在 `server/api/sync.py:110` 的 `VALID_OUTCOMES` 旁边**
  ——它本来就是这个模式的原产地，`export_incremental.py` 也已经 `import _sync`。
- **改什么**：
  - `server/api/sync.py:110` 附近加 `ERROR_CODES: frozenset`，收今天实际在用的 **9 个码**
    （AST 扫 `server/api/` 得，去重后正好 9 个）：
    `ack_ahead_of_stream / cursor_ahead_of_stream / cursor_below_retention / event_stream_unavailable /
    export_token_not_configured / gen_mismatch / invalid_export_token / invalid_parameter / range_too_wide`。
    新增 `internal_error`（2.2 引入）。
  - 顺手修掉 `_err` 的一处潜伏地雷：`server/api/sync.py:182-184` 是
    `body = {...}` 然后 `body.update(extra)`——`_err(503,"x","y",error="oops")` 会静默把 `error` 改掉。
    改成**先剔除保留键**。（原提案要求「命中就 raise」，**不采纳**：那会把一个本该回 409
    `cursor_below_retention` 的请求变成 500。剔除 + 由下面的用例看守就够了。）
  - 新建 `tests/test_error_codes.py`：AST 扫 `server/` 全树，取每处 `_err(...)` 的第二个位置实参，
    断言必属 `ERROR_CODES`。模式照抄 `tests/pgdb/test_sync_api.py:187 test_valid_outcomes_matches_the_schema`。
- **原提案的第二半降级**：「断言文档表里的码集合 == 端点实际可达的码集合」按字面写不出来——
  `docs/sync_contract.md:211-218` 那张表只覆盖 `/records` 一个端点，而 `gen_mismatch`（`sync.py:891/1023`）
  与 `ack_ahead_of_stream`（`sync.py:898`）属于 `/ack`、`/ack-prune`，全文件级比对当场假红。
  **降级为「文档里出现的码 ⊆ ERROR_CODES」**，方向性守卫，成本 S。
- **怎么验**：`pytest tests/test_error_codes.py -q`；故意在某个 `_err` 里写一个不存在的码，确认它红。`GATES` 全绿。
- **失败怎么回退**：`git revert`。
- **数目订正**：原计划说「42 处 `_err` 调用点」，实测是 **46 处**（`server/api/sync.py` 40 + `export_incremental.py` 6；
  `sync.py:176` 是 def 本身不算）。

#### 2.4 扩黄金：15+ 个错误路径站点（**breaks_golden = yes，本轮唯一有意的基线增长**）

- **改什么**：`tests/golden/scenario.py:296`（末行 `results_final`）**之后**追加错误路径步骤。
  站点全部已复核命中：
  `app.py:1035`（upload 400 未找到有效 ASIN）、`:1053`（400 非法 callback_url）、
  `:1252` / `:1392` / `:1471`（批次 404）、`:1309`（callback/retry 404）、`:1311`（400 该批次没有配置 callback_url）、
  `:1426`（400 batch_ids 必须是数组）、`:1440`（400 batch_ids 为空或无效）、
  `:2524`（400 时间格式错误）、`:2527`（400 间隔天数至少为 1）、
  `:2582` / `:2616`（定时任务不存在 404）。
- **为什么是纯 append**：批次/任务自增 id（batch id 3、task id 1/3/7/8）被逐值钉死，插中间会让后面每一步全漂，
  diff 从「纯追加」变成几百处差异。追加在末尾 → 前 64 步一个字节不动。
- **必须遵守的选步硬筛（§1 规矩 2）**：新增步骤两个后端必须逐字节相同。
  上面这批全是 `HTTPException` 的 `{"detail": "..."}`，与后端无关，安全。
  `app.py:2524/2527` 在 `app.py:2530` 建目录/写文件之前就 raise，无落盘副作用。
- **怎么验**：
  ```bash
  python -m tests.golden.run selfcheck      # 先跑两遍确认新步骤确定性
  python -m tests.golden.run record         # 只在 sqlite 侧
  python -m tests.golden.run verify
  DB_BACKEND=postgres python -m tests.golden.run verify   # 必须也全绿，否则该步不能留
  ```
- **声明模板**（写进提交信息）：「仅新增步骤，无任何行为改动；前 64 步的 status/content_type/body 逐字节不变；
  diff 是纯 append，从 64 步增至 N 步。」
- **失败怎么回退**：`git checkout tests/golden/samples/sqlite_baseline.json tests/golden/scenario.py`。
- **收益**：错误路径的回归网从 2 个站点扩到 15+ 个。做完之后，「42 处理论上可改」才真正变成「可以安全地改」。

---

### Phase 3 — 分层拆分（8 步）

#### 承重前提：本轮**不搬任何模块级可变全局**

这是本合成计划相对原拆分计划最大的一处结构性改变，必须先说清楚。

原计划的 S1（抽 `server/state.py` + `shared.py` + `background.py`）**被驳回**（§5-S1）。
后果是：S2–S11 没有 `state.py` 可依赖。**替代姿态**：

> 新模块**只搬路由与它们的私有助手，一个模块级可变全局都不搬**。
> `db` / `_worker_registry` / `_runtime_settings` / `_settings_version` / `_global_coordinator` /
> `_worker_restart_flags` / `_completion_check_set` / `_expand_rounds` / `_callback_send_queue`
> **全部留在 `server/app.py`**，新模块一律用 `server/api/sync.py:188-191` 的惰性访问模式：
> ```python
> def _srv():
>     from server import app as _s
>     return _s
> ```
> 读写一律走 `_srv().xxx` 属性访问，**禁止 from-import**（`_settings_version` 是 int，会被快照）。

**为什么这是净收益，不是妥协**：拆分计划自己列的「最高风险」就是
「黄金夹具按名字打补丁（`harness.py:151-162` no-op 5 个后台协程、`:270-273` 换两个路径常量、
`:293-297` 重置 5 个全局），搬走名字却留别名 = 补丁打空 = 样本不确定」，
再加上三个 PG 夹具的 `monkeypatch.setattr(srv, "db", pgdb, raising=False)`
（`tests/pgdb/test_sync_api.py:38`、`test_retention.py:55`、`test_export_retention_window.py:37`）
和第 4 处裸赋值 `tests/pgdb/test_sync_api.py:470/479 srv.db = db`（已复核）。
**不搬全局 = 这整类风险归零，夹具一行都不用改。** 代价是 `app.py` 的行数减得比原计划少（全局与后台协程留着），
拆分的真正目标（59 个端点按域分文件、SQL 收口有边界）仍然达成。

#### 拆分的通用规矩（每一步都适用）

- **router 一律光秃 `APIRouter()`**：不带 `tags=`、不带 `prefix=`、不设 `include_in_schema`。
  当前整份 openapi 里**没有任何 `tags` 键**，照抄 `sync.py:129` 的 `APIRouter(tags=[...])` 会给该 router 下
  每个 operation 加一个 tags 数组，51 个 path 一起红。
- **禁止改函数名、docstring（连错别字都不行）、路径**——它们被编码进 `operationId` / `summary` /
  `description` / `Body_*` schema 名，而 openapi 是黄金第 **11** 步、逐字节钉死。
- **每步的验收是双重的**：`GATES` 全绿 **且** `tests/golden/samples/sqlite_baseline.json` **一个字节不变**。
  只有 3.8 允许重录。
- **实测已确认的假设**（verify 在 fastapi 0.141.1 下真跑过）：
  光秃 `APIRouter()` + `include_router` **不改 openapi**；`operationId`/`summary` 由 `route.name`(=函数名) 与 path 生成；
  `run.py:41-43` 用 `sort_keys=True` 落盘、`harness.py:437-447` 按 key 比较，所以**路由注册次序不影响 openapi 步**。

#### 3.1 `server/api/pages.py`（5 个 HTML 页面，48 行，试点）

- **改什么**：`server/app.py:761-808` 的 `page_dashboard(763)` / `page_tasks(778)` / `page_results(788)` /
  `page_workers(793)` / `page_settings(798)`，连同 `templates = Jinja2Templates(...)`(`:247`) 与
  `templates.env.filters["cst"]`(`:291`) 的绑定。`app.mount("/static", ...)`(`:246`) 留原地。
  5 个 handler 还要访问 `db`(`:765`)、`_worker_registry`(`:774`)、`_runtime_settings`(`:783,802`)——
  按上面的规矩走 `_srv()`。
- **怎么验**：`GATES` + 基线字节不变。5 步黄金（`scenario.py:288-291`）+ openapi 第 11 步双重验证。
- **失败怎么回退**：`git revert`（48 行）。
- **它的真实作用**：验证「光秃 APIRouter 不改 openapi」这个假设。假设错了，代价只有 48 行。**所以它必须第一个做。**

#### 3.2 `server/api/debug.py`（5 个诊断端点，≈110 行）

- **改什么**：`server/app.py:2282-2293`（`api_diagnostic`）、`:2719-2824`（`_pct(2723)` / `_summary(2731)` /
  `api_debug_lock_stats(2744)` / `api_debug_event_stream(2757)` / `api_debug_lock_stats_reset(2790)` /
  `api_clear_database(2801-2822)`）。
  `api_debug_event_stream` 的 `include_in_schema=False` 是**装饰器级**参数（`:2757`），原样带走；router 本身仍不设。
  `api_clear_database` 的裸事务（`:2805-2814`）**原文照搬一个字不改**——尤其 `:2810` 的
  `DELETE FROM sqlite_sequence` 是 `common/pgdb/pool.py:291` 的字典键（已复核；`pool.py:285` 的注释里写的定位
  `server/app.py:2654` 已过时 156 行，**顺手改对**）。
- **怎么验**：`GATES` + 基线字节不变。黄金已覆盖 diagnostic 与 lock-stats。
- **失败怎么回退**：`git revert`。

#### 3.3 `server/api/fleet.py`（worker 注册表 / 心跳 / 配额 6 个端点，≈130 行）

- **改什么**：`server/app.py:1513`（`api_coordinator`）、`:1533`（`api_delete_all_offline`）、
  `:1544`（`api_restart_worker`）、`:1799`（`api_worker_sync`）、`:1828`（`api_workers`）、`:1856`（`api_delete_worker`）。
  `_register_worker`(`:155-173`) 与 `_allocate_quotas`(`:728-759`) **留在 app.py**（不搬全局的推论：
  它们直接读写 `_worker_registry` / `_global_coordinator`），新模块调用 `_srv()._register_worker(...)`。
- **路由遮蔽**：无风险。`/api/workers/{worker_id}/restart`(POST) 与 `/api/workers/{worker_id}`(DELETE) 方法不同；
  `/api/workers`(GET/DELETE) 是静态路径。
- **怎么验**：`GATES` + 基线字节不变。黄金覆盖 worker_sync / workers / coordinator 三步；delete/restart 无网（见 §6）。
- **失败怎么回退**：`git revert`。

#### 3.4 `server/api/sellers.py`（F-009 四个端点，≈160 行）

- **改什么**：`server/app.py:1087`（`_SELLER_URL_RE`）、`:1088`（`_BARE_SELLER_RE`）、`:1091`（`_extract_sellers_from_text`）、
  `:1116`（`api_upload_sellers`）、`:1183`（`api_seller_batch_progress`）、`:1189-1213`（`api_seller_discoveries`），
  **并把 `api_submit_seller_result`（`:1700-1731`）从 worker 节一起搬过来**。
  `api_seller_discoveries` 用的是 `db.read()`（`:1209`）不是裸 `db._db`，f-string 拼 WHERE 在 `:1202-1206`，原文照搬。
- **为什么值得单独一步**：这是本次调查的头号误导源。`app.py:1083` 的 F-009 节头之后，
  只有 `1087-1214` 是卖家采集，`1215`（`@app.get("/api/batches")`）到 `1560` 共 347 行是被挤下来的批次/worker/设置端点；
  而真正的卖家结果提交端点却在 `:1561` 的 worker 节下。**归域必须逐端点做，不能信节头。**
- **跨节搬运的安全性（verify 已核）**：`/api/tasks/seller-result` 是静态路径，
  worker_queue 域内没有 `/api/tasks/{x}` catch-all（`app.py:1563/1600/1630/1655/1732/1781` 全是静态），换模块不改匹配。
- **怎么验**：`GATES` + 基线字节不变。
  **⚠ 这一步没有黄金网**：4 个 seller 端点在 64 步里一步都没有，而它们**也补不进去**——
  `/api/upload-sellers` 的响应含 `sellers_{%Y%m%d_%H%M%S}` 批次名，不被 `harness.py` 的 `_TS_RE` 覆盖，逐次不同。
  **替代验证（必须做，不许省）**：改前改后各手工打一次这 4 个端点，逐字节比响应；
  并补一条普通 pytest 用例（非黄金）覆盖 upload-sellers → progress → discoveries 的串联。
- **失败怎么回退**：`git revert`。

#### 3.5 `server/api/worker_queue.py`（`/api/tasks/*` 六个端点，≈205 行）

- **改什么**：`server/app.py:1563`（`api_pull_tasks`）、`:1600`（`api_release_tasks`）、`:1630`（`api_submit_result`）、
  `:1655`（`api_submit_batch`）、`:1732`（`api_upload_screenshot`）、`:1781`（`api_screenshot_fail`）。
  `api_release_tasks` 的 `task_ids` 裸事务（`:1606-1623`，`async with db._write_lock` 在 `:1611`）
  **本步原文照搬、不删**——删除是 X.1 的独立决策。
  `_completion_check_set.add`（`:1693`，全仓唯一的 HTTP 层写点）改成 `_srv()._completion_check_set.add`。
- **怎么验**：`GATES` + 基线字节不变。黄金覆盖 pull / result / batch；release / screenshot / screenshot-fail 无网。
  调用方已核实只有 `worker/engine.py:993` 与 `tools/smoke_local.py:142/217`，不碰 erpAPI 路由面。
- **失败怎么回退**：`git revert`。

#### 3.6 `server/api/results.py`（结果查询与删除 5 个端点，≈190 行）

- **改什么**：`server/app.py:1865`（`api_results`）、`:1883`（`api_result_detail`）、`:1892`（`api_change_stats`）、
  `:2294-2365`（`api_delete_by_file`）、`:2366-2449`（`api_delete_results`）。
  搬运期间 **SQL 一字不动**，包括 `:2392-2396` 自己拼的 f-string LIKE
  （它的 PG 语义靠 `common/pgdb/pool.py:273-278` 的 `_LIKE_QMARK_RE` + `:281` 的 `LIKE_NO_ESCAPE` 撑着，D-16）。
- **实际的裸 SQL 面（verify 订正）**：裸读 **5 处**不是 4 处——`:2341`、`:2396`、`:2404`、`:2409`、`:2424`；
  裸事务 2 个：`:2348-2359`、`:2431-2442`。
- **怎么验**：`GATES` + 基线字节不变。
  **⚠ 两个删除端点今天一个黄金步都没有**，2.4 也没覆盖它们（它们不是错误路径）。
  替代验证同 3.4：改前改后手工比响应 + 一条普通 pytest 用例。
- **失败怎么回退**：`git revert`。

#### 3.7 `server/api/export.py` + 路由顺序守卫升级（**承重，最后拆**）

- **改什么（S11）**：`server/app.py:1918-2279` 整段——
  `BATCH_STATUS_EXPORT_HEADERS(1923)` / `_VARIANT_PAGE_ASIN_RE(1934)` / `_parse_selected_fields(1937)` /
  `_EXPORT_ROWS_PER_CHUNK(1947)` / `_export_needed_columns(1950)` / `_get_export_headers(1964)` /
  `_batch_status_export_values(1984)` / `_prepare_row(2027)` / `api_export_fields(2047)` / `api_export_all(2056)` /
  `api_export_batch(2066)` / `_export_xlsx_streaming(2078)` / `_export_csv_streaming(2171)` /
  `api_export_screenshots(2216-2279)`。
- **承重要求**：`/api/export/{batch_name}` 必须仍排在 `/api/export/incremental` 之后。
  **采用顺序局部化方案**：在 `export.py` 文件顶部、任何 `@router.get` 之前写
  ```python
  from server.api import export_incremental as _incr
  router.include_router(_incr.router)
  ```
  `app.py` 只 include 一个 export router。顺序从此由单文件自上而下阅读保证，
  `app.py` 的 include 列表怎么重排都打不破。
  **这个方案的关键假设 verify 已经实测过**（fastapi 0.141.1）：
  `exp.include_router(incr)` 写在 exp 自己的 `@exp.get` 之前，再 `app.include_router(exp)` →
  GET `/api/export/incremental` 命中 incremental handler 而非 catch-all；
  openapi paths 里只有 `/api/export/fields` 与 `/api/export/{batch_name}`；整份 schema 无 tags 键。
  即 router 级 `include_in_schema=False`（`export_incremental.py:98`）在被父 router 包含时**确实保留**。
- **改什么（S11b，必须同一提交）**：三层守卫。
  1. `tests/test_incremental_export.py:42-60` 的 `index_of_our_router()` 与 catch-all 查找改成**递归展开**：
     遇到 `getattr(r,'original_router',None) is not None` 就按序展开子路由，产出与 Starlette 匹配顺序一致的扁平列表再比索引。
     （实测：`app.include_router(r)` 在 `app.routes` 里留下的是 `_IncludedRouter`、`path is None`，
     所以 `:60` 的 `assertIsNotNone` 会红——**它红是好事，修法绝不能是删断言**。）
  2. 保留 `:69-75` 的行为断言（真打一次，响应体不含「批次不存在」）——结构断言会被下一次 FastAPI 版本变化绕过，行为断言不会。
  3. 新增：断言 `export.py` 源码里 `include_router(_incr.router)` 出现在第一个 `@router.get` 之前。
- **第二份副本必须同步改，且它今天是坏的**：`tools/phase5_preflight.py:220-239` 的逻辑是
  `if incr is None: fail(...) elif catch is not None and incr > catch: fail(...) else: ok(...)`
  ——**catch 为 None 时直接落到 else 报绿**。它没有前提断言，拆分后会静默通过。必须一并修成 fail。
- **怎么验**：
  ```bash
  pytest tests/test_incremental_export.py -q
  python tools/phase5_preflight.py            # 期望它在守卫失效时 fail 而不是 ok
  # GATES + 基线字节不变
  curl -s "localhost:PORT/api/export/incremental?limit=1"   # 真打一次，确认不是 404 {"detail":"批次不存在: incremental"}
  ```
- **失败怎么回退**：`git revert`（生产代码与守卫同一提交，必须整提交回）。
- **为什么最后拆**：动这条承重线时其余 6 个模块已全绿，变量只剩一个。

#### 3.8 裸 SQL 五批收口（**breaks_golden 分批判定，每批独立提交独立验证**）

`grep -c 'db\._db' server/app.py` = 49，`db\._write_lock` = 8（7 个真块在
`:1345 / :1401 / :1450 / :1611 / :2347 / :2430 / :2805`，第 8 处是 `:135` 的 docstring）。

按收益/风险排，**每批一个提交**：

| 批 | 收什么 | 位置 | breaks_golden |
|---|---|---|---|
| (1) | `db.clear_all_data()` | `app.py:2805-2814` | no |
| (2) | `db.delete_batches(ids) -> list[screenshot_path]` | `app.py:1396/1401-1412` + `1444/1450-1459` | no |
| (3) | `db.delete_asins(asins)` + `db.find_asins_by_search(terms)` | `app.py:2341/2347-2360` + `2396/2404/2409/2424/2430-2443` | **unsure** |
| (4) | `db.retry_failed_tasks(batch_id, exclude_error_types)` | `app.py:1345-1377` | no |
| (5) | release 旧分支 → 见 X.1（优先删除而不是收口） | — | — |

- **C4 是强制的，且有现成守卫**：每加一个方法必须**同时**改 `common/database.py`（SQLite）与 `common/pgdb/` 对应 mixin，
  并把名字加进 `common/pgdb/__init__.py:56-84` 的 `PUBLIC_API`——
  `:92 _assert_api_complete` / `:100 _assert_single_owner` 在**导入期**执行（`:120-121`），
  且 `tests/pgdb/test_skeleton.py:40-41` 反向断言「`PUBLIC_API` 里不许有 SQLite 版没有的名字」。**只加一侧会当场炸。**
  （这正是 sqlite-exit 第 3 步被驳回的原因——它只给了 PG 一侧。**批 (1) 取代它。**）
- **批 (1) 的连带**：删掉 `common/pgdb/pool.py:285-297` 的 `_STATEMENT_OVERRIDES` 与 `:615-620` 的整句替换分支，
  同步改 `tests/pgdb/test_skeleton.py:312-322` 与 `tests/pgdb/test_admin.py:341-346`。
  黄金 64 步不碰 `/api/database`；`/api/database` 在第 11 步 openapi 的 51 条 path 里，
  但只改 handler 内部不动路由签名，openapi 不变。
- **批 (3) 是已知活雷**：删除路径的 LIKE 模式是 `app.py:2392-2396` 自拼的 f-string，
  读路径是 `common/pgdb/results_read.py` 的 `_TERM_OR`，两者靠 `pool.py` 的正则改写 + `ESCAPE ''` 对齐。
  D-16 记录过 `search="back\\slash"` 在两后端删掉不同的行、且都回 `{"deleted":1}`。
  收进同一个 db 方法是净收益，**但必须补一条针对反斜杠/百分号/下划线的双后端用例**，
  否则等于把一个已知不一致搬了个家。
- **怎么验（每批）**：`GATES` 两个后端各一遍 + `pytest tests/pgdb -q`；
  批 (3) 额外跑 0.2 的快照用例与新增的转义用例。
  批 (2)(3) 若把裸读从写连接改到读池、或重排 CHUNK=500 的分块边界，响应体理论不变但**必须实测**，
  飘红就先查错、不许顺手重录。
- **失败怎么回退**：单批 `git revert`；红了就知道是哪一批。
- **C2 记账**：退役 SQLite 后 (1)(2)(3)(4) 每条只需写一半，省 `common/database.py` 侧约 4×25 行；
  更大的是 (1) 能连带删掉 `pool.py:264-297` 整套「为 app.py 字面量 SQL 做文本翻译」的垫片（约 35 行）及其用例——
  **那套垫片存在的唯一理由就是 app.py 的裸 SQL，3.8 做完它本身就该退休，与 SQLite 去留无关。**

---

### Phase 4 — 去重收敛（8 步 + 1 条登记）

#### 4.1 `common/core/`：把 `_shared.py` 的真源搬出 `common/database.py`（**先做，它是落点裁决**）

- **改什么**：新建 `common/core/`，放 `common/pgdb/_shared.py:26-58` 再导出的那 **23 个纯 Python 符号的定义**——
  重试策略集与 `_fail_cap`(`common/database.py:54`)、`LOCK_STATS`(`:69-76`) / `TimedLock`(`:125`) / `record_stage` 那套锁仪表、
  `_NA_VALUES`(`:168`) / `_normalize_screenshot_path`(`:171`) / `_is_parse_failure`(`:181`)、
  四个比较器(`:198-249`)、`_HASH_FIELDS`(`:251`) / `_compute_content_hash`(`:265`) / `_compute_title_bullets_hash`(`:270`)、
  `ASIN_DATA_FIELDS`(`:276`) / `_ASIN_DATA_COLUMN_SET`(`:294`)。
  `common/database.py` 与 `common/pgdb/_shared.py` **都**改成从 `common/core` 再导出，
  两者的 `__all__` 和导入名一字不动。
- **必须显式再导出，不许用 `from ... import *`**：这正是 sqlite-exit 第 1 步被驳回的原因——
  星号导入**跳过下划线开头的名字**，而要搬的 23 个里有 15 个是下划线开头的，
  后果是 `server/app.py:1921 from common.database import _parse_price_float` 直接 ImportError，
  以及 `class Database` 自己的方法体 NameError。**照 `_shared.py:26-56` 的逐名写法。**
- **调用点清单（sqlite-exit 第 1 步漏了 4 个，这里补齐）**：
  `server/app.py:1336`（`NO_AUTO_RETRY_ERROR_TYPES`）、`:1921`（`_parse_price_float`）、`:2747` / `:2793`（`LOCK_STATS`）、
  `tests/test_engine_not_found.py:73-77`、`tests/test_slowhash.py:536-541`、
  `tests/test_golden_with_relay.py:52`、`tests/pgdb/test_results_read.py:20`。
- **顺带搬一个**：`common/pgdb/pool.py:214-229` 的 `as_int` 也搬进 `common/core`
  （它是纯函数，但 `pool.py:74` 是模块级 `import asyncpg`——留在 pool.py 就没人能安全地用它，
  这正是 sqlite-exit 第 5 步违反 C4 的原因）。
- **`common/pgdb/_shared.py:21` 的那段论证要照抄进新模块**，并删掉「导入 `common.database` 的代价：它 import aiosqlite」——
  那正是这次搬迁消除的东西。
- **怎么验**：`GATES` 全绿（纯搬迁）；
  `tests/pgdb/test_skeleton.py:85-95` 的 `for name in _shared.__all__: assert getattr(_shared,name) is getattr(sq,name)`
  必须原样通过（三个模块指向同一批对象）。
- **失败怎么回退**：`git revert`。
- **C2 的账**：SQLite 若留，这次搬迁让 pgdb 不再为了拿几个常量而 import aiosqlite；
  SQLite 若走，**退役从「一次带隐藏依赖的手术」变成一次 `git rm`**。这是本轮对 §7 最有价值的一步。
- **规模订正**：`common/database.py` 是 **2486 行**（已复核），不是某处说的 5000 行；
  `class Database` 从 `:297` 到文件尾 = 2190 行，前 296 行就是这次要搬的东西。

#### 4.2 `common/core/idents.py`：ASIN 正则收成一份

- **改什么**：定义 `ASIN_PATTERN = r'B[0-9A-Z]{9}'` 与 `ASIN_RE = re.compile('^'+ASIN_PATTERN+'$')`。
  `server/app.py:905` 与 `worker/parser.py:2534`（今天逐字节相同）改成导入；
  `worker/parser.py:1811` 的局部字符串改成 `ASIN_PATTERN`（`:1819/:1827/:1833/:1841/:1848` 五个用点不动）。
  `server/app.py:1934` 的 `_VARIANT_PAGE_ASIN_RE` **保持独立**并加注释说明它是另一条规则（10 位、不要求 B 前缀、IGNORECASE），
  免得下一个人「顺手统一」。
- **一处 verify 补的注意**：`worker/parser.py` 今天**不 import 任何 `common.*`**（只有 `:18 from worker.ziputil import ...`）。
  本条会新开一条 worker→common 依赖。`worker/engine.py:27-28` 已经这么做了所以打包上可行，
  但要在 `parser.py` 文件头写明。
- **本轮不动的**：`worker/parser.py:2575` / `:2614` 直接 `_ASIN_RE.match(asin)` 不做大小写归一，
  而 `server/app.py:910-914` 的 `_normalize_asin` 先 `.strip().upper()`——**这是真正的分叉，但它在调用侧**，
  且改它踩 C3。登记在 X.1。
- **怎么验**：`GATES`（字面量不变，纯物理搬迁）。
- **失败怎么回退**：`git revert`。

#### 4.3 `common/core/timeutil.py`：DB 时间戳收一份 + 修 3 处本地时钟

- **改什么**：定义 `now_ts() -> str` = `datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')`，
  docstring 引 `common/pgdb/schema.py:11-15` 说明**为什么这个格式是契约、不许改成 RFC3339**
  （「时间戳一律 text…否则 erpAPI 拿到的每一条 created_at/updated_at 都变形」）。
  把 **28 处** `datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')` 换成 `now_ts()`：
  `common/database.py:961/1014/1069/1163/1254/1255/1297/1298/1337/1390/1517/1670/1711/1826/2300`、
  `common/pgdb/tasks.py:230/328/329/383/384/425/522`、`batches.py:230/280/331`、
  `results_write.py:256/332/532`、`media.py:244/506`、`server/app.py:415/1263`。
  带 timedelta 的用 `ts_from(dt)`——**注意 `server/app.py:651` 属于这一类**
  （`(datetime.utcnow()+timedelta(seconds=delay)).strftime(...)`），原计划把它错列进了 `now_ts()` 那组。
- **有意的行为改动（必须声明）**：`server/app.py:1609` / `:2561` / `:2688` 三处 `datetime.now()`（无时区的本机墙上时间）
  一并换成 `now_ts()`。UTC 主机上输出逐字节相同；本机（`docs/local_macos_setup.md` 那条路径）上修掉 8 小时错位。
- **两处不动**：`server/app.py:296-299` 的 `_cn_now()`（`utcnow()+timedelta(hours=8)`）保持不动，
  只在 docstring 加一句「它不是 `now_ts()`，两者永远不该互换」；
  `common/pgdb/schema.py:58` 的 `TS_DEFAULT`（SQL 侧同格式）保持不动，加注释交叉引用。
- **一处 finding 的订正**：原计划说「`:2561/:2688` 写进 schedules 的 `created_at` 与 `common/database.py` 写进同库的
  `updated_at` 差 8 小时、格式一样无法区分」——**这是错的**。
  `_save_schedules`(`server/app.py:2459-2461`) 写的是 `_runtime_settings` → settings 文件，**根本不进 DB**。
  改动本身仍然对（本机时区→UTC），但理由要写对。
- **怎么验**：`GATES`。黄金不红的理由比原计划说的更强：不只是 `harness.py:30-37` 的 `_VOLATILE_KEYS`，
  `:39-42` 的 `_scrub_scalar` 对**任何字符串**做 `_TS_RE.sub("<TS>")`，CSV 行也走这条（`:106`）。
- **失败怎么回退**：`git revert`。
- **C2**：SQLite 退役后 `common/database.py` 那 13 处随文件一起消失，剩下 15 处一行不用动。

#### 4.4 RFC3339 `_iso` 去重

- **改什么**：`server/api/sync.py:153-159` 与 `common/pgdb/retention.py:964-970` 今天**逐字节相同**（含
  `.replace("+00:00","Z")`）。移进 `common/core/timeutil.py`（命名 `iso_utc` / `now_iso_utc`），两边改导入，
  retention 那份直接删（`:481/:669/:671/:798/:842` 五个调用点不变）。
  **方向很重要**：让 `common/pgdb/retention.py` 去 import `server.api.sync` 才是分层倒置。
- **不动的**：`server/api/export_incremental.py:207-213` 的 `_iso_seconds`（在 `_iso` 之上截小数秒，
  是契约 v1「精确到秒」的要求，不是重复）；`tools/smoke_local.py:152` 与
  `.agent/monitor/batch_runtime_monitor.py:43`（独立脚本，刻意不引仓库依赖，在新模块 docstring 里列出并注明）。
- **怎么验**：`GATES`（两处都在 PG-only 路由上，黄金 64 步不含 `/api/v1/sync/*` 与 `/api/export/incremental`，零风险）
  + `DB_BACKEND=postgres pytest tests/pgdb -q`。
- **失败怎么回退**：`git revert`。

#### 4.5 completeness：位常量收一份，停止重算合取式

- **改什么**：新建 `common/core/completeness.py`（**不放 `common/pgdb/schema.py`，因为 worker 不该依赖 pgdb**），
  定义 `BREADCRUMB=1 / DETAIL=2 / IMAGE=4 / MEASURED=8 / MAX=15 / REQUIRED_MASK=7` 与 `completeness_ok()`。
  `worker/parser.py:50-53`、`common/pgdb/schema.py:462-467`、`server/api/sync.py:118-119` 三处改成导入，
  **各自保留现有别名**（`common/pgdb/relay.py:134-135` 从 schema 导入 `EVENT_COMPLETENESS_*`，别名保留即可不动）。
  `server/api/export_incremental.py:243-246` 的手工重算删掉，改调 `completeness_ok(...)`，与
  `server/api/sync.py:333-336` 走同一条代码。
- **为什么必须做**：契约 §4.3 的判据改一次要改两处，漏一处就是
  `/api/v1/sync/records.completeness_ok` 与 `/api/export/incremental.completeness_ok` 对同一行给出不同答案——两个消费者、两个结论。
  `worker/parser.py:48` 注明「位定义已对外公布（`docs/sync_contract.md` §6.4），勿改」——收成一份之后这条「勿改」才有唯一的执行点。
- **C3**：只改常量的**来源**，不改任何数值，`worker/parser.py` 的解析产出一个字节不变。
- **怎么验**：`GATES` + `DB_BACKEND=postgres pytest tests/pgdb -q`；输出逐字节不变。
- **失败怎么回退**：`git revert`。

#### 4.6 `server/app.py:1591` 的内联邮编正则改调 `_normalize_zip`

- **改什么**：`if prefer_zip and re.match(r'^\d{5}$', prefer_zip)` → `pz = _normalize_zip(prefer_zip)`
  （`_normalize_zip` 就在同一文件的 `:917-935`，相隔 674 行）。
- **修的是什么**：worker 传 `prefer_zip="1001"` 时，`:917` 会 zfill 成 `"01001"` 接受，`:1591` 直接判 None 丢弃，
  任务分配静默退回「不挑邮编」。改后归一成 `"01001"` 生效。
  顺带修掉 `re.match(r'...$')` 允许尾随换行这个小口子（防注入强度不降反升）。
- **不合并的三份**：`worker/parser.py:1756 _norm_zip`（串内抽取）、
  `common/pgdb/relay.py:375 normalize_zip`（原样透传，`:380-385` 明写理由）、
  `:411 normalize_zip_observed`——三种契约各有成文理由。
  **但要兑现 `relay.py:418-421` 那条「补零规则必须与 normalize_zip 一致」的注释**：抽一个共享的
  `_zfill_short_numeric(s)` 私有函数，只这一小步，不动三者的外层语义。
- **怎么验**：`GATES`（`tests/golden/scenario.py:147-149` 的 pull_tasks 不传 `prefer_zip`，不受影响）；
  加一条 `prefer_zip="1001"` 的 pytest 用例。
- **失败怎么回退**：`git revert`。

#### 4.7 批次名收成一份 + 统一秒精度（**有意行为改动，需声明**）

- **改什么**：`server/app.py` 加 `def _batch_name(prefix): return f"{prefix}_{_cn_now():%Y%m%d_%H%M%S}"`，
  五处调用点全改：`:711`（`auto_{sched_name}`，今天是**分钟**精度）、`:1046`（`batch`）、`:1161`（`sellers`）、
  `:2059`（`all`）、`:2645`（`auto_{target.name}`，今天也是分钟精度）。
- **修的是什么（机制描述照 verify 订正）**：同名批次**不是** `create_batch` 层面的 no-op——
  `common/database.py:812-826` 是 `INSERT OR IGNORE` 之后 `SELECT id FROM batches WHERE name=?`，
  会返回**既有批次的 id**；随后 `create_tasks`(`common/database.py:1121-1125`) 才靠 tasks 上的 `INSERT OR IGNORE` 吃掉重复 ASIN。
  所以同一分钟内自动触发 + 手动触发同一个定时任务，**「什么都不做」只在 ASIN 清单未变时成立；
  清单变了会把新 ASIN 悄悄塞进上一个批次**。这比原计划描述的更严重。
- **怎么验**：`GATES`。黄金不红已实测：`harness.py:339-347` 的 Recorder 只记 status/content_type/body，
  **不记任何 header**，基线的 `export_all_csv` body 里也不含批次名；上传用的是显式 `BATCH_A`/`BATCH_B`。
- **声明**：`:711` 与 `:2645` 从分钟精度升到秒精度。
- **失败怎么回退**：`git revert`。
- **与 4.3 的口径问题（未决，登记在 §6）**：本条把 `_cn_now()` 的调用点从 2 处涨到 5 处，
  而 `_cn_now()` 自身（`datetime.utcnow()+timedelta(hours=8)`）在非 UTC 主机上和 `:1609/:2561/:2688` 是同一类错误。
  本轮**不动它**，但要在 4.3 的 docstring 里点名。

#### 4.8 字段表守卫测试 + 补 `title_bullets_hash`

- **改什么**：
  - `common/models.py:30-77` 的 `AsinData` 补 `title_bullets_hash: str = ""`。
    今天它**没有**这个字段，而 `common/database.py:287` 的 `ASIN_DATA_FIELDS` 有、
    `common/pgdb/schema.py:158` 与 `common/database.py:518` 两份 DDL 也都有；
    于是 `common/models.py:118` 的 `_INTERNAL_FIELDS` 在排除一个根本不存在的 dataclass 字段。
    补它安全：`AsinData` 除 `common/models.py` 外**零引用**，唯一用途是 `:119` 推导 `EXPORTABLE_FIELDS`，
    而它已在 `:118` 被排除 → `/api/export/fields` 与所有导出列一字不变。
  - 新建一条守卫测试：从 `AsinData`、`ASIN_DATA_FIELDS`、
    `common/pgdb/schema.py:119-160` 与 `common/database.py:506-540` 两份 DDL 抽列名，
    断言四者的差集**恰好等于一张显式白名单**（`{id, created_at, updated_at}` 与 6 个 `baseline_*`）。
- **verify 补的一条必须加进去的断言**：**列序也在漂**。
  `common/pgdb/schema.py:44-46` 与 `:115` 明写「列集与列序是 API 契约的一部分」
  （`SELECT d.*` 无 response_model，列序会整个泄进 erpAPI 的响应），
  而今天 `common/models.py:53-57` 的 `variant_attributes` 排在 `parent_asin`/`variation_asins` **之前**，
  `ASIN_DATA_FIELDS`(`common/database.py:280-282`) 与两份 DDL 都排在**之后**。
  **只测集合差集抓不到它**——守卫必须同时断言 `ASIN_DATA_FIELDS` 与两份 DDL 之间的**顺序**一致。
- **怎么验**：`pytest <新用例> -q`；故意把 DDL 里两列对调，确认它红。`GATES` 全绿。
- **失败怎么回退**：`git revert`。

---

## 4. X.1 删除 / 待确认候选（**只登记，本轮不执行**）

C1 说既有端点可以改，但 erpAPI 用到的那些端点在清单到手前**不能默默动**。以下全部单独列出、标记待确认：

| 候选 | 位置 | 证据 | 待确认什么 |
|---|---|---|---|
| `/api/tasks/release` 的 `task_ids` 分支 | `server/app.py:1606-1623` | 全仓无发送方：`worker/engine.py:993-998` 与 `tools/smoke_local.py:142-145/217-220` 都只发 `tasks`；注释自认「兼容旧格式（无 lease 校验，直接释放）」 | 是否还有老 worker / erpAPI 在发 `{"task_ids":[...]}`。删掉能消灭 1 个裸事务 + 1 处本地时钟(`:1609`) + 一个「任意 worker 可释放别人任务」的降级路径。**若最终保留兼容，最小改法是走 db 层新方法，不要在 app.py 里内联 UPDATE。** 注意还有**第三份副本**在 `tests/pgdb/test_admin.py:246-254`（注释里的 `app.py:1497-1511` 已过期，真值 `:1606-1623`） |
| 4 个 legacy `/api/auto-scrape/schedules*` | `server/app.py:2658-2718` | **只有 1/4 有前端调用方**：grep 全部模板只有 `settings.html:485` 调 `POST /api/auto-scrape/schedules`；GET(`:2660`)/PUT(`:2696`)/DELETE(`:2707`) 无任何前端调用。它们与 `/api/schedules` 是同一份数据的两套 CRUD | 这两条 path 在黄金第 11 步 openapi 的 51 条里，是 erpAPI 最可能真在用的一组 |
| `/api/worker/download` | 模板引用：`server/templates/settings.html:223`、`workers.html:65`、`:68` | 59 条路由里**没有这个路径**，三个按钮点了就是 404 | 要么实现要么删按钮——**但不要在拆分提交里顺手做** |
| `worker/parser.py:2575` / `:2614` 的 ASIN 大小写 | 同左 | `server/app.py:912` 的 `_normalize_asin` 会 `.upper()`，parser 不会：同一个小写 asin 串，上传接口收、列表页解析器丢 | 列表页 ASIN 的实际来源。改它踩 C3（D-27） |

---

## 5. 已否决及理由（**这一节和正文一样重要**）

> 20 条 proposal 未通过验证。**不要再原样提一遍。** 每条写明：错在哪、要重提得先修什么。

### 性能

**P4 `/api/results` 加 `with_total` 参数 —— BREAKS_CONSTRAINT（C1）**
核心设计是「默认 True → 既有响应一个字不变」，正是 C1 第一句点名禁止的兼容性扭曲设计，
代价是 `results_read.py:145-148` 与 `database.py:2120-2123` 各多一条分支，而提案自己承认 erpAPI「拿不到任何提速」。
附带一处 breaks_golden 误判：它要改 `server/templates/results.html`，而 `page_results` 记的是
`size_bucket = len(text)//1000`，**基线值 26（已复核）**，verify 实测渲染 26,509 字节 → 距 bucket 27 只剩 **491 字节**。
提案的 risks 说「意外飘红说明等价性推理错了，应当先查错」——那会让人去追一个不存在的 SQL 等价性 bug。
**要重提**：直接把 total 从翻页响应里去掉（或 cursor 存在时不返回）+ 明确声明 + 重录基线 + 把 `/api/results` 列进 erpAPI 待确认。
另注意 `results.html:279`（`resultTotal`）与 `:280`（`stat-total`）**两处**都要防 null，提案只提了 279。

**P5 `/api/results` 加 `fields` 参数 —— BREAKS_CONSTRAINT（C1）**
同上，且代价更高：投影拼装要在 `results_read.py:247-253` 和 `database.py:2226-2232` 各维护一份带分支的 SQL。
事实层面是准确的（现成白名单 `results_read.py:330-332`、必须强制带 `id` 与 `asin`、前端只用 15 列
`results.html:326-340` 已逐个核对），可惜设计违约。
**要重提**：默认收窄 → `results_page1/page2/page_prev/search/filter_new/results_final` 六步会红，
那是必须声明并重录的有意改动，不是 `no`。**且 P4+P5 同时改前端很容易把 size_bucket 26 顶过 27。**

**P6 `batch_asins` 加 `d_id` 冗余列 —— WRONG**
(1) 行号错：SQLite 的 `batch_asins` DDL 在 **`common/database.py:465-472`**（PK 在 `:470`，`idx_batch_asins_asin` 在 `:472`），
提案写的 `:530-540` 是 asin_data 的 `baseline_*` 与 `rating/review_count/seller_*` 列。
(2) 违反 C5：「给 save_result 首次落 asin_data 时补写（results_write.py）」整句没有行号——
真正的 INSERT 点是 `common/pgdb/results_write.py:747` 与 `common/database.py:1974`，
而它恰好落在 `accept_results_batch` 的**全局写锁内**（README:553 的 p99 71-94ms 就是这把锁），
**与 P1「把写锁时间抢回来」的立论自相矛盾**，提案既没定位也没估这条写的代价。
(3) 结构性遗漏两处：count 侧的 `count_join_parts`（`results_read.py:164` / `database.py:2139`）仍是 `ba.asin = d.asin`，
两条查询形状分叉；`batch_id` 与 `change_filter` 同时给出时的拼装（`results_read.py:172/180/186`）完全没覆盖。
**代价**：仪表盘的默认查询（batch_id 翻页，实测第 5/20 个批次 262-293ms / 340,724 buffers，且越老越慢）本轮**不修**。见 §6-C1。

### 错误规范

**E1 新建 `server/api/errors.py` 注册表 + `error_response()` —— WRONG**
四处硬伤：
(1) 数目错：46 处 `_err` 不是 42 处。
(2) **层次倒置**：让 `common/pgdb/retention.py:130 RetentionInvariantError` 继承 `server/api/errors.ApiError`
——`common/` 与 `worker/` 全树对 `server/` 的 import 数为 0，而且它只在 `retention.py:217/443` 抛、
**全仓 retention.py 之外零处 catch**，根本到不了 HTTP 边界。
(3) 「保留键命中就 raise」把错误路径变成异常路径：一个本该回 409 `cursor_below_retention` 的请求会变成 500。
(4) **最要命**：`error_response` 的 body 含 `server_time_utc`（`sync.py:183 _now_iso()`），逐次不同；
`run.py:cmd_selfcheck` 连跑两遍比整个 body。**任何被 error_response 化的响应体一进基线就必然不确定**——
这正是今天 64 步里 3 个非 200 全是 `HTTPException` 的 `{"detail": ...}`、一条 `_err` body 都没有的原因。
**它与 2.4（扩基线）直接冲突，而计划一个字没提。**
**要重提**：先给 `harness.py` 加 `server_time_utc` / `request_id` 的字段归一化（今天 harness 只有
`_numeric_but_not_bool` 一处类型宽容，没有任何字段归一化机制），或把 app.py 的 HTTPException 路径**排除**在 error_response 之外；
`ApiError` 放 `common/`（4.1 之后就有家了）；注册表放 `sync.py:110` 旁边（已由 2.3 完成）。

**E2 scoped `RequestValidationError` 处理器 —— BREAKS_CONSTRAINT（C1）**
`docs/sync_contract.md:223-225` **逐字写着**（已复核）：
「422 有两种响应体：契约层面的错误带 `{"error":...,"detail":"…"}`；框架层面的类型错误（例如 `after_seq=abc`）
只带 FastAPI 标准的 `{"detail":[…]}`（detail 是数组）。两者都不要重试。」
——这不是实现漂了，**这是已发布契约明文规定的两形状**，改它就是单方面改契约。
支撑它的 finding 也是错的：`server/api/sync.py:488-489` 的 `after_seq`/`limit` 是**裸 `Query(...)`，没有 ge/le**（已复核），
越界值走的是 `:496-501` 的 `_err(422,"invalid_parameter")`，早就带 error 键；
只有「类型解析失败」走框架 422，而那正是文档特意豁免的那一种。
另一处：`export_incremental.py:365` 的 cursor 是 `Query(0, ge=0)`，**没有 le**（已复核），
所以 `cursor=2**63` 会一路走到体内那条检查，不可达的只是 limit 那一路。
**要重提，按这个形状重提**（verify 给的更小的解法，需单独提案 + 单独验证）：
`server/api/export_incremental.py:365-366` 把 `ge/le` **摘掉**，改成在 handler 体内用
`_sync._err(422,"invalid_parameter",...,parameter=...)` 校验——这正是隔壁 `sync.py:488-489 + 496-501` 已经在用的写法。
收益一样（`docs/incremental_export_contract.md:194` 承诺的 `invalid_parameter` 真的会出现，而那份文档**没有** sync 那样的豁免段落），
改动范围从「app 全局 + 路径白名单 + 47 条其他路由的回归风险」缩成一个 handler 的前 4 行，
**并顺带修掉一个 C4 缺陷**：`DB_BACKEND=sqlite` 下 `GET /api/export/incremental?limit=9999` 今天回 422，
而不是 `docs/incremental_export_contract.md:195` 承诺的 503 `event_stream_unavailable`
（Query 校验在 handler 体之前，`:369-373` 的 `_unavailable()` 在体内）。

**E3 `_save_settings` 返回成功与否 —— WRONG（方向对，清单错）**
`server/app.py:2620` 不是调用点（是 `if source_file and os.path.isfile(source_file):`）；
`:2657` 不是调用点（是空行）。
`_save_schedules` 的真实调用点是 **7 处**：`:2566`、`:2600`、`:2623`、`:2652`、`:2692`、`:2703`、`:2715`。
「四个 schedule 变更端点」**漏掉了后三个 legacy 端点**——而它们就在黄金第 11 步 openapi 的 51 条 path 里，
是 erpAPI 最可能真在用的那一组，正好是待确认清单该覆盖的对象。
**要重提**：补齐 7 个调用点 + 把 legacy 三个列进 X.1。核心行号是对的
（`:105-112` 确是 `except Exception as e: logger.warning(...)` 且无返回值；`:1913-1915` 确是 `return {"ok": True, ...}`）。

**E4 对齐 `PUT /api/schedules/{sched_id}` 的入参校验 —— WRONG（只解了一半）**
time 那半是对的（`:2588-2594` 确是 try/except pass，POST 侧 `:2524` raise 400）。
**interval_days 那半事实错误**：`server/app.py:2596` 的 `val = int(body["interval_days"])` **完全没有 try 包着**（已复核）。
所以今天 `PUT {"interval_days":"abc"}` 不是「静默忽略回 200」，而是未捕获 ValueError → **text/plain 500**。
按提案字面「照 `:2527` 抛 400」只处理 `val<1`，非数字那条仍旧 500。
**要重提**：两个分支一起处理；并把 `server/app.py:2696-2704` / `:2707-2716` 一起收进来——
`PUT`/`DELETE /api/auto-scrape/schedules/{index}` 在 index 越界时（`:2700`/`:2710` 的 `if 0 <= index < len(schedules)` 为假）
**什么都不做却照样 `return {"ok": True, ...}`**，是**第二处「对客户端撒谎」**（原计划断言 `_save_settings` 是唯一一处，这是错的）。

### 分层拆分

**S0 补黄金网（28 个未覆盖端点）—— WRONG**
「schedules 与 auto-scrape（文件态，隔离度高）」是这一组里**最不确定**的，不是最干净的。三处不可重复源：
(1) `server/app.py:2532 sched_id = f"sched_{uuid.uuid4().hex[:8]}"`（已复核）与 `:2677` 同样一句，
随机值直接进响应体 `{"id": sched_id}` 与 `source_file` 路径；
(2) `:2549` 的 `last_run_date` 是**纯日期串**，`harness.py:25-27` 的 `_TS_RE` 要求带 `HH:MM:SS` 才匹配，
`_VOLATILE_KEYS` 里也没有它——**录完第二天 verify 必红，而 selfcheck（同一天连跑两遍）看不见**；
(3) `:2645-2646` 的 `batch_name` 是分钟精度，跨分钟即漂。
算术也错：`scenario.py:91-296` 实际覆盖 **31** 个 app 端点（第 11 个 `rec.call` 是 `/openapi.json`，不算端点），
未覆盖 28 个——提案自己列的清单正好 28 条，与标题「27 个里的 20 个」「32/59→52/59」自相矛盾（真值 31/59→59/59）。
还有一个它没看见的硬筛：**单一基线文件要求两后端逐字节相同**，
而 `/api/_debug/event-stream` 在 sqlite 回 `{"enabled":false,"reason":"event stream is postgres-only"}`、
PG 回一整包 `event_stream_stats`——它被列进「只读探针」第一组，**实际上永远录不进同一份基线**。
**要重提**：逐端点先过「确定性 + 双后端字节相同」两道筛，再排序；schedules 那组必须先解决 uuid4 与 last_run_date。
（2.4 是这条的可执行子集。）

**S1 抽 `state.py` / `shared.py` / `background.py` —— WRONG**
(1) 漏了一个会红的守卫：`tests/pgdb/test_rollback_and_ordering.py:241`（已复核，共三条 `_app_has`：`:165`、`:207`、`:241`）
逐字钉的是 `server/app.py:434-438` 的 `ORDER BY updated_at DESC NULLS LAST, id DESC LIMIT 30`，
那段在 `_timeout_task_loop`(`:303-448`) 里——正是 S1 要搬进 `background.py` 的东西。
而且它在 `tests/pgdb/` 下（`conftest.py` importorskip asyncpg），**只在 PG 侧翻红，sqlite 侧全绿**——C4 意义上的单侧生效。
(2) 「三个 PG 夹具」不全：`tests/pgdb/test_sync_api.py:470` `srv.db = db` 与 `:479` `srv.db = old`（已复核）
是第 4 处直接写 `server.app.db`，是普通属性赋值（连 `raising=False` 都谈不上）。
**本轮的替代姿态见 §3 Phase 3 的承重前提：不搬任何全局。**
**要重提**：补齐这两处 + 把 `_app_source()` 改成能同时扫多个文件。

**S4 `server/api/schedules.py`（「整域零数据库访问」）—— WRONG**
`server/app.py:2648-2649` 就在 `api_run_schedule_now` 里：`await db.create_batch(...)` / `await db.create_tasks(...)`。
而且「只碰 `_runtime_settings`」与 S5 自相矛盾——`:2459-2461` 的 `_save_schedules` 既写
`_runtime_settings["auto_scrape_schedules"]` 又调 `_save_settings()`，schedules.py 会成为 `_runtime_settings` 的第二个写方。

**S5 `server/api/settings.py`（「写方从 6 处收敛到 1 个模块」）—— WRONG**
payoff 的核心主张不成立：`server/app.py:2460` 的 `_runtime_settings["auto_scrape_schedules"] = schedules` 归 S4，
是第 2 个写方。收敛结果是 2 个模块不是 1 个。其余行号都对。

**S10 `server/api/batches.py` —— WRONG（修法不成立）**
`tests/pgdb/test_rollback_and_ordering.py` 有**三条** `_app_has` 断言（已复核 `:165` / `:207` / `:241`），不是两条：
`:165`→`server/app.py:1492`、`:207`→`:1480`、**`:241`→`:434-438`（在 `_timeout_task_loop` 里）**。
把 `_APP_PY`(`:34-35`) 单点「改指本文件」会让第三条**永远 False**。
**要重提**：`_app_source()` 拼接多个文件，或每条断言各带自己的目标文件。
（另：提案说「逐字断言 1470-1481 的两条 ORDER BY」，真值是 `:1477-1480` 与 `:1489-1492`。）

### 去重

**D1 `common/sentinels.py` 里让 `worker/engine.py:1450/1467/1477` 改用 `SCRAPE_FAILURE_TITLES` —— WRONG**
那三处**不是同一个集合的第四份内联**，是三条不同控制流：
`:1450 title == "[验证码拦截]"` → `session.solve_captcha` + `last_error_type="captcha"` + `slot.rotate`；
`:1467 title == "[API封锁]"` → `last_error_type="blocked"` + `slot.rotate`；
`:1477 title in ["[页面为空]","[HTML解析失败]"]` → `last_error_type="parse_error"` + `slot.rotate_on_empty`。
换成一个 4 元素集合的 `in` 会把三条合成一条，而 `error_type` 直接决定
`common/pgdb/relay.py:222-224` 的 outcome 映射（captcha/blocked→`blocked`，其余→`parse_failed`）。
另：`common/slowhash.py:3-4` 明写「本模块自包含，只依赖标准库，不 import 任何 `common.*`/`worker.*`」——
让它改成从 `common/sentinels` 导入会推翻这条写进 docstring 的不变量，提案没提要同步改。
**要重提**：收成四个**具名单值常量**（不是集合），且先处置 slowhash 的自包含约束。
（其余三处再导出——`export_incremental.py:91`、`tools/phase5_compare.py:57`（两者逐字节相同）、`relay.py:213-216`——是可做的，可作为一条更小的提案单独提。）

**D2 export `_clean` 加认方括号哨兵 —— BREAKS_CONSTRAINT（C1）+ 前提是伪的**
它改的正是契约 v1 必填字段 `slow.title` 的取值（`export_incremental.py:268`），提案自己也说要升 `CONTRACT_VERSION`
——那说明它该被**列进待确认**而不是列进 proposals。
而且**前提事实错误**：`[验证码拦截]` 根本到不了 payload——`worker/engine.py:1450-1465` 撞到就 `continue` 回重试循环，
最终走 `success=False` 提交；`common/pgdb/relay.py:302-305` 逐字写明了这件事。
唯一走 success 路径的 `[商品不存在]` 也不在 payload 里：`worker/engine.py:1277-1279` 用
`_NOT_FOUND_PRESERVED_FIELDS`(`:104-113`) 把 SLOW_HASH_FIELDS 全体删键，而 title ∈ REVIEW_HASH_FIELDS ⊂ SLOW_HASH_FIELDS，
所以 404 提交体里**没有 title 键**。**今天 `slow.title` 已经是 null——这条修的是一个不存在的现象，代价却是动契约。**

**D3 `_NA_VALUES` 改大小写不敏感 + `:188 startswith("[")` 换全等 —— WRONG**
(1) 「`[2-Pack] Storage Bins` 这类真标题今天会被打成 server_reject」不成立：
`common/database.py:186-195` 的 `startswith("[")` 只让 `has_valid_info` 变 False，接着还要过
`:194 all_empty = all(data.get(f) in _NA_VALUES for f in key_fields)`——
一个真商品有 `current_price`/`stock_count`/`brand` 任一非 NA 就 `all_empty=False` → 返回 False → **不是**解析失败。
所以那个「有意保留的 bug」今天造不成 server_reject。
(2) `breaks_golden=yes` 与「diff blob 就是评审物」是错的：golden 的 `_product`(`scenario.py:42-86`)
title=`"Golden Test Product B0GOLDEN01"`、brand=`"GoldenBrand"`，改前改后 `has_valid_info` 都是 True；
失败步骤 `submit_result_failed`(`:182-187`) 不带任何字段，改前改后都判 `all_empty`。
**这条改完 golden 不会红，重录出来的 blob 是空的。**
(3) 方向危险：改松 `_NA_VALUES` 会让更多记录被判成 server_reject（`stock_status="NULL"` 今天不算空、改后算空），
「更多拒绝」比「更多接受」危险。
**要重提**：先给 `scenario.py` 的 `_product()` 加脏值行并重录一次「无行为改动的扩样本基线」，
再用 `tools/phase5_compare.py` 那套工具链统计真机数据的翻转条数，然后才谈改。
**⚠ 顺带的 C2 发现**：`common/pgdb/_shared.py:15-16` 标注的「含有意保留的 bug，必须逐字共享」
存在的唯一理由就是两个后端必须逐字一致——**SQLite 一旦退役，这条约束当场消失，本条从「推翻已归档决策」降级成「单后端的普通行为修正」。**

**D4 价格解析统一（`_parse_price_float` 变成 `_price` 的别名）—— WRONG**
(1) `breaks_golden=yes` 空头支票：基线里 `export_batch_csv` 的总价列已实测为 `$19.99`/`$33.50`，
输入是 `buybox_price="19.99"/"33.50"`、`buybox_shipping="0.00"`（`scenario.py:54-56`），
两种实现结果完全相同 → **golden 不会红、blob 为空**。
(2) 漏了一个方向相反的行为变化：`_parse_price_float` 的 `[^\d.-]` **保留负号**，
`_price` 的 `\d[\d,]*\.?\d*` **丢负号**——`"-5.00"` 会从 -5.0 变成 5.0。
(3) 漏了影响面：`_parse_price_float` 不只喂 `server/app.py:2032-2035` 的 total_price，
还喂 `common/database.py:210-211` 的 `_compare_price` → `asin_changes` 的涨跌 → `/api/changes/stats` 与
`/api/results?change_filter`。**这是写库语义变化，不是导出格式变化。**
**要重提**：先加脏值样本 + 明确处理负号 + 把 `asin_changes` 的口径变化写进 erpAPI 待确认清单。

**D5 多值字段 join/split 对账 —— WRONG**
(1) 「没有任何测试断言 join 侧与 split 侧对得上」是错的：
`tests/test_export_multivalue_split.py:89 ParserStillJoinsWithNewline`（已复核，`:102-105` 把
`_slx_parse_bullet_points`/`_slx_parse_images`/`_parse_bullet_points`/`_parse_images` 四个函数体切出来断言含 `"\n".join`）
**就是一条源码级 join 侧守卫**。要做的是照这个模式**扩到 category_tree**，不是从零发明。
(2) `breaks_golden="unsure"` 错得比 `no` 更危险：**golden 从不执行 `worker/parser.py`**——
`scenario.py:170-180/201-206` 是直接把 `_product()` 造的 dict POST 给 `/api/tasks/result`，
`category_tree` 的值 `"Home > Test > Sub"` 是场景写死的常量。**不是「可能红」而是「零覆盖」，C3 的风险一点网都没有。**
(3) 补充事实：`worker/parser.py:987 " > ".join(names)` 与 `:2490 " > ".join([n.strip() for n in names if n.strip()])`
**今天就不一致**（前者不 strip 单个节点名），统一它们本身就是行为改动。
**要重提**：export 侧（`export_incremental.py:171/227`，PG-only 路由）可以单独做，风险为零；
parser 侧单独立项、单独声明、单独重录，且先解决「两条引擎路径今天不一致」这个 D 级决策。

### SQLite 退役

**Q1 第 1 步 `common/domain.py` + `from common.domain import *` —— WRONG**
**机制本身不成立**：星号导入不导入下划线开头的名字（模块无 `__all__` 时），
而要搬的 22 个符号里有 **15 个是下划线开头**（`_fail_cap:54`、`_record_wait:82`、`_record_hold:89`、
`_NamedLockCtx:109`、`_NA_VALUES:168`、`_normalize_screenshot_path:171`、`_is_parse_failure:181`、
`_parse_price_float:198`、`_compare_price:209`、`_compare_stock_qty:221`、`_compare_stock_status:239`、
`_HASH_FIELDS:251`、`_compute_content_hash:265`、`_compute_title_bullets_hash:270`、`_ASIN_DATA_COLUMN_SET:294`）。
后果：`server/app.py:1921` 直接 ImportError，且 `class Database` 自己的方法体 NameError。
调用点还漏了 4 个测试文件。**已由 4.1 取代（显式再导出 + 补齐清单）。**

**Q2 第 3 步 `reset_identities()` —— BREAKS_CONSTRAINT（C4）**
它排在「删 SQLite」之前，此时 `DB_BACKEND=sqlite` 仍必须能走通，而提案**只给了 PG 实现**
→ `server/app.py:2801-2814` 是两后端共用的 handler，SQLite 侧 AttributeError → `DELETE /api/database` 500。
还漏了 `common/pgdb/__init__.py:56-84 PUBLIC_API` 与 `tests/pgdb/test_skeleton.py:27-41`。
**已由 3.8 批 (1) 取代（明确要求两侧同时实现 + 进 PUBLIC_API）。**

**Q3 第 4 步 删 `class Database` + `dbfactory` —— WRONG**
调用点清单不完整，**照它做会把黄金回归网自己打死**：
`tests/golden/harness.py:234 import common.database as database`、
`:235 from common.dbfactory import get_database_class, is_postgres`、
`:243 db_cls = get_database_class()`、`:281 if is_postgres():`（已复核）。删掉这两个模块，GATE 1/GATE 2 都起不来。
其余漏掉的：`tests/test_event_stream_endpoint.py:12,24-26,32`（逐条断言 `body["backend"]==get_backend()`、
`body["enabled"]==is_postgres()`，且它在 `tests/` 根下，Phase 0 的 `--ignore` 之后仍然跑）、
`tests/test_engine_not_found.py:73-77`、`tests/test_slowhash.py:536-541`、`tests/test_golden_with_relay.py:52`、
`tests/pgdb/test_results_read.py:20`、`tests/pgdb/test_skeleton.py:31,48`、`tests/pgdb/test_admin.py:401`；
`server/api/sync.py` 的守卫是**三段**（`:213-216` 非 PG、`:217-220` db is None、`:221-223` 无 event_relay_metrics），
不是一段；依赖方向也反了——**asyncpg 不在 `requirements.txt`，它在 `requirements-dev.txt:7`**，
退役后必须把它挪进生产依赖。

**Q4 第 5 步 给 `/api/tasks/release` 加 `as_int` —— BREAKS_CONSTRAINT（C4）**
`as_int` 的唯一实现在 `common/pgdb/pool.py:214-229`，而 `pool.py:74` 是模块级 `import asyncpg`。
从两后端共用的 `server/app.py` 里 import 它，等于让 SQLite 部署硬依赖 asyncpg——
这正好推翻 `common/dbfactory.py:15-17` 白纸黑字的设计（「pgdb 是惰性 import 的…所以 SQLite 部署不需要装 asyncpg」）。
**修法**：`as_int` 是纯函数，随 4.1 搬进 `common/core`（已写进 4.1）。之后这条可以重提。
（顺带记一条这条 proposal 没说的硬约束：黄金第 11 步把 `/openapi.json` 逐字钉死，
今天 `server/app.py:1601` 收的是裸 `Request` 所以在 handler 体内强转不动 schema；
一旦有人顺手改成 pydantic 模型，黄金当场红。）

---

## 6. 刻意没做什么

> 照 `.agent/MIGRATION_STATUS.md` 的规矩：把没做的和做了的一样写清楚。

**C1. 仪表盘默认查询的 O(N) 翻页（batch_id + `ORDER BY d.id DESC`）。**
实测第 5/20 个批次一页 51 行：PG 走 Nested Loop，`Index Scan Backward using asin_data_pkey` 扫了 **100,051 行**
去 `batch_asins` 逐行探测，共 **340,724 buffers / 262-293ms**；最新批次同一查询 0.24ms。
**越老的批次、越往后翻越慢。** SQLite 侧 90.8ms（`USE TEMP B-TREE FOR ORDER BY`）。
唯一的方案（性能 P6）事实有误被驳回。退路 `WITH b AS MATERIALIZED` 只有 4.7x（262→55.7ms）且仍是 O(批次大小)。
**本轮不修，留作独立立项。** 重提时必须先补齐 P6 缺的三样：SQLite DDL 的正确行号（`common/database.py:465-472`）、
`results_write.py:747` / `database.py:1974` 那条写的代价评估（它在写锁内）、count 侧与 `batch+change_filter` 组合的拼装。

**C2. `xlsx` 导出的「看起来像挂了」。**
`server/app.py:2124-2130` 迭代 → `:2143 wb.save` → `:2152-2168` 才开始 yield。
29 万行导出期间 HTTP 响应完全静默数分钟，同时 `results_read.py:338` 的 `async with self.read()` 把一条池连接
（`common/config.py:39 PG_POOL_MAX=10`）占满全程。**不是「慢」，是「看起来像挂了」**，优先级低于 Phase 1 三条。
（csv 分支没问题。）

**C3. D-8 那个刻意复现的 COUNT 崩溃。**
`common/pgdb/results_read.py:52-69`：`?search=<≥3字符>&cursor=<id>` 今天是 500，
且被 `tests/pgdb/test_results_read.py:278 test_count_bug_is_reproduced` 钉住。
它属于 Phase 1.5 的 COUNT 重构，**需要显式声明并重录基线，不要顺手在本轮「捎带修好」**。
1.3 必须实测它仍绿。

**C4. `pool.py` 的 `?`→`$n` 翻译与 aiosqlite 形状面清理（约 187 行）。**
`common/pgdb/pool.py:323-387`（`translate_sql`/`qmark_to_numeric`）、`:389-443`（Cursor）、`:445-469`（`_ExecOp`）、
`:534-552`。删它要改 316 个占位符、178 个 `.execute(` 调用点、47 处 `rowcount` 读取，跨 11 个文件。
**触发条件不是「SQLite 退役了」，而是 Phase 1.5（放开写并发）**——`pool.py:33-37` 的 D-2 明写
「换掉它的前提是先把 app.py 里的裸 SQL 抽干净」，那正是 3.8 在做的事。**单独立项，不要和退役同一个 PR。**
**必须把 breaks_golden 从 "unsure" 定死**：正确重构不动黄金，**错误重构黄金也照样绿**——
绑错参数不报错，只是查错行，而基线只覆盖 36 条 path、搜索步骤全是单一大小写 ASCII。真答案是「golden 抓不到」。

**C5. `ascii_lower` / `LIKE ... ESCAPE ''` / `text_affinity` 的 SQLite 口径（约 106 行）。明确不做。**
`common/pgdb/pool.py:128-135`（`ascii_fold`）、`:145-206`（`text_affinity`）、`:250-281`（LIKE 改写）。
`pool.py:250-271` 的注释逐字记着实测：39 探针 × 5 种写法，`ILIKE` 有 9 处与 SQLite 不一致，`ascii_lower` 是 0 处；
`DELETE /api/results {"search":"back\\slash"}` 在两种转义语义下删的是不同的行，**而两边都回 `{"deleted":1}`**。
`ascii_lower` 还与 `common/pgdb/schema.py:64` 建的三个 trgm 表达式 GIN 索引逐字绑定，改了要重建索引。
`text_affinity` 改动会改落库值 → `content_hash` → `asin_changes` → 交付给 erpAPI 的数据（踩 C3/D-27）。
**这笔账本身就是负的（收益 106 行，代价一次不可逆的导出数据变更），跟 SQLite 去留无关。**
**一处必须拆开说的自相矛盾**：原计划把这条标 `breaks_golden=yes`，同时又说黄金对 LIKE 语义是瞎的——两者都对，但被混成一条了。
真相是：**删 `ascii_lower`/`ESCAPE ''` 黄金全绿（无网）；删 `text_affinity` 的 SQLite 口径黄金当场红**
——因为基线 items 里 `"content_hash": "d608fc205fbfe478665c624edc858f1d"` 是逐字钉死的。要分开标，否则后来人会以为 LIKE 那半边有网。

**C6. `worker/parser.py` 一个字节没碰。** D-27 不触发，导出数据的值不变。
顺带记一笔：从字节数看最大的单一杠杆其实是 `long_description` 的 10000 字符上限
（`worker/parser.py:929`、`:2452`），它一个人占了行宽的 2/3——但改它会改导出数据的值、改
`slow_hash`（`common/slowhash.py:114` 里 `long_description ∈ SLOW_HASH_FIELDS`）、顺着
`/api/export/incremental` 流到沃尔玛侧。**明确不动。**

---

## 7. 已知盲点（五份 verify 的 `missed` 汇总）

### A. 网的问题（最要命的一组）

**A1. `tests/pgdb/` 才是 C4 的执行机制，五份计划里只有一份提到。**
`test_results_read.py:267`（50 条）与 `:318`（25 条）逐字段比对两个后端，
参数矩阵覆盖 `batch_id × change_filter` 全组合；`test_tasks.py:135 test_pull_tasks_prefer_zip_wins`
覆盖的正是黄金完全没覆盖的 1.2 拆分路径。**「先跑一遍黄金全绿」是不够的。**（已写进 §1 规矩 1。）

**A2. 三处「diff blob 就是评审物」是空头支票。**
黄金样本太干净：价格全是 `19.99`/`33.50`/`0.00`，title 全是 `Golden Test Product ...`，
brand 恒为 `GoldenBrand`，`stock_status` 恒 `In Stock`，搜索样本全 ASCII 单一大小写。
于是 §5-D3（哨兵语义）与 §5-D4（价格解析）改完基线**逐字节不变**，重录出来的 blob 是空的。
**要让基线真的当网用，必须先给 `scenario.py:42-86` 的 `_product()` 加一批脏值行**
（`"$12.99 with free shipping"`、`"[2-Pack] Storage Bins"`、`stock_status="NULL"`、`buybox_price="-5.00"`）
并重录一次「无行为改动的扩样本基线」。**本轮不做**（它本身就是一次基线重录，应当单独提案），但记在这里。

**A3. 黄金从不执行 `worker/parser.py`。**
`scenario.py:170-180/201-206` 直接 POST 造好的 dict 给 `/api/tasks/result`。
**C3 管辖的所有改动在黄金上是结构性零覆盖，不是「可能红」。**

**A4. 黄金不比任何响应头。** `harness.py:378-404` 的 `diff_steps` 只比 status / content_type / body。
任何 `Retry-After`、`request_id` 回传头，改没改都不会红。要守就得单独写用例。

**A5. `error_response` 的 `server_time_utc` / `request_id` 与基线确定性冲突。** 详见 §5-E1。
若日后复活 `errors.py`，必须先给 `harness.py` 加字段归一化——今天它只有 `_numeric_but_not_bool` 一处类型宽容，
**没有任何字段归一化机制**。

**A6. Phase 3 的三处无网搬运**：3.3 的 `workers/{id}/restart` 与 `DELETE /api/workers/{id}`、
3.4 的 4 个 seller 端点（**且补不进黄金**，响应含逐次不同的批次名）、
3.6 的两个删除端点。已在各步给了替代验证，但这是本轮最大的裸奔面。

**A7. 3.8 批 (3) 的 LIKE 转义。** 必须补反斜杠/百分号/下划线的双后端用例，否则等于把 D-16 那个已知不一致搬了个家。

### B. 未被任何计划测绘的东西

**B1. `get_change_stats`（`common/pgdb/results_read.py:447-457` / `common/database.py:2476` 起）**
跑的是无 batch 过滤的 `SELECT change_type, COUNT(DISTINCT asin) FROM asin_changes GROUP BY change_type`，
没有任何可用索引，对一张按 10 万/天增长的表做全扫 + DISTINCT 聚合；
而它由 `server/templates/results.html:254` 在**每次打开结果页时无条件请求**（`server/app.py:1892`）。
与性能 finding #1 的 COUNT(*) 是同一类问题、同一个页面，**整段被跳过了。**

**B2. `get_total_asins`（`common/pgdb/results_read.py:439-441`）** 是另一条无条件 `SELECT COUNT(*) FROM asin_data`，
调用点 `server/app.py:2284`。暴露面与 finding #1 完全相同，既没测也没提。

**B3. 21/29 处 `except Exception` 未被测绘。** 2.1 只处置了 8 处裸 pass。
剩下的至少 `server/app.py:2144`（except 里 `run_in_executor(_close)`）与 `:2251-2252`（except 里再套 try）
不是简单的 log-and-continue，属于控制流分支；`:864-865` 与 `:890-891` 把异常压成字符串返回值
（`"invalid_url:{e}"` / `"dns_fail:{...}"`）——那正是 `:1053` 那条 400 的来源，且这些字符串会进 400 的 detail。
**一份自称「测绘」的计划留白 72%。**

**B4. 时间戳的解析侧完全没数。** 4.3 只收了写侧。解析侧同样有重复：
`server/app.py:283-284`（`_cst_filter`）、`:585-586`、`:1265-1266`、`common/pgdb/relay.py:250 _CRAWL_TIME_LEGACY_FMT`。
更直接的是 `server/app.py:583-587` 与 `:1263-1268` 是**同一段「批次持续时长」计算的两份内联副本**。
全仓非测试代码里 `'%Y-%m-%d %H:%M:%S'` 字面量共 57 处，4.3 只覆盖了 strftime 那一半。

**B5. 界面上两列时间是两个时区。** `server/app.py:282-285 _cst_filter` 做 `+8` 转换（`dashboard.html:107` 的 `created_at` 用它），
而 `results.html:339` 的 `crawl_time` 是 JS 直接 `esc(r.crawl_time)`，不转。
且 D-61 之后 `crawl_time` 是 RFC3339，`_cst_filter` 的 `strptime` 会静默 `except ValueError` 原样返回。
**这是产品决策（界面该显示哪个时区），不是收敛问题**，本轮不出提案。

**B6. 第二处「对客户端撒谎」**：`server/app.py:2696-2704` / `:2707-2716` 的 auto-scrape index 越界回 `{"ok": True}`。详见 §5-E4。

### C. 计数与行号的订正（照 C5，错行号等于没给）

| 说法 | 真值 |
|---|---|
| 黄金覆盖 32/59 端点 | **31/59**（第 11 个 `rec.call` 是 `/openapi.json`，不算端点） |
| openapi 是黄金第 5 步 | **第 11 步**（已复核；全仓至少三处注释写错：`server/app.py:264`、`export_incremental.py:95` 附近） |
| 42 处 `_err` 调用点 | **46 处**（sync.py 40 + export_incremental.py 6） |
| `test_results_read.py` 90 条 / 80 条纯 diff | collect **97**，依赖 `seeded_sqlite` **93**，纯 diff **88** |
| `common/database.py` 5000 行 | **2486 行**（`class Database` 从 `:297` 起 = 2190 行） |
| README:551/552/556 | **README:553/554/558**（三处统一偏移 2 行） |
| `pool.py:290` 的字典键 | **`pool.py:291`**（`_STATEMENT_OVERRIDES` 在 `:285-297`） |
| `test_rollback_and_ordering` 两条 `_app_has` | **三条**：`:165` / `:207` / `:241` |
| `server/app.py:2531` uuid4 | **`:2532`** |

**成批过期的跨文件行号引用**（拆分是一次性修正的最好时机，或干脆改成引函数名）：
`common/pgdb/pool.py:285` 注释里的 `server/app.py:2654`（真值 `:2810`，差 156 行）、
`pool.py:502` 的 `server/app.py:1298 / 2230 / 2281 / 2289 / 2294 / 2309`（已全错）、
`tests/pgdb/test_skeleton.py:312` docstring 的 `server/app.py:2654`、
`tests/pgdb/test_rollback_and_ordering.py:143/180/218` 的 `server/app.py:1385 / 1378 / 341`（已全错）、
`tests/pgdb/test_admin.py:250` 的 `app.py:1497-1511`（真值 `:1606-1623`）、
`common/slowhash.py:153-158` 里登记 parser 拼接方式写死的 `:764`（真值 `:987` 与 `:2490`）。

### D0. Phase 4.1 审计照出的两个覆盖盲区（本轮登记，未修）

**D0-1. `worker/parser.py::parse_seller_listing` 全仓零测试覆盖。**
这恰好是 4.2 收口 ASIN 正则的**那一侧消费者**。审计把正则改坏之后，
`tests/test_seller_api.py` + `tests/test_parser_quality.py` 共 40 passed 依然全绿，
红的全是 `server/app.py:_normalize_asin` 那条路。
即：门禁守得住 `ASIN_RE` 的**服务端**用法，守不住 **parser** 用法。
今天两边共用同一个对象所以还安全，但将来有人只改 parser 侧的用法，没有任何门会响。

**D0-2. `LIMITED_RETRY_ERROR_TYPES['variant_offset']` 只被 `tests/pgdb` 覆盖。**
把它从 1 改成 2 之后：golden（两个后端）+ pytest sqlite + unittest 全绿，
只有 `DB_BACKEND=postgres` 跑到 `tests/pgdb` 时才红。
**SQLite 侧的重试上限策略实际上没有回归网。**
注意这条与 Phase 0.1 的取舍相互作用：sqlite 列加了 `--ignore=tests/pgdb` 之后，
这类「只有 tests/pgdb 覆盖」的策略常量在 sqlite 列里彻底不设防。

**D0-3. 一处调用侧分叉（计划原文描述有误，已实测订正）。**
计划 4.2 说「`worker/parser.py` 里直接 `_ASIN_RE.match` 不做大小写归一，
而 server 侧 `_normalize_asin` 先 `.strip().upper()`」——**这个描述不准确**。
实测 `parse_seller_listing` 的两条引擎分支在 match 之前**都**做了 `.strip().upper()`。
真正的分叉在 `_extract_page_asin`：selectolax 分支读 `input#ASIN` 时归一后再匹配，
而 **regex 兜底分支**（canonical / currentAsin / input 属性反序那三条）
拿原始 HTML 直接匹配、不归一。
实测判别：同一张小写 ASIN 的页面，tree 路径返回 `'B0G6KPHQ4G'`，regex 路径返回 `None`。

**D0-4. 两份 `_ASIN_RE` 用的是 `$` 而不是 `\Z`**，所以 `'B0G6KPHQ4G\n'` 能匹配。
两份副本今天都这样，属于搬迁范围外，已逐字保留并在 `common/core/idents.py` 标注。
要收紧得单独立项（它会改变既有的接受面）。

### D. 工具与守卫的洞

**D1. `tools/phase5_preflight.py:220-239` 的路由顺序守卫在 catch-all 为 None 时报绿**（`else: ok(...)`）。
拆分后会静默通过——正是它守的那类失效。已写进 3.7。

**D2. 新建的共享模块没有任何守卫。** 仓库里唯一一条「禁止复制粘贴」的执行机制是
`tests/pgdb/test_skeleton.py:85-95` 的 `is` 断言，而它只盯 `_shared.__all__` 那 23 个符号。
4.1–4.5 建的模块应当把它泛化成一条「新共享模块的符号在任何其它模块里不得被重新定义」的 AST 扫描测试
（`tests/test_golden_env_isolation.py` 有 AST 扫 `config.py` 的现成写法可抄）。**本轮不做，登记。**

**D3. `sync.py` 的循环导入守卫会静默缩水。** `tests/pgdb/test_sync_api.py:174-183` 用 AST 禁止 sync.py 模块级 import
以 `("common.pgdb","asyncpg","server.app")` 开头的东西（理由：`server/api/sync.py:27-29`「启动即崩 = 整个 erpAPI 全线下线」）。
本轮新增 8 个 `server/api/*.py` **一个都没有对应守卫**。（好消息：因为不搬全局，黑名单不需要加新条目。）

**D4. `harness.py` 是被源码文本断言的**：`tests/test_golden_env_isolation.py:123-124`
`src.index("_ENV_DERIVED_SETTINGS.items()")` 必须早于 `src.index("with TestClient(srv.app) as client")`。
任何改 harness 的动作必须原样保留这两个字面量（尤其 `srv.app`）。

**D5. `tests/golden/run.py:28` 把基线文件名硬编码。** 退役 SQLite 之后基线要在 PG 上重录并改名，
「重不重录、diff 怎么评审」是退役 PR 必须回答而目前完全空缺的一格。

---

## 8. SQLite 去留的账（C2 要求的显式记账）

**结论：本轮不退役，也不假设它会留。做完 4.1 之后重新算。**

**退役能省的（诚实版）：**

| 项 | 幅度 | 备注 |
|---|---|---|
| CI 时间 | **约 5s**，不是 190s | 其中 184s 是「同一批 PG 用例跑了两遍」，**Phase 0.1 今天就能拿回来，不需要任何退役决策** |
| 生产代码 | 约 2380 行 | `common/database.py` 的 `class Database` 2190 行 + `dbfactory.py` 68 行 + 守卫约 60 行 + `_shared.py` 塌缩 |
| 3.8 的收口 | 每个方法省一半 | 四个方法约省 100 行；**并可整体删掉 `pool.py:264-297` 那套为 app.py 字面量 SQL 服务的翻译垫片（约 35 行）——但那套垫片在 3.8 做完之后本来就该退休，与 SQLite 去留无关** |
| `pool.py` 989 行垫片 | **只有约 5% 是免费的** | `_STATEMENT_OVERRIDES`(`:285-297`) + `_open_read_pool`(`:886-891`) + 三个占位属性(`:832-835`)。**435 行（44%）无论去留都留着**：`ConnProxy` 的 `_op_lock`(`:522`)、事务归属路由(`:570-604`)、`_abort_dangling`/`_release_top_xact`(`:693-740`)、`WriteLock`(`:756-800`)、`PoolMixin`(`:807-989`)——它们的出处是 D-2/D-15，与后端无关 |
| §5-D3 的争议 | **降级一个量级** | `_shared.py:15-16`「有意保留的 bug 必须逐字共享」存在的唯一理由就是双后端逐字一致；退役后它从「推翻已归档决策」变成「单后端的普通行为修正」 |

**退役要付的（唯一一条真实的、非情怀的损失）：**
**没有 PostgreSQL 的机器从此起不了服、跑不了测。** 实测本沙箱 PG 拒连时 `pytest tests/ -q` 仍有
**338 passed / 379 skipped / 6.22s**；退役之后这 338 条会全挂。
`tests/pgdb/conftest.py:2-3` 明写这条设计是有意的。
要保住等价能力得引入容器化 PG（testcontainers 之类），**那是新增依赖和新增 CI 复杂度，必须计入成本，不能算成免费。**

**永久失去的（`git revert` 回不来）：**
- `test_results_read.py` 那 **88 条纯差分用例的唯一裁判**。文件头明写「SQLite 侧是唯一裁判——不写死期望值，
  免得把『我以为的正确』写成契约」。转快照之后只能证明「没变」，不能证明「对」。
  （**0.2 已经把这份快照录出来了，所以这条损失被压到最小——但快照不等于裁判。**）
- `tests/pgdb/test_skeleton.py:27-43/44-70` 对 pgdb 50 个公开方法签名的唯一外部参照物。
  `common/pgdb/__init__.py:92 _assert_api_complete` 会变成自指（拿 `PUBLIC_API` 元组和 pgdb 自己对）。
- `.agent/MIGRATION_STATUS.md` §2 那份 R1..R9 残留差异清单从此不可复核，从「可重跑验证」降级成「文档里的说法」。

**关于「切回 `DB_BACKEND=sqlite` 作为回滚路径」：用户的推理成立，而且比他说的还更弱。**
`.agent/MIGRATION_STATUS.md` §7.1 明写本项目采用路径 A（全新部署 + 全新 PG 库 + 全新 worker，旧系统在原机器上继续跑不动），
§7.3 也专门写了路径 A 的回滚是「直接停用新系统，旧系统全程没被碰过」。
更强的一点：切回 sqlite 拿到的不只是空库——`GET /api/export/incremental` 的数据源是 `scraper.scrape_events`，
`server/api/sync.py:213-216` 在非 PG 后端一律 503 `event_stream_unavailable`，
即**一个沃尔玛契约整体离线的服务（503 而不是空页）**。这条路的真实回滚价值是 0。

**本轮对退役做的唯一实质动作：4.1（`common/core/`）。**
它把「删掉 `common/database.py`」从一次带隐藏依赖的手术，变成一次 `git rm` +
删 `common/pgdb/_shared.py`（届时 pgdb 直接导 `common/core` 即可）。
**退役本身等 erpAPI 清单到手 + 用 Phase 0 之后的真数字重新算一次账。**