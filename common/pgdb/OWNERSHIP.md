# common/pgdb 方法归属与实现约定（Phase 1 唯一真源）

SQLite → PostgreSQL 移植的骨架。**六个实现 agent 各认领一个文件，互不越界。**
本文件是分工与决策的唯一真源；与 `.agent/pg_migration_plan.md` 冲突时以本文件为准
（该计划书写在黄金基线录制之前，有两处已被基线证伪，见决策 D-1 / D-3）。

---

## 0. 一分钟上手

```bash
# 导入自检（会跑公开面完整性 + 单一真源检查）
.venv/bin/python -c "import common.pgdb"

# 骨架自检（纯函数 + DDL + 垫片；连不上 PG 会自动 skip）
.venv/bin/python -m pytest tests/pgdb -q

# SQLite 黄金校验（移植期间每改一块就跑；必须一直是 64/64）
.venv/bin/python -m tests.golden.run verify

# 全包验收（Phase 1 完工判据）
DB_BACKEND=postgres .venv/bin/python -m tests.golden.run verify
```

写自己的用例：

```python
import pytest

@pytest.mark.asyncio          # 仓库是 pytest-asyncio strict 模式，必须逐个打
async def test_xxx(pgdb):     # pgdb 夹具 = 一个全新的临时库，用完即删
    assert await pgdb.create_batch("b1") == 1
```

---

## 1. 文件清单

| 文件 | 状态 | 负责人 |
|---|---|---|
| `common/pgdb/_shared.py` | ✅ 已完成（纯再导出，**不准**加任何定义） | 骨架 |
| `common/pgdb/pool.py` | ✅ 已完成（连接层 + aiosqlite 形状垫片） | 骨架 |
| `common/pgdb/schema.py` | ✅ 已完成（完整 DDL + 列序断言） | 骨架 |
| `common/pgdb/admin.py` | ✅ 已完成（4 个 no-op） | 骨架 |
| `common/pgdb/__init__.py` | ✅ 已完成（Database 组装 + 导入期自检） | 骨架 |
| `common/dbfactory.py` | ✅ 已完成（DB_BACKEND 开关） | 骨架 |
| `common/pgdb/batches.py` | ✅ 已完成 | agent C |
| `common/pgdb/tasks.py` | ✅ 已完成 | agent D |
| `common/pgdb/results_write.py` | ✅ 已完成 | agent E |
| `common/pgdb/results_read.py` | ✅ 已完成 | agent F |
| `common/pgdb/media.py` | ✅ 已完成 | agent G |
| `tests/pgdb/helpers.py` + `conftest.py` | ✅ 已完成（临时库夹具） | 骨架 |
| `tests/pgdb/test_skeleton.py` | ✅ 已完成（21 条契约自检） | 骨架 |
| `tests/pgdb/test_concurrency.py` | ✅ 已完成（单写连接并发回归） | 收口 |

**Phase 1 状态：完工。** 两个后端各自 `64 步与基线完全一致`，
`pytest tests/ -q` 全绿。收口阶段做的事见 §3 的 D-13 / D-14。

**没人碰 `common/database.py`。** 每个 agent 只碰自己那一个文件 + 自己的
`tests/pgdb/test_<domain>.py`。骨架文件（pool / schema / __init__ / _shared /
admin / dbfactory / harness / app.py）已经定稿；需要改动请先提出来，不要直接动手——
六个人同时改 pool.py 就没法并行了。

---

## 2. 方法归属：50 个公开属性，一个都不能少、一个都不能重

`common/pgdb/__init__.py` 在**导入期**就会校验这张表（`_assert_api_complete` +
`_assert_single_owner`）：漏了方法、或者同一个方法被两个 mixin 定义，`import
common.pgdb` 直接炸。多重继承下重复定义不会报错、只会被 MRO 静默遮蔽，那正是
"改了 A 文件却没生效"的来源。

### pool.py（10 项）— PoolMixin · MRO 必须排第一
| 方法 | SQLite 出处 | PG 侧 |
|---|---|---|
| `__init__(db_path=None)` | 300 | 保签名；DSN 来自 `PG_DSN` |
| `connect()` | 316 | 建池 + 建写连接 + `init_tables()` + 预热 |
| `close()` | 408 | 关连接与池 |
| `read()` | 360 | `@asynccontextmanager`，从池借连接 |
| `_open_read_pool()` | 342 | **no-op**（签名保留） |
| `_db`（property） | 302 | 单条专用写连接的代理 |
| `_write_lock` | 306 | `TimedLock`（从 common.database 共享） |
| `_tx()` | — | 新增：`async with self._tx() as conn:` |
| `translate_sql` / `qmark_to_numeric` | — | 新增：`?`→`$n` + LIKE 折叠改写 |
| `rowcount_from_tag` / `text_affinity` / `as_int` / `ascii_fold` | — | 新增工具 |

### schema.py（3 项）— SchemaMixin
`init_tables()` (439) · `verify_schema()`（新增） · `reset_all()`（新增）

### admin.py（4 项）— AdminMixin
`run_startup_optimize()` (372) · `maintenance_loop()` (385) ·
`start_maintenance()` (402，**同步方法**) · `wal_checkpoint()` (426，恒返回 None)

### batches.py（8 项）— BatchesMixin · agent C
`create_batch` (799) · `get_batches` (828) · `get_batch_by_name` (846) ·
`get_batch_completion_status` (913) · `mark_batch_completed` (956) ·
`list_callback_due` (983) · `mark_callback_attempt` (1005) ·
`reset_callback_for_retry` (1067)

### tasks.py（9 项）— TasksMixin · agent D
`create_tasks` (1095) · `pull_tasks` (1154) · `reclaim_dead_worker_tasks` (1246) ·
`auto_retry_failed_tasks` (1283) · `fail_task` (1331) · `release_tasks` (1383) ·
`prioritize_batch` (1413) · `get_progress` (1422) · `get_batch_failures` (2092)

> `get_batch_failures` 名字像 results，但 SQL 全在 tasks 表上，所以归 tasks.py。

### results_write.py（6 项）— ResultsWriteMixin · agent E
`_save_result_inner_unlocked` (1816) · `accept_success_result` (1655) ·
`accept_results_batch` (1702) · `accept_failed_result` (1811) ·
`save_result` (1987) · `save_results_batch` (2084)

### results_read.py（7 项）— ResultsReadMixin · agent F
`get_results` (2120) · `get_result_by_asin` (2272) · `get_asin_changes` (2282) ·
`iter_results` (2344，**async generator**) · `get_total_asins` (2464) ·
`get_all_asins` (2468) · `get_change_stats` (2476)

### media.py（10 项）— MediaMixin · agent G
`get_pending_screenshots` (2291) · `update_screenshot_status` (2298) ·
`get_screenshot_progress` (2333) · `_get_done_screenshot_path` (2008) ·
`_get_done_screenshot_paths` (2033) · `_hydrate_screenshot_paths` (2071) ·
`create_seller_batch` (1443) · `accept_seller_discovery_result` (1500) ·
`get_seller_batch_progress` (1616) · `expand_batch_variants` (851)

### 跨文件欠账（先声明，谁都不用等谁）
```
pool.py    欠 所有人  : _db / read() / _write_lock / _tx() / translate_sql / rowcount_from_tag / text_affinity
schema.py  欠 所有人  : 表和索引已就位，列序由 verify_schema 守住
media.py   欠 results_write : _get_done_screenshot_path(asin, batch_id) -> Optional[str]
media.py   欠 results_read  : _hydrate_screenshot_paths(items, batch_id) -> None（原地改）
tasks.py   欠 media         : create_tasks(...) -> int
tasks.py   欠 results_write : fail_task(...) -> {"accepted": bool, "stale": bool}
```
欠账方法**先写签名 + `raise NotImplementedError` 就能让别人跑起来**，不必等对方写完。

---

## 3. 决策台账（已定，不要重开）

| # | 决策 | 理由 |
|---|---|---|
| **D-1** | 遗留表保持 **TEXT 时间戳**（`%Y-%m-%d %H:%M:%S`）+ **INTEGER 布尔** 0/1 | app.py:1168 与 487 对 `created_at` 做 `strptime(x[:19], ...)`，datetime 对象不可下标；基线里 `/api/batches` 的 `needs_screenshot` 是 int `0`。**与 `.agent/pg_migration_plan.md:383-386` 直接冲突，以本条为准。** |
| **D-2** | `_write_lock` 保持**真锁**，`_db` 是**一条专用写连接** | 换成 TimedNoLock 就得给每个方法单独取连接，而 app.py 那 7 个 `async with db._write_lock: ... db._db.execute('BEGIN')` 块会立刻错乱。保持单写连接同时消掉了 PG 独有的一整类新故障：pull_tasks 双发、mark_callback_attempt 丢更新、accept_results_batch↔reclaim 死锁。读侧已经走池 —— "重读阻塞写"这个真正的痛点已经解决。写并发留到 Phase 1.5（前提：先把 app.py 的裸 SQL 抽干净）。 |
| **D-3** | `TimedLock` + `LOCK_STATS` **保留**，且从 `common.database` **共享同一个对象** | 基线 step 56 钉死了 `waits`/`holds` 的三个 caller key 与 `stage_timings` 的四个 stage key，而 `_summary` 对空样本返回形状不同的 `{"count": 0}`。**与 `.agent/pg_migration_plan.md:311-313` 的"删除"直接冲突，以本条为准。** 命名调用点（`_write_lock("pull_tasks")` / `("accept_results_batch")`）与五处 `record_stage()` 必须原地保留。 |
| **D-4** | app.py 用**垫片**兼容，不按后端分叉 | app.py 有 7 个事务、11 条裸 SQL、6 处 `db.read()`，全是 aiosqlite 协议。抽成方法要改 common/database.py（禁止），分叉 app.py 会破坏"SQLite 路径逐字节不变"。app.py 最终只改了 2 行（import + `create_database()`）。 |
| **D-5** | `LIKE` → `ascii_lower(x) LIKE ascii_lower(y)`，**不是 ILIKE** | 实测 39 探针 × 5 种写法：ILIKE 9 处不一致、ILIKE+ESCAPE 5 处、`ascii_lower` **0 处**。根因是非 ASCII：SQLite 只折 ASCII（`'CAFÉ CREME'` 不匹配 `'%café%'`），ILIKE 折全 Unicode 且依赖 collation。 |
| **D-6** | pgdb 内部 SQL 方言是 `?` 占位符，由 `translate_sql` 统一改写 | 顺带消掉 get_results 那个"`join_params + where_params` 与文本顺序不一致"的编号陷阱。同一条语句里混用 `?` 和 `$n` 会直接 raise。 |
| **D-7** | `statement_cache_size=0` | `_save_result_inner_unlocked` 每种非 None 字段组合一份 SQL 文本，且 asyncpg 会冻结首次推断的参数类型 OID。Phase 1 要确定性。Phase 2 视性能再开。 |
| **D-8** | `get_results` 的 COUNT 崩溃 **刻意复现**，不修 | 今天 `?search=<≥3字符>&cursor=<id>` 是 500（`"d.id" not in p` 的过滤把搜索谓词连同参数一起剔了），`search=Go&cursor=3` 是 200。黄金没覆盖这个组合。修法留给 Phase 1.5 连同 COUNT(*) 重构一起做。做法见 results_read.py 头注释。 |
| **D-9** | `DELETE FROM sqlite_sequence` 由**垫片整句替换**成 5 条 identity RESTART | app.py:2654 在裸 `db._db` 块里，垫片替换比改 app.py 干净。响应体仍是 `{"ok": true}`。已有用例覆盖。 |
| **D-10** | 每个 text 列都带 `COLLATE "C"`，建库也用 `LC_COLLATE=C` | 双保险。scraper_dev 恰好是 C.UTF-8 所以现在看不出问题，但生产库若用 en_US.UTF-8 建，`iter_results` 的 keyset（`ba.asin > $n ORDER BY ba.asin`）会重排导出行序。逐列 COLLATE 能扛住"恢复进另一台 collation 不同的库"。 |
| **D-11** | 黄金夹具在 `DB_BACKEND=postgres` 时**每次运行建一个新库** | 基线钉死了自增 id 的**烧号**（batch id 1/3、task id 1,3,7,8），复用脏库序列不从 1 开始，64 步无法重放。SQLite 路径完全没动（该分支有 `is_postgres()` 守卫）。 |
| **D-12** | NUL 字节默认**不剔除**（`PG_STRIP_NUL=1` 可开） | 剔除会改变落库数据，进而改变 `content_hash` / `title_bullets_hash` / `asin_changes`；不剔除则一条脏标题会让整批上传 500（SQLite 没有这个故障模式）。黄金定不了这件事，留给人拍板；做成开关，翻一行即可。 |
| **D-13** | `ConnProxy` 内置**语句级** `asyncio.Lock`（`_op_lock`），复刻 aiosqlite 的连接内排队 | D-2 让 `_db` 是**一条**连接。aiosqlite 把操作排进该连接的工作线程，所以并发使用合法；asyncpg 不排队，直接抛 `InterfaceError: another operation is in progress`。而仓库里确实有"不持 `_write_lock` 就碰 `_db`"的合法路径（`list_callback_due`、app.py:1298/2230/2281/2289/2294/2309），它们在 SQLite 下完全正常、按 equivalence-first **不能改**。黄金结构性看不到这件事（4 个后台协程被 no-op、TestClient 顺序执行），但**真实服务**必然撞上——实测 `_callback_dispatcher` 与写路径并发 100% 复现。锁只包**一条**语句、不包事务，所以"另一个协程的 SELECT 插进开着的事务中间"两个后端行为一致。回归由 `tests/pgdb/test_concurrency.py` 守。 |
| **D-14** | `run_startup_optimize` 回到 `_write_lock("optimize")` + `self._db`，与 SQLite 逐字一致 | 早先为绕开上面那个 `InterfaceError` 改走了读池且不拿锁，代价是活着的 PG 服务在 `/api/_debug/lock-stats` 里比 SQLite **少一个 `optimize` caller key**（实测差异）。D-13 修掉根因后这个绕行就没必要了。走读池反而有 SQLite 没有的风险：导出会长时间占用池连接，池满时 ANALYZE 会举着 `_write_lock` 干等，拖死所有写路径。<br>**注意** `maintenance_loop` 仍然没有 `checkpoint` 这个 caller key——那不是取舍，是 PG 里根本不存在对应操作，为了让指标"看起来一样"去空转一个锁等于伪造观测数据。 |

### 明确推迟到 Phase 1.5 / Phase 2 的（**别在 Phase 1 顺手做**）
- `get_results` 的 COUNT 崩溃（D-8）与 COUNT(*) 重构。
- 真正的写并发：抽走 app.py 的 24 处裸 SQL → 换 `TimedNoLock` → 每个方法自己取连接
  → `pull_tasks` 的 CTE + `SKIP LOCKED`、`_save_result_inner_unlocked` 的
  `pg_advisory_xact_lock(hashtext(asin))`、`accept_results_batch` 的 40P01 重试。
- `accept_results_batch` 失败分支缺失的 lease 谓词与 rowcount 检查
  （.agent/catalog_sync_audit.md:167）。
- app.py:1499 的 `datetime.now()`（本地时间写进 UTC 列）——**照抄，不修**。
- `app.py:1375` `GROUP BY error_type ORDER BY cnt DESC` 的并列不稳定。

---

## 4. 实现者必须遵守的硬规矩

1. **等价优先，改进其次。** 连 bug 一起移植。任何"顺手修好"都会让黄金校验的差异
   变成"要逐个解释"的噪声，而不是信号。有意的偏离必须写进本文件的决策台账。
2. **不准从 `common.database` 复制任何常量或函数。** 一律
   `from common.pgdb._shared import ...`。`test_shared_symbols_are_the_same_objects`
   会逐个断言 `is` 同一个对象。
3. **不准自己 `import asyncpg` 建连接。** 只能用 `self._db` / `self.read()` /
   `self._tx()`。需要 asyncpg 专有能力时用 `proxy.raw`。
4. **写路径必须在 `self._write_lock` 里**，并保留原有的 caller 名字符串。
5. **rowcount 必须是 int。** 走垫片就已经是了；绕开垫片就自己过
   `pool.rowcount_from_tag`。lease 门错一次 = 静默丢结果。
6. **`total_changes` 没有替代品。** 要"实际插入几行"就用单条
   `INSERT ... SELECT unnest(...) ON CONFLICT DO NOTHING` 读命令标签。
   `ConnProxy.total_changes` 会直接 raise，就是为了别让人误用。
   **绝不允许**为了少插几行而预过滤——自增烧号被基线钉死。
7. **绑到 TEXT 列的值一律过 `pool.text_affinity()`。** 裸 `str()` 是错的
   （`True`→`'True'` 而非 `'1'`）。黄金抓不到这一类。
8. **`ORDER BY` 里可空的排序键要显式写 NULL 位置**：DESC 补 `NULLS LAST`、
   ASC 补 `NULLS FIRST`（PG 默认与 SQLite 正好相反）。
9. **不准在 Phase 1 加 `REPEATABLE READ`。** 多语句读路径现在就是每条语句一个
   快照，基线是照着这个录的。
10. **每个 agent 自带 pytest。** 黄金 64 步覆盖不到的地方（seller 全套、截图全套、
    变体展开、回调机制、批量删除、`DELETE /api/database`、`get_batch_failures`）
    只能靠自己的用例兜。**不要**扩 `tests/golden/scenario.py`、**不要**重录基线——
    基线是不变式。

### 规格书里两处**已证伪**的说法（别照着"修 bug"）

移植规格书（以及本文件早先的措辞）有两处与实际行为不符。两处都已在 SQLite
原实现上实测确认，PG 侧照抄的是**真实行为**而不是规格书的描述：

| 说法 | 实测（SQLite，唯一真源） |
|---|---|
| `get_progress` 遇到未知 status 会 `KeyError` | **不会。** 那行是 `stats[row["status"]] = row["cnt"]`，赋值不是取值。未知 status 会往返回的 dict（= HTTP 响应体）里**多塞一个 key**，且不计入 `total`。为"保留 bug"去加 `raise` 才是真的引入回归。 |
| `get_batches` 对零任务批次返回 `completed/failed/pending/processing = NULL` | **返回 int `0`。** `LEFT JOIN` 产出一行全 NULL 的 `t.*`，`CASE` 走 `ELSE 0`，`SUM` over 单行 = 0。两个后端一致。 |

---

## 5. 验收

| 层级 | 命令 | 判据 |
|---|---|---|
| 导入 | `python -c "import common.pgdb"` | 无异常（公开面完整 + 无重复定义） |
| 骨架 | `pytest tests/pgdb -q` | 全绿 |
| 单域 | `pytest tests/pgdb/test_<domain>.py -q` | 各 agent 自己的门 |
| 回归 | `python -m tests.golden.run verify` | `✅ 64 步与基线完全一致` |
| **完工** | `DB_BACKEND=postgres python -m tests.golden.run verify` | `✅ 64 步与基线完全一致` |
