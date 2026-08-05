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
| **Phase 2 —— 事件流（PG 独有，SQLite 上零字节）** | | |
| `common/slowhash.py` | ✅ 已完成（§4 哈希规格，纯 stdlib，零 `common.*` 依赖） | agent hash |
| `common/pgdb/relay.py` | ✅ 已完成（`EventStreamMixin`：`_emit_outbox` / 引导 / relay / 分区 / 指标） | agent schema-relay |
| `common/pgdb/outbox.py` | ✅ 已完成（写钩子侧胶水：payload 定形、`EventContext`、stale 独立事务） | agent write-hooks |
| `common/pgdb/schema.py` 的事件流 DDL 段 | ✅ 已完成（追加在 `SchemaMixin` 之前；遗留 DDL / `EXPECTED_COLUMNS` / `verify_schema` 一个字没动） | agent schema-relay |
| `tests/pgdb/test_relay.py` / `test_write_hooks.py` / `test_event_stream_wiring.py` | ✅ 74 条 | 各自 + 收口 |
| `tests/test_slowhash.py` / `test_event_stream_endpoint.py` / `test_golden_with_relay.py` | ✅ 87 条（放 `tests/` 根，不进 `tests/pgdb/`——那里的 conftest 会 `importorskip("asyncpg")`，而这几条必须在无 PG 的机器上也跑） | 各自 + 收口 |
| `tests/conftest.py` | ✅ 新增（全树事件循环修复，见 D-27） | 收口 |

**Phase 1 状态：完工。** 两个后端各自 `64 步与基线完全一致`，
`pytest tests/ -q` 全绿。收口阶段做的事见 §3 的 D-13 / D-14。

**Phase 2 状态：完工。** 两个后端各自 `64 步与基线完全一致`；
`pytest tests/ -q` = 427 passed / 6 skipped，`DB_BACKEND=postgres` 下 429 / 4。
事件流的公开方法**一个都不在 `PUBLIC_API` 里**——那个元组是与 SQLite 的对等面契约，
事件流是 PG 独有的增量。两道导入期自检对此安全：`_assert_api_complete` 只查"少了没有"，
`_assert_single_owner` 只遍历已在 `PUBLIC_API` 里的名字。收口阶段做的事见 D-25..D-27。

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
| **D-13** | `ConnProxy` 内置**语句级** `asyncio.Lock`（`_op_lock`），复刻 aiosqlite 的连接内排队 | D-2 让 `_db` 是**一条**连接。aiosqlite 把操作排进该连接的工作线程，所以并发使用合法；asyncpg 不排队，直接抛 `InterfaceError: another operation is in progress`。而仓库里确实有"不持 `_write_lock` 就碰 `_db`"的合法路径（`list_callback_due`、app.py:1298/2230/2281/2289/2294/2309），它们在 SQLite 下完全正常、按 equivalence-first **不能改**。黄金结构性看不到这件事（4 个后台协程被 no-op、TestClient 顺序执行），但**真实服务**必然撞上——实测 `_callback_dispatcher` 与写路径并发 100% 复现。锁只包**一条**语句、不包事务。<br>~~所以"另一个协程的 SELECT 插进开着的事务中间"两个后端行为一致。~~ **这半句已被 D-15 推翻**：那个"一致"意味着别人的 SELECT 一报错就把本事务 abort 掉，实测能毁掉一整批已接收的结果。现在这类语句按事务归属改道读池，详见 D-15。回归由 `tests/pgdb/test_concurrency.py` 守。 |
| **D-14** | `run_startup_optimize` 回到 `_write_lock("optimize")` + `self._db`，与 SQLite 逐字一致 | 早先为绕开上面那个 `InterfaceError` 改走了读池且不拿锁，代价是活着的 PG 服务在 `/api/_debug/lock-stats` 里比 SQLite **少一个 `optimize` caller key**（实测差异）。D-13 修掉根因后这个绕行就没必要了。走读池反而有 SQLite 没有的风险：导出会长时间占用池连接，池满时 ANALYZE 会举着 `_write_lock` 干等，拖死所有写路径。<br>**注意** `maintenance_loop` 仍然没有 `checkpoint` 这个 caller key——那不是取舍，是 PG 里根本不存在对应操作，为了让指标"看起来一样"去空转一个锁等于伪造观测数据。 |
| **D-15** | 写连接上的事务是**有主的**（`ConnProxy._tx_owner`）：<br>(a) **非持有者**发的普通只读语句改道读池；<br>(b) 释放 `_write_lock` 时若事务还开着，直接回滚（`WriteLock._do_exit`），`BEGIN` 处再补一道兜底 | **这条推翻 D-13 末尾"两个后端行为一致"那句话**——那个"一致"是拿数据安全换来的。`_op_lock` 只串行化**语句**、不串行化**事务**，于是不持锁的读会执行在别人开着的事务**内部**。SQLite 下无害；PG 下那条读一报错，别人的事务立刻 abort。实测：一个只读的 `DELETE /api/results` 预览（`search` 里带 NUL）把一个 worker **6 条结果的整批提交全毁掉**，任务卡死在 `processing`；反向则是写方的错误以 `InFailedSQLTransactionError` 泄漏给 `_callback_dispatcher`。`PG_STRIP_NUL` 覆盖不到（app.py 自己拼参数，不过 `text_affinity`），客户端断开触发的 `57014` 同样能触发。<br>改道**只**发生在"另一个 asyncio.Task 持有事务"时——顺序调用方（黄金夹具、TestClient）永远碰不到。**加锁读（`FOR UPDATE`/`FOR SHARE`）与持有者自己的读不改道**：`pull_tasks` 的认领、`mark_callback_attempt` 的租约、10 处在途读全靠它们留在事务里（`_is_plain_read`）。判定在 `_op_lock` 内做、借池连接前先放锁，不形成环。<br>唯一的行为改变：跨 Task 的**脏读**变成**已提交读**。没有调用方能依赖旧值——同一个调用早/晚一个调度 tick 就返回已提交结果。而且旧行为本身是 bug：实测 `DELETE /api/results {"batch_id": B}` 在别人未提交的事务中间解析出空目标集，**在 SQLite 上照样删掉了另一个批次已提交的结果**。SQLite 路径原样未动。<br>(b) 的位置选在**锁释放**而不是每个调用点：本仓库"在 `_db` 上开事务"的前置条件只有"持有 `_write_lock`"，所以"锁释放了而事务还开着"= 零误判的"被遗弃"信号。它一次覆盖 D-17 补不到的地方（`tasks.py`/`media.py`/`results_write.py` 那 10 处 `except Exception` 的取消泄漏）以及以后任何人新写的漏回滚 `BEGIN` 块。必须**真回滚**而不是只清标志位：`release_tasks` 那个 `DataError` 是 asyncpg **客户端侧**抛的，`BEGIN` 已经上了服务端，留下一个握着锁、钉着 xmin horizon 的 idle-in-transaction。回归由 `tests/pgdb/test_concurrency.py` 的 4 个用例守（黄金**结构上**盖不到：4 个后台协程被 no-op、TestClient 严格顺序，"外人的事务"这个前置状态根本不存在）。 |
| **D-16** | 每一处 `LIKE` 都带 `ESCAPE ''`（`_LIKE_QMARK_RE` 的改写产物 + `results_read._TERM_OR` 显式拼），`_like_pattern` **不再**加倍反斜杠 | D-5 只对齐了**大小写折叠**，没对齐**转义**。SQLite 的 `LIKE` 没有转义字符，PG 默认拿反斜杠当转义——于是模式里的反斜杠会静默改变命中的行集。实测 `DELETE /api/results {"search": "back\\slash"}`：sqlite 删掉 `back\slash` 那行（对），pg 删掉 `backslash` 那行（错），**两边都回 `{"deleted":1}`**，调用方察觉不到。`{"search":"\\"}` → sqlite 删 2 行 / pg 删 0 行。<br>旧做法（在 Python 侧加倍反斜杠）只够得着 pgdb 自己拼的 SQL，够不着 `app.py:2277` 那条 f-string 拼的 DELETE（D-4 禁止分叉 app.py），于是**读路径和删除路径互相不一致**——同一个 `search`，GET 命中一行、DELETE 删另一行。`ESCAPE ''` 加在 SQL 侧，两条路径共用同一份语义，结构上不可能再漂。顺带修掉"模式以孤立反斜杠结尾"在默认转义下直接 `InvalidEscapeSequenceError` 崩溃。<br>实测 `ESCAPE ''` 对计划**完全中性**（`EXPLAIN` 与不带时逐字相同，pg_trgm GIN 照常命中），因为 PG 把 `like_escape($1,'')` 在计划期折成了同一个 `~~`。 |
| **D-20** | `text_affinity` 对 SQLite 会**拒收**的值一律抛异常（越界 int → `OverflowError`，list/dict/tuple/set/bytes → `sqlite3.ProgrammingError`），并复刻 SQLite 对 `-0.0`/`NaN`/`±Inf` 的落库结果 | 等价性是**双向**的：原来 `str(v)` 兜底 + 不做 int 范围检查，让 SQLite **拒收**的载荷在 PG 下被静默收下。`POST /api/tasks/result/batch` 六条里掺一条毒项：sqlite **500 + 整批回滚**（`/api/progress` done=0），postgres **200 `{"accepted":6}`**（done=6）——**批次原子性的语义被反转**，而且库里留下 `"['a', 'b']"` 这种字符串。异常类型直接复用 `sqlite3` 的，两个后端的失败面完全一致（残留差异只有 sqlite 消息里多一个参数序号，`text_affinity` 按值调用拿不到那个下标）。<br>浮点那三个值**到得了**：`json.loads` 接受 `NaN`/`Infinity`/`-Infinity` 字面量，`-0.0` 和 `1e400` 就是普通 JSON 数字。实测 sqlite 存 `'0.0'`/NULL/`'Inf'`/`'-Inf'`，pg 原来存 `'-0.0'`/`'nan'`/`'inf'`。pool.py:89 那条"JSON 到不了，记录备查"的注释是**错的**，已改。 |

| **D-17** | `server/app.py` 的 7 个裸 `BEGIN` 块 + `common/pgdb/tasks.py` 的 3 个，一律补 `except BaseException: ROLLBACK; raise` | 原版没有回滚路径（照抄自 `common/database.py`）。SQLite 下这些语句基本不会失败；PG 下失败是常态，而事务是**粘在连接上**的状态：一次失败 → 写锁随异常释放、垫片事务槽不清 → 之后每一次 `BEGIN` 都撞"嵌套 BEGIN" → **整条写路径永久焊死，读却还是 200**（健康检查全绿）。实测 `POST /api/tasks/release {"task_ids":["1"]}` 这一个请求就够。补的代码**只在错误路径上跑**，成功路径一条语句都没多，两个后端的返回值/异常类型都没变（黄金 64 步逐字不动）。<br>捕 `BaseException` 而不是 `Exception`：这些块全在 HTTP handler 里，客户端断开会让 Starlette 取消请求协程，`CancelledError` 同样必须回滚（`batches.py` 的 `_tx()` 本来就是这个口径）。SQLite 侧同样会 wedge（`cannot start a transaction within a transaction`），所以这是**双向改进**而不是偏离。<br>⚠ 尚未统一：`tasks.py` 194/289/374/436、`media.py` 219/344/540、`results_write.py` 168/289/497 仍是 `except Exception`，取消场景照旧泄漏。归 Phase 1.5。 |
| **D-18** | 三条不确定的 `ORDER BY` 补全序 tiebreaker（`app.py:341` / `1378` / `1385`） | 这三条的排序键都不是全序，而 `updated_at` 是**秒级**精度且 `accept_results_batch` 给整次提交盖同一个时间戳——于是 `LIMIT` 返回的是不确定的**行集合**，不只是顺序。实测：260 个任务一起失败，`/api/batches/{name}/errors` 的 200 行里 **60 行**两个后端不同；`app.py:341` 的 30 行里 10 行不同；`app.py:1378` 的并列组顺序完全不同。<br>写法对齐本来就正确的 `get_batch_failures`：`ORDER BY updated_at DESC NULLS LAST, id DESC`；`error_summary` 用 `ORDER BY cnt DESC, error_type NULLS FIRST`。`NULLS LAST`(DESC) / `NULLS FIRST`(ASC) 就是 SQLite 的默认（已实测），写出来只为让 PG 对齐，**SQLite 侧是 no-op**；文本序两边都是字节序（PG 靠 D-10 的 `COLLATE "C"`）。<br>**这是一处有意的 SQLite 行为改变**：并列时 SQLite 原来的顺序是任意的，现在被钉死。黄金基线不受影响（`errors_batch_a` 那一步两个数组都是空的），实测两个后端 64/64 不变。 |
| **D-19** | `get_pending_screenshots` 补 `ORDER BY id`；`create_batch` 补 `except IntegrityConstraintViolationError: pass` | 前者是 `LIMIT` 没有 `ORDER BY`：SQLite 全表扫走 rowid（`screenshots.id` 就是 rowid），PG 走堆序，截图 `done→pending` 重试一次堆序就漂——实测 20 行 churn 8 行、`limit=5` → sqlite `[S001..S005]` / pg `[S009..S013]`。补的是"SQLite 今天实际产出的那个序"。<br>后者：`ON CONFLICT DO NOTHING` 只吞**唯一/排他**冲突，`INSERT OR IGNORE` 吞**所有**约束冲突，于是 `create_batch(None)` 从 sqlite 的返回 `0` 变成 pg 抛 `NotNullViolationError`。异常必须逃出 `_tx()` 之后才吞（PG 事务一 abort，同事务内再发语句就是 25P02）。identity **照样烧号**，两个后端实测一致。今天 app.py 到不了 `name=None`，属于防御。 |

### Phase 2 决策（事件流）

| # | 决策 | 理由 |
|---|---|---|
| **D-21** | `UNIQUE(source_id)` 建在**分区**上，不建在父表上；relay 用**无目标** `ON CONFLICT DO NOTHING` | 计划 §2.1 第 278 行照抄会被 PG 16 直接拒绝：`FeatureNotSupportedError: unique constraint on partitioned table must include all partitioning columns`。连带：`ON CONFLICT (source_id)` 推断不出约束、抛 `InvalidColumnReferenceError`，只能用无目标形式（唯一的另一条唯一索引是 `seq` 主键，`bigserial` 撞不上）。**代价**：跨分区的重复 `source_id` 抓不到——可接受，relay 的认领→落库是单事务，一行不可能被处理两次；这道索引防的是「第二个 relay」，由单例锁挡住。**不要**为此加全局去重表，那正是分区要消掉的不可裁剪热点。 |
| **D-22** | `gen` **复用**，只有全新库 / 检出回退才新铸 | 计划 §2.1 说「每次启动新铸」，但 §5.5 把 `gen` 变化定义为消费侧**硬停 + 全量对账**——每次启动新铸等于每次例行部署都触发一次全量对账。按 T11 的读法：`max(seq) < max_seq_ever` 才算回退，那时新铸并把序列 `setval` 推过历史高水位（必要时先建分区）。`gen` 仍然**逐行落库**：从快照恢复不得把历史重贴成新标签。`instance_id` 由 `SCRAPER_INSTANCE_ID` 配置，**永不自动铸造**（T12：它是人用来区分两个克隆的）。 |
| **D-23** | 租约 `UPDATE` **绝不加 `RETURNING`**；要服务端事实就在同一事务里另发一条普通 `SELECT` | 实测：`普通 UPDATE 租约不匹配 -> rowcount=0，门触发 True`；`同一条 + RETURNING -> rowcount=-1，门触发 False`。`ConnProxy._run_unlocked` 见到 RETURNING 就走 `returns_rows` 分支返回 `Cursor(rows, -1)`，而 `-1 == 0` 是 False —— **租约门会放行每一条过期结果**，这是全系统安全性最高的一条谓词。`results_write.py:140/:219` 与 `tasks.py` 的那条 `FOR UPDATE` 只**加列**、不加 RETURNING（`row[0]` 仍是 `retry_count`）。 |
| **D-24** | 新增死信表 `scraper.scrape_outbox_dead`（**计划外**） | 没有它，一行畸形到过不了 `NOT NULL`/`CHECK` 的 body 会永远卡在 `ORDER BY id` 队头，**整条流停摆**。策略是保守的：分区溢出（`23514` + "no partition"）与连接故障**绝不**隔离——那两类要保留计划要的「响亮停摆、零丢失」；只有批量已缩到 1 且连续失败 `RELAY_QUARANTINE_AFTER` 次，才把队头搬走，body 逐字节保留 + ERROR 日志 + 计数器。 |
| **D-25** | 事件流 DDL 由 `SchemaMixin.init_tables()` 在 `connect()` 期建，**不**由 relay 启动时建 | 被两个约束夹死：黄金必须 no-op 掉 relay 循环（否则录制期后台任务改状态、样本不可重复），但**写钩子在 PG 黄金重放里照常触发**——建表若只在 relay 启动路径上，写钩子会在事务里撞上缺表。`verify_schema` / `EXPECTED_COLUMNS` 一个字没动：它们只比 `table_schema='public'`，`scraper.*` 对它们不可见，也必须保持不可见（那道门是防 `SELECT d.*` 把新列泄进 erpAPI 响应的）。 |
| **D-26** | `start_event_relay` 在三个 `await` 点上重查 `stopping` / `start_epoch`；`stop_event_relay` 第一句就置 `stopping = True` | **实测泄漏**，不是防御性编程。`lifespan` 是 `create_task(relay)` → `yield` → `await stop()`，进程刚起来就关时（配置错误、健康检查失败、同进程内起停一次服务器）stop 跑到的时候 relay 还卡在 `asyncpg.connect()` 里，而 `relay_task` / `relay_conn` 要到 start 最后才被赋值 —— stop **什么都看不见、空转返回**，`db.close()` 之后 relay 才起来：<br>`关服之后：relay_state='running'  库上残留会话=1  残留 advisory lock=1`<br>`第二个实例 start_event_relay() -> False  state='refused'`<br>一条泄漏的连接 + 一把没人放的单例锁，而且同进程内再起的服务器被永久拒之门外、事件流静默不工作。修完 0 残留、第二个实例正常启动。回归由 `tests/pgdb/test_event_stream_wiring.py` 守（撤掉 `stopping` 标志即失败）。<br>连带：`relay_state` 增加 `starting` / `failed`，把「还没起来」「已经死了」「干净停了」分开——观测端点存在的唯一理由就是让停摆变响，三者同名等于没有观测。 |
| **D-27** | 新增 `tests/conftest.py`：每个用例跑完把「当前事件循环」补回去 | `tests/test_session_slot.py:147` 用的是已废弃的 `asyncio.get_event_loop().run_until_complete(...)`，而 `asyncio.run(...)`（黄金夹具建/删临时库、事件流抽干）与 pytest-asyncio 都会把当前循环置空。原本 `tests/pgdb/conftest.py` 有一份同样的修复，靠的是 pgdb 用例**恰好排在** `test_session_slot` 前面；Phase 2 新增的两个 `tests/` 根文件按字母序落在中间，那份修复就够不着了——实测 26 个用例转红。提到全树级别，新测试文件不必各自记得。`tests/pgdb/conftest.py` 那份保留原样（带着自己的来龙去脉，重复无代价）。正解是把 `test_session_slot.py` 从 `get_event_loop()` 迁走，31 个用例的改动，与 Phase 2 无关，不在这里顺手做。 |

### Phase 2 验证后的修复决策（B1-B8，落点 `relay.py` / `schema.py`）

> 来源：`.agent/phase2/verify*.md` 的四份验证报告 + 修复者的实测报告。
> 每一条都有反事实（在 `fea7395` 上重跑同一探针）。

| # | 决策 | 理由 |
|---|---|---|
| **D-28** | 新分区 `LIKE` **父表** + **显式**建 source_id 唯一索引；并在 `ensure_event_partitions` 里做一次自愈式 `_repair_foreign_range_checks()` | `LIKE <上一个分区> INCLUDING ALL` 会连**模板自己的 range CHECK** 一起抄过来：p0 没有 range CHECK 所以 p1 干净，p1→p2 抄来 `scrape_events_p1_range`，此后每个分区都带一条**自相矛盾**的边界，一行也写不进去。生产默认 SPAN 下 `connect()` 一跑完就复现。<br>验证报告建议的 `EXCLUDING CONSTRAINTS` **不成立**：它同时丢掉父表的 `marketplace` CHECK，而 `ATTACH PARTITION` 要求子表**已经带着**它 —— `DatatypeMismatchError: child table is missing constraint "scrape_events_marketplace_check"`。所以改成模板父表 + 显式补索引，与 p0 的配方逐字一致，分区局部的垃圾在结构上不可继承。<br>光改 DDL 不够：p1/p2 是在 `connect()` 期建的，**任何被 Phase 2 代码碰过的库都已经带着一个中毒的 p2**，所以要一次目录查询驱动的自愈（干净时零 DDL）。 |
| **D-29** | `run_event_relay()` 是**重试循环**（`RELAY_RETRY_SECONDS`，默认 5s）；`refused` 是**瞬态**，不是终态 | 滚动重启会留下**一个 relay 都不剩**：新实例起来时旧的还握着单例锁 → `refused` → 任务直接结束；旧实例随后退出、锁释放，而新实例已经不会再试了。实测 `t3: NEW.state=refused holders=0 {'outbox': 6, 'events': 0}` —— 事件流静默停摆。改成重试循环后 `t3: NEW.state=running holders=1 {'outbox': 0, 'events': 6}`。 |
| **D-30** | relay 连续 `RELAY_RECOVER_AFTER` 次 tick 失败后**自己重连**并重新抢单例锁；抢不到就 `refused` 并退出；重连不上就 `failed` | 原来的 `failed` 只覆盖「主循环异常逃出」，不覆盖「每个 tick 都抛但循环还在转」。`pg_terminate_backend` 之后实测：行不丢（都在 outbox 里等着），但 relay 每秒吐一个 `InterfaceError`，而 `relay_state` 一直报 `running` —— 单实例部署上事件流永久停摆，偏偏那个为了让停摆变响而设的字段报的是健康。`consec_tick_fail` 一并进 `event_relay_metrics()`，让先兆在状态翻转之前就可见。 |
| **D-31** | 回退判据从 `max(seq)` 换成**序列水位**（`_seq_high_water()`，认 `is_called`） | `COALESCE(max(seq),0) < max_seq_ever` 有一条 Phase 6 一定会踩的假阳性：保留期一旦把 `scrape_events` 清空，一次**普通重启**就铸新 gen，消费侧按契约 §5.5 硬停 + 全量对账。bigserial 不随分区 DROP 回退，所以「水位 < ever」才真正等价于「seq 会被重发」。实测 A（retention DELETE）gen 不变、B（`setval(1,false)`）照常铸新 gen。 |

### Phase 3 决策（同步 API，落点 `server/api/sync.py`）

| # | 决策 | 理由 |
|---|---|---|
| **D-32** | 四个端点在**两个后端上都挂**，SQLite 下回 **503 + `error: event_stream_unavailable`**，而不是「不挂路由」 | 计划允许二选一。选 503 有三个理由：(a) 路由表在 `import server.app` 时定型，而 `DB_BACKEND` 是运行期变量 —— 条件挂载会让「路由存不存在」变成 import 时序的函数，而本仓库的测试与黄金夹具都在 import 之后改后端；(b) 不挂 = 404，而 404 正是契约里最危险的码：消费者把它读成「暂无数据」，游标永不推进、静默停摆；(c) 与既有的 `/api/_debug/event-stream` 口径一致（SQLite 上如实回 `enabled: false`，运维拿同一个 URL 探两种部署）。黄金不受影响：64 步不碰 `/api/v1/*`，且 `include_in_schema=False` 让 `/openapi.json` 逐字节不变（`test_openapi_json_does_not_change` 带反向哨兵钉住）。 |
| **D-33** | 空流 / 被裁空的流上，`min_available_seq` = `max_seq + 1`、`max_seq` = `max(max(seq), max_seq_ever)`（`sync._window()`），**不是** `COALESCE(..., 0)` | 计划只给了非空表的语义。照直取 0 有两个静默故障：(a) `min_available_seq = 0` 让 `after_seq + 1 < min` **永假** —— 一个数据被裁光的消费者拿到 200 空，于是永远等下去，两侧都不告警；(b) `max_seq = 0` 会触发消费侧自己的 `max_seq < stored_max_seq_ever` **硬停**（§5.5 第 2 行），一次正常的保留期裁剪就让全网全量对账。表非空时两式与朴素写法**逐字相等**（relay 每批 bump `max_seq_ever`，保留期只从底部裁），所以这不改变常态语义。反事实：把 `_window` 换回 `COALESCE(...,0)`，4 个用例转红。 |
| **D-34** | `MIN`/`MAX`/页查询在**一个** `REPEATABLE READ READ ONLY` 事务里，页查询后**再复核一次下界并取较大者** | 计划 §5.1 明确要求前半句。后半句在 RR 下是零成本断言 —— 它守的是「有人把隔离级别降下来」这类未来改动。反事实实测（把 `_snapshot` 默认改成 `read_committed`）：`ERROR 同步快照不稳定：页查询前后的 min_available_seq 不同（10 -> 12）…按保守方向返回 409`，也就是复核确实是最后一道网。方向永远选**多一个 409**：多一次全量对账 vs. 静默丢一段数据。 |
| **D-35** | `/ack` 走**池连接**（不是 D-2 的写连接），且事务显式降到 **READ COMMITTED** | 走写锁的话，这个由**远端消费者**触发的端点就成了采集入库路径上一个外部可触发的排队面。`sync_meta` 本来就在写锁之外被写（relay 用自己的连接 bump `max_seq_ever`），所以这不是新先例。降到 READ COMMITTED 是实测逼出来的：RR 下两个并发 ack 打同一行直接 `SerializationError: could not serialize access due to concurrent update`（= 500）。单调性由**一条** `ON CONFLICT DO UPDATE SET v = GREATEST(...)` 保证，READ COMMITTED 下 ON CONFLICT 会重读被更新的行再算 GREATEST，语义正确且并发安全（`test_ack_is_monotonic_under_concurrency`）。 |
| **D-36** | `/ack` 对 `ack_seq > max_seq` 回 **409 `ack_ahead_of_stream`**（计划外新增） | 计划只写了 `gen` 不符回 409。但 Phase 6 的保留期下界是 `max(磁盘下界, min(时间下界, ack_seq))` —— 收下一个本实例从没发出过的 `ack_seq`，等于**授权裁掉消费者其实没拿到的数据**。这是数据丢失向量，必须响。 |
| **D-37** | `completeness_ok` 由**服务端**算好放进每条记录；位 3（值 8）= MEASURED，`completeness_ok := (c & 8) != 0 AND (c & 7) == 7` | Phase 2 收口时把「契约必须显式规定消费侧拿 `completeness = 0` 怎么办」列为 Phase 3 的必答题。放服务端算是因为 §4.3 的合取式有一个反直觉点（`7` 也是 false，因为没标 MEASURED），让每个消费者各拼一遍就是等着有人拼错。**连带的契约条款**：Phase 4 之前 `completeness` 恒为 0，所以按算法字面执行**没有一行会进 `catalog.products`** —— 这一条在 `docs/sync_contract.md` §6.4 用整段写明，包括「不要把 `0` 当 ok 写死进代码」。 |
| **D-38** | `next_after_seq` **只推进到真正投递过的那一条**；空页不推进。`outcomes` 因此不适合放进拉取循环，契约里明确禁止 | 另一个选项是空页时把游标推到流头 —— 那样带 `outcomes` 的循环不会原地打转，但会**跳过**被过滤掉的行。宁可重扫，不可跳过：这是唯一会丢数据的方向。`has_more` 用 `LIMIT n+1` 精确判定（而不是 `next_after_seq < max_seq`），否则带过滤时会谎报「还有」。 |

> **D-27 已作废。** 那份 `tests/conftest.py` 的 autouse 夹具在 B6 修复中被删除：
> `conftest.py` 只有 pytest 读，`unittest discover`（本仓库最早的 runner）不读，
> 于是同一个缺陷 pytest 绿、unittest 红 26 个。根因已迁走
> （`tests/test_session_slot.py` 的 `run()` 自持事件循环），
> 约束由 `tests/test_runner_parity.py` 看守，它是 unittest 用例，两个 runner 都跑。

### 明确推迟到 Phase 1.5 / Phase 2 的（**别在 Phase 1 顺手做**）
- `get_results` 的 COUNT 崩溃（D-8）与 COUNT(*) 重构。
- 真正的写并发：抽走 app.py 的 24 处裸 SQL → 换 `TimedNoLock` → 每个方法自己取连接
  → `pull_tasks` 的 CTE + `SKIP LOCKED`、`_save_result_inner_unlocked` 的
  `pg_advisory_xact_lock(hashtext(asin))`、`accept_results_batch` 的 40P01 重试。
- `accept_results_batch` 失败分支缺失的 lease 谓词与 rowcount 检查
  （.agent/catalog_sync_audit.md:167）。
- app.py:1499 的 `datetime.now()`（本地时间写进 UTC 列）——**照抄，不修**。
- ~~`app.py:1375` `GROUP BY error_type ORDER BY cnt DESC` 的并列不稳定。~~
  → 已在 D-18 修掉：它返回的不只是顺序不定，是**行集合**不定。
- `except Exception` → `except BaseException` 的统一（D-17 末尾那一段）。

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
