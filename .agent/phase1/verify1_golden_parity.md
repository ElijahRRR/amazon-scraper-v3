## VERDICT: the Postgres pass is REAL. The harness was not weakened.

### 1. Actual output of both runs

```
$ cd /home/user/amazon-scraper-v3 && .venv/bin/python -m tests.golden.run verify
...
22:03:35 [INFO] 服务器关闭
✅ 64 步与基线完全一致                                    EXIT=0

$ cd /home/user/amazon-scraper-v3 && DB_BACKEND=postgres .venv/bin/python -m tests.golden.run verify
...
22:03:46 [INFO] 服务器关闭
✅ 64 步与基线完全一致                                    EXIT=0
```

Repeatability: 4 PG verify runs, 2 SQLite verify runs, all green. `DB_BACKEND=postgres ... run selfcheck` → `✅ 两次独立运行完全一致（64 步），场景是确定性的`.

### 2. The harness was not made permissive

| Check | Result |
|---|---|
| `samples/sqlite_baseline.json` blob | `2f5e82b4…` identical at **b504585 (record) == HEAD == working tree**. Never re-recorded. |
| `scenario.py`, `run.py`, `test_golden.py` | zero commits since Phase 0 |
| pre-existing tests modified/deleted by the port | none; `tests/pgdb/*` is all new files. `pytest.ini`/conftest untouched |
| backend conditionals in `scenario.py` | **zero** (`grep DB_BACKEND\|is_postgres\|postgres` hits only harness isolation + a DSN path) |
| step-sequence guard | `diff_steps` compares the name list first and bails — silent drops impossible; both captures produced 64 steps |

`tests/golden/harness.py` is the only pre-existing test file the port touched, and the whole diff is isolation-only: a per-run scratch database (`_pg_scratch_db`), and patching `get_database_class()` instead of hardcoded `database.Database` so the 4 background loops actually get no-op'd on the selected backend. **No scrub relaxed, no expectation loosened, no step skipped.** The no-op patching is symmetric — same 4 loops + `start_maintenance`/`run_startup_optimize` on both backends — so PG gets no easier ride.

### 3. Mutation testing on the Postgres path (Phase 0 only ever mutated SQLite)

| Mutation in `common/pgdb/` | Result |
|---|---|
| M1 extra column in `SELECT d.*` (`results_read.py`) | ❌ **19 diffs** caught |
| M2 lease gate defeated in `accept_success_result` (`results_write.py:142`) | ❌ **45 diffs** caught — exactly the number Phase 0's README records for the same mutation on SQLite, and the bidirectional lease assertion fired (`submit_result_stale_lease.stale: True -> False`, `result_after_stale_reject status: 404 -> 200`) |
| M3 `ORDER BY d.id` → `d.asin` | ❌ **69 diffs** caught |

(M2's first attempt aborted — my anchor matched 4 sites; I redid it line-targeted.) Working tree verified clean afterwards, still at `5957860`.

### 4. The PG run genuinely drives Postgres

`server.app.db class = common.pgdb.Database`, `current_database = scraper_golden_7093_1277d280`, `server_version = 16.13`, with rows readable over an **independent** asyncpg connection (batches 1, tasks 3, asin_data 4, asin_changes 3). `common/dbfactory.py` has **no fallback** — an unknown `DB_BACKEND` raises, and the postgres branch hard-imports `common.pgdb`.

### 5. Two harness blind spots I found — pre-existing, not introduced by the port, but they mean "64/64" alone does NOT prove the D-1 type decision

- **bool vs int is invisible.** `harness._diff_body:385` exempts any `(int,float)` pair from the type check, and `bool` subclasses `int`, so `1` vs `true` and `0` vs `false` compare equal. Measured: `diff_steps({'needs_screenshot':1}, {'needs_screenshot':True})` → `[]`.
- **TEXT vs timestamptz is invisible.** `_TS_RE` deliberately matches both `' '` and `'T'` separators plus `Z`/offsets → both rewrite to `<TS>`; every timestamp key is additionally in `_VOLATILE_KEYS` → `<VOLATILE>`. A timestamptz would also still be a `str`, so a type-strict differ wouldn't catch it either.

**I closed both holes independently, and D-1 holds:**

1. Type-strict re-diff (bool≠int) of a full 64-step capture vs the baseline: `STRICT-DIFF CLEAN` on **both** backends. Non-vacuous — the baseline carries 5 int-valued `needs_screenshot` leaves that would have flipped.
2. Raw wire formats with scrubbing disabled: both backends emit exactly **one** timestamp shape, `9999-99-99 99:99:99`, **124 occurrences each**, over identical key sets (`created_at`, `updated_at`, `crawl_time`, `baseline_updated_at`, CSV/xlsx cells). No `T`, no offset, no fractional seconds.
3. Physical schema introspection of a freshly-`connect()`-ed PG database: all 15 timestamp columns `text`, all 3 boolean-ish columns `integer`, and **0** `timestamptz`/`timestamp`/`boolean` columns anywhere across the 7 tables.

These blind spots are worth recording for the later phases — golden will *not* protect the type contract if someone revisits it.

### 6. Consistent with the integration report

The report's `checkpoint`-in-lock-stats caveat is genuinely outside golden's reach: the harness no-ops all four background loops plus `start_maintenance` on both backends, so `maintenance_loop` never runs. That's a Phase 0 design choice, not a port-time relaxation. No leaked scratch databases; `scraper_dev` intact.

Scripts: `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/{strictdiff.py,capture_dump.py,raw_shapes.py,schema_types.py,vp1_prove_pg.py}`