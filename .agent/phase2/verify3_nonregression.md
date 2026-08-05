# VERDICT: no cheating. One real non-regression failure: `unittest discover` is broken on Postgres.

The golden fixture was not touched, the SQLite path is a *structural* no-op (not a guarded branch), no test was weakened or deleted, and the harness has exactly the same teeth as it had at `e21e2c6`. One runner regressed, and the integration report's claim that the 26 event-loop failures were "lifted to `tests/conftest.py`" is true only under pytest.

---

## 1. The four suites, real output

| Suite | SQLite | Postgres |
|---|---|---|
| `tests.golden.run verify` | `✅ 64 步与基线完全一致` EXIT=0 | `✅ 64 步与基线完全一致` EXIT=0 |
| `tests.golden.run selfcheck` | `✅ 两次独立运行完全一致（64 步）` EXIT=0 | `✅ 两次独立运行完全一致（64 步）` EXIT=0 |
| `pytest tests/ -q` | `427 passed, 6 skipped` EXIT=0 | `429 passed, 4 skipped` EXIT=0 |
| `unittest discover -s tests` | `Ran 51 tests … OK (skipped=6)` EXIT=0 | **`Ran 51 tests … FAILED (errors=26, skipped=4)` EXIT=1** |

Both `verify` runs re-run clean at the very end, after all mutations were restored.

pytest baseline math: base `e21e2c6` collected **272** node ids, HEAD collects **433** — `lost=0, added=161`, identical on both backends. 272 = the pinned 268 passed + 4 skipped. **Zero pre-existing tests were deleted or renamed away.**

The 6 SQLite skips are the 4 pre-existing `selectolax 未安装` plus 2 new PG-gates, and both gates provably *run* under Postgres (PG skip set is exactly the 4 selectolax ones).

## 2. The regression — `unittest discover` + `DB_BACKEND=postgres`

All 26 errors are `RuntimeError: There is no current event loop in thread 'MainThread'`, all in `tests/test_session_slot.py`. It is new:

```
base e21e2c6, postgres:  Ran 47 tests … OK (skipped=4)
HEAD 69886c6, postgres:  Ran 51 tests … FAILED (errors=26, skipped=4)
```

Root cause, isolated — each new root-level file breaks it *independently*:

```
A) session_slot ALONE, postgres:                       Ran 31 tests … OK
B) test_event_stream_endpoint THEN session_slot:       Ran 34 tests … FAILED (errors=26)
C) test_golden_with_relay     THEN session_slot:       Ran 32 tests … FAILED (errors=26)
```

Both new files are `unittest.TestCase` subclasses, so `unittest` collects them; under PG they run, call `asyncio.run(...)` (via `_pg_scratch_db` / the drain), which closes and unsets the loop; they sort alphabetically before `test_session_slot`, whose 31 cases use the deprecated `get_event_loop()`. Under SQLite they `skipTest`, so no `asyncio.run` — which is why only the PG runner turns red.

The fix at `/home/user/amazon-scraper-v3/tests/conftest.py` is a **pytest autouse fixture**; `unittest` never loads `conftest.py`. This is a test-infrastructure regression, not a product defect — nothing in `common/` or `server/` is implicated.

**Proven remedy** (applied, measured, reverted; tree left clean): a `tearDown` doing the same loop repair on the two new `TestCase` classes is runner-agnostic and fixes it without touching D-27's deferred `test_session_slot` rewrite —

```
unittest discover, postgres, WITH tearDown:  Ran 51 tests … OK (skipped=4)
unittest discover, sqlite,   WITH tearDown:  Ran 51 tests … OK (skipped=6)
pytest (both new files + session_slot), pg:  35 passed
```

## 3. Cheat audit — every item clean

**Baseline never re-recorded.** `tests/golden/samples/sqlite_baseline.json` blob is `2f5e82b45b94…` at **b504585 (Phase 0 recording) == e21e2c6 == HEAD == working tree**; it is the only commit in the entire branch that ever touched that path.

**No backend conditionals.** `scenario.py` (`4a4c8f15a7fa`), `run.py` (`79bb0230d0ed`), `common/database.py` (`d19e04bbf65b`) are blob-identical to base. `tests/golden/harness.py` is the only pre-existing test file touched: **+6 lines, 0 removals** — one tuple entry `"_scrape_event_relay"` in `_PATCHED_LOOPS` plus comments. Every backend conditional in `harness.py` predates Phase 2.

**Nothing weakened.** Zero files deleted; **zero removed lines anywhere under `tests/`**; no `assert True`, no `except AssertionError`, no swallowed assertions. Only 2 `skipTest` calls, both PG-gates. New tests: 106 test functions / 357 assertions.

**Trap 1 respected.** No lease `UPDATE` gained `RETURNING` (all three sites still end at `status='processing'`). The two `SELECT retry_count … FOR UPDATE` statements were widened by columns only — predicate and `FOR UPDATE` intact, `row[0]` still `retry_count`.

**Trap 2 respected.** AST extraction of real call sites, base vs HEAD, is **identical**:
```
["_write_lock('accept_results_batch')", "_write_lock('checkpoint')", "_write_lock('optimize')",
 "_write_lock('pull_tasks')", "record_stage('commit')", "record_stage('save_result')",
 "record_stage('total_in_lock')", "record_stage('update_tasks_lease')"]
```
A grep flags `record_stage("save_record"` as added — it is a docstring at `common/pgdb/results_write.py:123` saying *not* to add it. `PUBLIC_API` is untouched; only the class bases and `_MIXINS` gained `EventStreamMixin`.

## 4. SQLite no-op — proven, not taken on faith

```
backend                      = 'sqlite'
database class               = common.database.Database
phase-2 modules imported     = []          ← after importing the full server.app graph
sqlite Database attrs        = {"run_event_relay": false, "stop_event_relay": false,
                                "start_event_relay": false, "event_stream_stats": false,
                                "event_relay_metrics": false, "_emit_outbox": false,
                                "_gen": false, "_instance_id": false}
sqlite objects total         = 38
event-stream-ish objects     = []
relay entrypoint on sqlite   = returned without touching db (tripwire object, no attribute reached)
relay entrypoint with db=None= returned cleanly
```

`common.pgdb` and `common.slowhash` are **never imported** on the SQLite path — stronger than a runtime `if is_postgres()` guard, exactly as the design claimed. And the physical SQLite schema after a real `connect()` is byte-identical base vs HEAD: both `sha256 f823979bce41bf15b03eded3a3b0e54401bc8253f9e8a7108d190b5e383b8348`, 38 objects each.

## 5. Mutation tests — the harness still has teeth, on both backends

Line-targeted, restored via `git checkout` after each; `common/database.py` sha256 `282c2f82…` identical before and after.

| Mutation | SQLite (`common/database.py`) | Postgres (`common/pgdb/`) |
|---|---|---|
| M1 extra column in `SELECT d.*` | ❌ **19 diffs** | ❌ **19 diffs** |
| M2 lease gate → `if False:` | ❌ **33 diffs** | ❌ **33 diffs** |
| M3 `ORDER BY d.id` → `d.asin` | ❌ **69 diffs** | ❌ **69 diffs** |

M2 fires the bidirectional lease assertion on both: `[submit_result_stale_lease].stale: True -> False`, `[result_after_stale_reject] status: 404 -> 200`.

**On the 45→33 discrepancy vs the Phase-1 report:** I ran the identical `if False:` mutation at base `e21e2c6` in a worktree — it also yields **33**. The teeth are unchanged; Phase 1's "45" was a different mutation flavour, not lost coverage.

## 6. Extra: the *new* tests bite too

Mutating Phase 2's own code (a suite that cannot fail is the classic cheat):

| Mutation | SQLite | Postgres |
|---|---|---|
| P1 `emit_fn = None` (hook never fires) | 35 failed / 34 passed | 36 failed / 34 passed |
| P2 `classify_success_outcome` → always `ok` | 3 failed | 3 failed |
| P3 `pg_try_advisory_lock` → `True` (singleton defeated) | 2 failed | 2 failed |

P3 fails `test_second_relay_refuses_to_start` and `test_two_databases_only_one_relay`, so the integrator's leaked-lock fix is genuinely guarded. `tests/test_golden_with_relay.py` is not vacuous: it asserts `_scrape_event_relay` is absent from `_PATCHED_LOOPS`, compares all 64 steps via `json.dumps(sort_keys=True)`, and fails under P1.

## 7. Tree and databases

Working tree clean at `69886c6`, nothing pushed. My base worktree symlink at `/tmp/pre2/.venv` removed; `/tmp/pre2` clean. No scratch databases leaked by me — the transient `scraper_try_*` names observed mid-run belong to a **sibling verifier running concurrently in this same session** (its command line points at this session's scratchpad). `scraper_dev` is intact and has **0 relations in `public` or `scraper`** — Phase 2's DDL never ran against it. (`scraper_dev` also carries pre-existing `ftstry` and `tx_probe` schemas from earlier phases' probes; I did not create or remove them.)

## 8. Two things I did not chase, flagged for the owner

- **`completeness = 0` on every row** blocks §5.5's `catalog.products` upsert. Already in the integration report; unchanged by this audit, and correctly out of Phase 2's reach.
- **`seq` gaps are real** (rolled-back relay batches burn `nextval`). Phase 3's `after_seq + 1 < min_available_seq` retention guard will false-positive across a gap.

Scripts, all re-runnable: `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/{sqlite_noop.py,sqlite_schema.py,mutate.sh,mutate_pytest.sh}`