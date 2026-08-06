Both backends are green and committed as `5957860` on `claude/walmart-api-db-refactor-7oergd`.

## Final status

```
$ .venv/bin/python -m tests.golden.run verify                     -> ✅ 64 步与基线完全一致
$ DB_BACKEND=postgres .venv/bin/python -m tests.golden.run verify -> ✅ 64 步与基线完全一致
$ .venv/bin/python -m pytest tests/ -q                            -> 252 passed, 4 skipped
$ DB_BACKEND=postgres .venv/bin/python -c "import common.pgdb; common.pgdb.Database()"  -> OK, 50 public attrs
```

The golden baseline was never re-recorded (`git diff tests/golden/samples/` is empty).

I verified the PG run genuinely uses Postgres rather than falling back: it constructs `common.pgdb.Database` against a real scratch PG 16.13 database, with `created_at` a `str` (`'2026-08-04 21:38:37'`) and `needs_screenshot` an `int` — the D-1 type decision holding at runtime. Golden is repeatable (3 consecutive runs green).

## The one real bug I found and fixed

**Concurrent use of the single write connection.** D-2 makes `_db` one dedicated connection. aiosqlite queues operations on that connection's worker thread, so two coroutines sharing it is legal; asyncpg does not queue and raises `InterfaceError: cannot perform operation: another operation is in progress`.

The repo legitimately touches `_db` without holding `_write_lock` — `list_callback_due` (called on a timer by `_callback_dispatcher`) and six raw reads in app.py (1298, 2230, 2281, 2289, 2294, 2309). Those are fine under SQLite, so equivalence-first says don't change them.

**The golden harness is structurally blind to this**: it no-ops all four background loops and `TestClient` is sequential, so there is never a second in-flight operation. I reproduced it outside the harness — 100% failure rate on Postgres, clean on SQLite.

Fix: `ConnProxy._op_lock`, a statement-level `asyncio.Lock` reproducing aiosqlite's internal queueing. It wraps one statement, not the transaction, so "another coroutine's SELECT lands mid-transaction" behaves identically on both backends. `_tx()` also had to stop using `raw.transaction()`, which bypassed the shim entirely.

I confirmed the regression tests aren't vacuous by reverting the fix: 3 of the 5 fail, exactly the ones targeting the bug.

## Diffs eliminated

Against the golden baseline: none existed when I started — the last agents had already driven PG to 64/64. My work was reconciliation and making it actually work outside the harness.

I built a stronger check the harness can't provide: a **real uvicorn server on both backends, background loops enabled, under 8-thread concurrent load** with the 2s completion watcher running. That started at 6 diffs and is now **0**, with zero 5xx and zero server-side exceptions on both sides. Fixed along the way:

1. **`optimize` caller key missing from `/api/_debug/lock-stats`** (real, live-only). Agent H had routed `run_startup_optimize` to the read pool without the lock to dodge the `InterfaceError` above. With the root cause fixed, I restored `_write_lock("optimize")` + `self._db`, matching SQLite exactly. The read-pool route was also riskier than SQLite: a full pool (exports hold connections for minutes) would leave ANALYZE holding `_write_lock`, stalling every write.
2. **Scratch-database name collisions** (agent F's finding). Names were `(label, pid)`, but conftest passes `node.name[:24]` so ~90 parametrized cases collapsed to one name — and `create_scratch` starts with `DROP DATABASE ... WITH (FORCE)`. Added a counter + random suffix in both `helpers.py` and the golden harness.

The other 4 initial diffs were contamination in my own test rig, not port bugs — worth flagging: **`config.DB_PATH` is not env-driven** (unlike `PG_DSN`), so a naively-launched SQLite server writes to the repo's real `data/scraper.db`. I removed the file I created (gitignored, contained only my test batch) and isolated the harness via a launcher.

## Differences I could NOT eliminate

**`maintenance_loop` produces no `checkpoint` caller key in lock-stats**, where SQLite does after 120s uptime. This is not a tradeoff — PostgreSQL has no client-side WAL checkpoint; WAL recycling is the server's checkpointer plus `max_wal_size`. Spinning an empty `_write_lock("checkpoint")` to make the metric match would fabricate observability data. Golden can't see it (`start_maintenance` is no-op'd by the harness). Documented in `admin.py` and OWNERSHIP.md D-14.

## Two spec claims I verified as false

Both were confirmed against SQLite, and both are traps for a future agent "preserving a bug":

- **`get_progress` does not raise `KeyError`** on an unknown status. The line is `stats[row["status"]] = row["cnt"]` — an assignment. An unknown status injects an extra key into the returned dict (which *is* an HTTP response body) and is excluded from `total`. Adding a `raise` to "preserve" this would introduce a genuine regression. The implementation was already correct; only the comments were wrong.
- **`get_batches` returns int `0`**, not NULL, for a zero-task batch.

I corrected both in `tasks.py` and OWNERSHIP.md, and amended `.agent/pg_migration_plan.md`, which still mandated `timestamptz`/`boolean` and deletion of `TimedLock`/lock-stats — all four contradicted by the baseline, and flagged by four separate agents. That file is what a new agent reads first.

**Still needs a human decision** (unchanged from the agents' reports): NUL bytes in scraped text are legal in SQLite and abort a whole PG transaction. Default is `PG_STRIP_NUL=0` (strict equivalence); stripping would alter stored data and therefore `content_hash`/`asin_changes`. Golden cannot settle it.