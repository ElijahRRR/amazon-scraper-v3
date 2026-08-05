Committed as `69886c6`, tree clean, nothing pushed.

## What I found and fixed

The three builders' work integrated more cleanly than expected — the `slowhash` ↔ `relay` seam resolves correctly (`compute_hashes` found, two-arg form detected, `HASH_VER 'v1'` → `hash_ver 1`), and the write-hooks builder had already deleted their duplicate emitter in favour of `EventStreamMixin._emit_outbox`. Three real defects remained, all in the接线 nobody owned:

**1. Relay start/stop race leaked the singleton lock (the serious one).** `lifespan` is `create_task(relay)` → `yield` → `await stop()`. Stop fast enough and the relay is still inside `asyncpg.connect()`; `relay_task`/`relay_conn` aren't assigned until the *end* of `start_event_relay`, so stop saw nothing and no-op'd. Measured before the fix:

```
关服之后：relay_state='running' relay_conn=<Connection ...>
库上残留会话=1  残留 advisory lock=1
第二个实例 start_event_relay() -> False  state='refused'
```

A leaked connection plus an unreleased advisory lock — and the next instance is **permanently refused**, meaning the event stream silently stops working while HTTP stays green and the outbox grows without bound. Fixed with a `stopping` flag + `start_epoch` re-checked at all three await points; after the fix, 0 residual sessions, 0 residual locks, second instance starts. Mutation-checked (removing the flag fails the test).

**2. `relay_state` conflated "never started", "cleanly stopped", and "died".** All three read `stopped`, which defeats the entire purpose of an observability endpoint. Added `starting`/`failed`. Also mutation-checked.

**3. 26 collateral test failures under `DB_BACKEND=postgres`.** Phase 2's new root-level test files land alphabetically between `tests/pgdb/` (whose conftest repairs the current event loop) and `test_session_slot.py` (which needs it), so the existing repair no longer reached. Lifted to `tests/conftest.py` — D-27 explains why I didn't instead rewrite `test_session_slot.py`'s 31 deprecated `get_event_loop()` calls.

## Golden harness: no-op'd, *and* proven harmless

I kept the relay in `_PATCHED_LOOPS` — consistent with the other four loops, and the baseline was recorded on SQLite where no relay exists, so background activity during recording is exactly what that list prevents. This is safe only because the DDL runs in `connect()` rather than relay startup (D-25); otherwise write hooks would hit a missing table during the PG golden replay. I then *also* proved the stronger property in `tests/test_golden_with_relay.py`: with the relay genuinely enabled, all 64 steps are byte-identical.

## Metrics

`GET /api/_debug/event-stream`, `include_in_schema=False` — required, since `/openapi.json` is a pinned baseline step. No existing response changed; `/api/diagnostic` is baseline step 55, so a new key there would have been a "field appeared" diff. Answers on both backends (SQLite returns `enabled: false`, not 404/500).

## End-to-end proof

Golden scenario + relay live + three supplementary terminal paths. Ledger reconciles exactly:

```
outbox 剩余=0  死信=0  source_id 重复=0  (task_id,attempt) 重复=0
outcome<>'ok' 却带哈希的行数=0   seq 严格递增=True
  OK blocked  预期 1 实际 1 | not_found 1/1 | ok 5/5 | parse_failed 1/1 | stale 1/1 | 合计 9/9

seq outcome      asin        zip   task att err            review_hash  slow_hash
  4 stale        B0GOLDEN02  90210    2   0 -              NULL         NULL
  7 not_found    B0EXTRA001  94105    9   0 -              NULL         NULL
  8 blocked      B0EXTRA002  94105   10   3 blocked        NULL         NULL
  9 parse_failed B0EXTRA003  94105   11   1 variant_offset NULL         NULL
```

Note seq 4: `zip_requested=90210` from `tasks.zip_code` while the payload said `10001` — the observed-vs-requested distinction working. And the per-task ledger showed events surviving for tasks whose batch was deleted, which I turned into an assertion: the stream is an independent append-only log, not a view of `tasks`.

## Two things worth your attention

**`completeness` is `0` on every row**, and §5.5's consumer algorithm gates the `catalog.products` upsert on `completeness_ok`. As written, **no Phase-2 row would ever reach `products` and the Walmart side stays empty.** Phase 2 can't fix this (§4.3 needs HTML-block presence, which isn't observable here). I recorded the recommended reserved-bit scheme in the plan, but Phase 3's contract must decide explicitly what a consumer does with `completeness = 0`.

**`seq` has gaps by design** (rolled-back relay batches burn sequence values). Phase 3's planned `after_seq + 1 < min_available_seq` retention guard will false-positive across a gap at a partition boundary.

I did not relitigate the two decisions the builders flagged — `gen` reuse (D-22, follows the design spec over the bullet, since per-boot minting forces a full reconcile after every deploy) and the計劃外 dead-letter table (D-24). Both are now in the ledger with their reasoning, and both are one branch away from reversal if you disagree.