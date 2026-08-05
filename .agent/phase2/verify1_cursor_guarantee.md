## VERDICT: cursor guarantee HOLDS. Negative control loses rows in every configuration. Both proven, not asserted.

I wrote my own harness from scratch (did not read the builder's tests until after my results were in). All scripts are re-runnable and drop their scratch DBs; `cg_verify` is gone, `scraper_dev` untouched, tree clean at `69886c6`.

---

## 1. The paired control — the decisive experiment

The two arms are usually run separately, which leaves the objection "the interleavings weren't the same." I removed that: **one writer transaction inserts the same `source_id` into `scraper.scrape_outbox` AND `scraper.naive_events` and commits both together**, and one poll loop reads both tables back-to-back. Identical transactions, identical commit order, identical poll instants. The delivery mechanism is the only variable.

```
PAIRED CONTROL — one transaction feeds both arms, one poll loop reads both
writer transactions committed        : 10000
rows in scrape_events / naive_events : 10000 / 10000
outbox+relay   delivered=10000 of 10000 LOST=0     ( 0.00%)  strictly_increasing=True  live_skip_polls=0
naive cursor   delivered=7512  of 10000 LOST=2488  (24.88%)  strictly_increasing=True  live_skip_polls=1323
rows the naive cursor lost that the outbox arm DID deliver : 2488 / 2488
```

Every single row the bare `seq > X` cursor lost was delivered by the outbox arm, from the same commit.

## 2. Full matrix — same harness, both arms, both schedules

`lockstep` = 8 transactions opened in id order then committed in **exactly reverse** order, with a forced consumer poll right after the first commit (highest id committed, every lower id still invisible). `chaos` = 8 independent asyncio writers with randomized transaction hold, consumer on its own randomized cadence.

| arm | mode | rows | commit-order inversions | seq strictly ↑ | dupes | live skip detections | **committed rows never returned** |
|---|---|---|---|---|---|---|---|
| **outbox+relay** | lockstep | 4000 | **14000** | True | 0 | 0 | **0** |
| **outbox+relay** | chaos | 4800 | **7840** | True | 0 | 0 | **0** |
| naive (control) | lockstep | 4000 | 14000 | True | 0 | 1000 | **3500 (87.50%)** |
| naive (control) | chaos | 4800 | 7803 | True | 0 | 709 | **1415 (29.48%)** |

The inversion count is the harness's own non-vacuity proof, computed independently of the DB (Fenwick count over `(row_id, commit_completion_index)`). Both arms saw the same inversion counts in lockstep — the harness really is producing out-of-order commits, and only the naive arm loses.

I also ran a **live skip detector** after every poll — `count(*) WHERE seq <= X` vs. rows collected — so a skip is caught at the instant it happens, not just inferred at the end. It never fired once on the outbox arm across ~22,800 rows; it fired 1000/709/1323 times on the naive arm.

```
naive lockstep, first detections:
  [{'poll': 2, 'cursor': 8,  'committed_below_cursor': 8,  'collected': 1, 'skipped_now': 7},
   {'poll': 4, 'cursor': 16, 'committed_below_cursor': 16, 'collected': 2, 'skipped_now': 14}, ...]
  their seqs (first 20): [1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22]
```

## 3. Adversarial probes on the assumptions the guarantee rests on

**T-A — a transaction held open across ~1000 rows and 56 relay ticks.** The single most legible demonstration:

```
  [outbox] A's write-side id/seq at INSERT time : 1   (lowest of 1001 rows)
  [outbox] consumer cursor X before A committed : 1000  (relay ticks elapsed while A was open: 56)
  [outbox] A's final seq in scraper.scrape_events : 1001
  [outbox] A DELIVERED TO CONSUMER              : True
  [naive ] A's write-side id/seq at INSERT time : 1   (lowest of 1001 rows)
  [naive ] consumer cursor X before A committed : 1001
  [naive ] A's final seq in scraper.naive_events: 1
  [naive ] A DELIVERED TO CONSUMER              : False
```

Write-side id 1 comes out at seq 1001. That is the whole design in one line.

**T-B / T-C — ring 1 is both enforced and load-bearing.**
```
  instance #1 start_event_relay() -> running
  instance #2 start_event_relay() -> False   state='refused'
  advisory locks held on key 0x5343524150455631 : 1
  after #1 stops, instance #2 start -> True   state='running'
```
Then I defeated the lock on purpose (a second lock-free relay clone doing the same claim→insert transaction):
```
  rows in scrape_events                 : 3000
  COMMITTED ROWS NEVER RETURNED         : 656  (21.87%)
```
So `pg_try_advisory_lock` is not decoration — remove it and the guarantee dies immediately.

**T-D — 13 relay kill/restart cycles under 6 concurrent writers, with the consumer cursor persisted to disk and reloaded on every poll** (simulating a consumer restart between every request): 2400 rows, 0 lost, 0 duplicates, 0 skip detections, outbox drained to 0.

**T-E — relay aborted *after* the INSERT burned sequence values**, every 3rd batch, so the burn lands between delivered rows:
```
injected aborts (post-INSERT, pre-COMMIT): 11
rows in scrape_events / distinct sid     : 600 / 600
outbox left / dead-letter                : 0 / 0
SEQ GAPS observed                        : 11  (seq values burned: 156)
  first 5 gaps (last_seq -> next_seq)    : [(23, 27), (72, 80), (128, 137), (190, 195), (248, 273)]
COMMITTED ROWS NEVER RETURNED            : 0
```
Gaps are real, interior, and harmless to the cursor. (T-D alone never caught the relay mid-transaction — 0 gaps — so this path needed forcing; the builder's suite has the same blind spot in its natural-timing tests.)

---

## 4. Findings

**(a) NEW BUG — the relay never recovers from a dead connection, and `relay_state` lies about it.** `pg_terminate_backend` on the relay's own session:

```
before kill: events=50 state=running tick_errors=0
pg_terminate_backend(9538) -> True
relay 批次失败（**不是**某一行的错，行全部留在 outbox，零丢失）：InterfaceError: connection is closed   ×∞
after kill : events=50 outbox_backlog=70 state='running' tick_errors=14 relayed=50
advisory locks still held on the singleton key : 0
a SECOND instance can now take the singleton lock : True
after the second instance takes over: events=120 outbox_backlog=0
instance #1 still reports relay_state='running'
```

Data safety is intact (0 rows lost, backlog waits in the outbox, a takeover drains it). But the relay never reopens the connection — it spins one `InterfaceError` per tick forever — and `relay_state` stays `'running'`. The `failed` state added in D-26 only covers "the main loop raised out"; it does not cover "every tick raises but the loop keeps going." On a single-instance deployment (which this is) the event stream stops permanently while the field designed to make the stall loud reports healthy. Two things do still work: `counters.tick_errors` climbs, and `/api/_debug/event-stream` gets `outbox_depth`/`relay_lag_s` from a live pool query in `event_stream_stats()`, overwriting the stale in-memory value — so the backlog is visible there. Suggested fix: after N consecutive tick failures either reopen the connection (re-acquiring the lock, refusing if someone else took it) or set `relay_state='failed'`. `common/pgdb/relay.py:1125` `_relay_main`.

**(b) Scoping — the hazard is not reachable through the API today, and that has a consequence for the test suite.** 24 concurrent `accept_success_result` calls through the real `Database`:

```
outbox id order                   : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] ...
write-completion rank of each row : [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] ...
COMMIT-ORDER INVERSIONS via the real API : 0
```

D-2 still keeps one write connection behind a real `_write_lock`, so production commits are serial and out-of-order commit cannot occur yet. The outbox is Phase-1.5 insurance, correctly built ahead of the need — **but no end-to-end or HTTP-level test can ever exercise the guarantee.** The direct-to-outbox multi-connection test is the only possible regression guard; if someone later "simplifies" it into an API-level test, the guarantee silently stops being tested while the suite stays green. Worth a comment at the test.

**(c) Rewind detection has a false-positive path for Phase 6.** `_bootstrap_identity` (`common/pgdb/relay.py:786`) mints a new `gen` when `COALESCE(max(seq),0) < max_seq_ever`. If retention ever empties `scrape_events`, an ordinary restart mints a new gen → consumer hard stop + full reconcile. I tripped this incidentally on every `TRUNCATE`:
```
事件流倒退：max(seq)=0 < max_seq_ever=1001 ... 铸新 gen 7b8ef3c5d32d -> 2974f9fdc4d7
```
Low probability under partition retention (the newest partition keeps high seqs), but it is a one-line guard and Phase 6 owns the DROP.

**(d) Assessment of the builder's own cursor test.** `tests/pgdb/test_relay.py:432` `test_cursor_guarantee_under_staggered_commits` is honest work — it has an explicit non-vacuity assertion and a naive control, which is exactly right and rarer than it should be. Its limits, all of which my harness covers: 96 rows (vs 4000–10000); end-state set equality only, no per-poll skip detector; cursor held in memory, never persisted/reloaded; no seq-gap case; no long-transaction-across-many-ticks case; and its naive control is a separately hand-built scenario rather than the same harness on the same transactions. Its conclusions replicate at 100× the scale.

**(e) One unresolved pytest flake, not event-stream code.** Golden is 64/64 on both backends, repeatedly. `pytest tests/ -q` = **427 passed, 6 skipped** (sqlite) / **429 passed, 4 skipped** (postgres) on 8 runs, matching the builder's claim exactly — including a run of their exact command sequence. But twice, early on, I got `FAILED tests/test_delivery_parse.py::SlxDeliveryTests::test_date_as_direct_text_node` (`1 failed, 428 passed, 4 skipped` sqlite; `1 failed, 430 passed, 2 skipped` postgres). The skip arithmetic proves `_HAS_SLX` was `True` in those runs, i.e. `selectolax` was importable — which nothing in the tree can cause (`selectolax` is not installed anywhere on the machine, and no test stubs it; instrumented collection reports `_HAS_SLX=False` every time). Pre-Phase-2 `e21e2c6` full suite: `268 passed, 4 skipped`, clean. I could not reproduce in 8 attempts and I am not going to claim it is benign — it is a parser test with zero relationship to the event stream, but it is a real observation and whoever owns test hygiene should know.

---

## 5. Scripts

- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/cursor_guarantee.py` — `--arm outbox|naive --mode lockstep|chaos`, the 4-cell matrix
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/paired_arms.py` — the paired control (both arms, one transaction)
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/cursor_adversarial.py` — T-A / T-B / T-C
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/cursor_churn.py` — T-D (relay restart churn, persisted cursor)
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/cursor_gaps.py` — T-E (burned seq / interior gaps)
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/relay_conn_death.py` — finding (a)
- `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/reachability.py` — finding (b)

I recommend porting T-A, T-D and T-E into `tests/pgdb/test_relay.py` at reduced scale; they cover three real paths the current suite does not, and each one already found or confirmed something.