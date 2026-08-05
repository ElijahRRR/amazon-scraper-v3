Cleanup done, tree clean, no scratch databases left. Here is my report.

---

# Verification 1 — re-attack of the Phase 2 fixes

Everything below is measured on fresh databases with the real `Database`/relay code, each original issue reproduced against a `git worktree` at the pre-fix commit `fea7395` before being declared fixed. Probes: `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v6/` (`harness.py`, `b1_boundaries.py`, `b2_rolling.py`, `b3_kill.py`, `b4_rewind.py`, `new1_recover_churn.py`, `new2_retry_leak.py`, `new3_upgrade.py`, `new4_cursor_under_kills.py`, `new5_outage.py`). All drop their own DBs; the worktree is removed; `git status` is clean.

## B1 — partition CHECK inheritance: **FIXED**

Fresh DB, **production `SPAN=20,000,000`**, three crossings driven by the real relay (outbox → claim → `scrape_events`). The sequence is `setval`'d to `boundary-3` before each crossing — the rows themselves are ordinary production-path rows at production-magnitude seq.

```
--- partitions straight after connect() ---
  scrape_events_p0   FROM (MINVALUE) TO ('20000000')   uniq(source_id)=True
  scrape_events_p1   FROM ('20000000') TO ('40000000') CHECK scrape_events_p1_range (seq>=20000000 AND seq<40000000)
  scrape_events_p2   FROM ('40000000') TO ('60000000') CHECK scrape_events_p2_range (seq>=40000000 AND seq<60000000)
  STRUCTURAL AUDIT after connect(): CLEAN

CROSSING p0->p1 (20,000,000)  drained=True   {'outbox':0,'events':8,'dead':0}
CROSSING p1->p2 (40,000,000)  drained=True   {'outbox':0,'events':16,'dead':0}
CROSSING p2->p3 (60,000,000)  drained=True   {'outbox':0,'events':24,'dead':0}   # p3/p4/p5 built at RUNTIME

READABILITY
  ONLY scrape_events_p0  n=2  seq 19999998..19999999
  ONLY scrape_events_p1  n=8  seq 20000000..39999999
  ONLY scrape_events_p2  n=6  seq 40000000..40000005
  ONLY scrape_events_p3  n=8  seq 60000050..60000057
  parent SELECT count(*) n=24    sum over partitions n=24
  cursor walk seq>X ORDER BY seq n=24 strictly_increasing=True
B1 VERDICT: PASS
```

Counterfactual at `fea7395`, same probe:
```
  scrape_events_p2  CHECK scrape_events_p1_range (seq>=20000000 AND seq<40000000)   <<< FOREIGN
  STRUCTURAL AUDIT after connect(): BROKEN
CROSSING p1->p2  drained=False   {'outbox':0,'events':8,'dead':0} -> {'outbox':4,'events':8,'dead':4}
  outbox 队头 id=9 已隔离进 scraper.scrape_outbox_dead …  (x4, healthy rows)
```

**Upgrade path independently verified** (`new3_upgrade.py`, two subprocesses so the two trees are separate imports — this is the state every existing Phase-2 database is in):
```
STEP 1 (fea7395 builds it):  foreign range CHECKs after connect(): [('scrape_events_p2','scrape_events_p1_range')]
STEP 2 (fixed tree opens it): foreign range CHECKs AFTER: []   range_checks_repaired = 1
   p2 still carries scrape_events_marketplace_check: True      # the ATTACH prerequisite survives
   drained=True {'outbox':0,'events':5,'dead':0}  rows now in p2: [40000001..40000005]
```
`scraper_dev` has no `scraper` schema, so nothing to repair there — still true.

**One caveat, and it is a probe artifact, not a defect.** The p2→p3 crossing took **103.6 s** to heal, because `setval` jumps the sequence 20 M in one step while partition maintenance runs on a tick cadence (`RELAY_PARTITION_EVERY=60`). In production this cannot happen: the invariant keeps ≥2 partitions ahead, i.e. ≥20 M seq of headroom, and outrunning a 60-tick cadence would take ~333 k events/sec. The only production path that moves the sequence *forward* is `_bootstrap_identity`'s rewind branch, and it calls `ensure_event_partitions(conn, floor_seq=target)` before the `setval`. During that stall, though, I saw the relay reconnect 16 times — which is finding **NEW-1** below.

## B2 — rolling restart: **FIXED**

Modelled on `server/app.py`'s lifespan (`create_task(run_event_relay())` … `await stop_event_relay()`), verdict measured on the database, not on `relay_state`.

```
                              patched                          fea7395
t0 OLD                        state=running holders=1          state=running holders=1
t1 NEW (OLD still up)         state=refused task.done=False    state=refused task.done=True
t2 OLD stops                  holders=0                        holders=1
t3 6 rows emitted after t2    state=running holders=1          state=refused holders=0
                              {'outbox':0,'events':6,'dead':0} {'outbox':6,'events':0,'dead':0}
B2 VERDICT                    PASS                             FAIL
```

## B3 — dead connection / lying `relay_state`: **FIXED**

`pg_terminate_backend()` on the relay's own backend, with a 10 Hz sampler recording `(relay_state, singleton-lock holders, events, outbox)` throughout, so "the state field lied" is a number.

```
recover  (patched)                                  fea7395
 3 tick failures -> reconnect, lock retaken          t=0..20s: state='running', holders=0, events frozen at 38
 emitted=518 final={'outbox':0,'events':518,'dead':0}   emitted=519 final={'outbox':481,'events':38,'dead':0}
 state=running reconnects=1 tick_errors=3               state=running reconnects=0 tick_errors=84
 (state=='running' AND holders==0): 6 samples, 0.5s     (state=='running' AND holders==0): 244 samples, 24.9s of 25s
 B3/recover VERDICT: PASS                               FAIL

handover (patched)
 victim state='refused' and exits; rival state='running'; 547/547 delivered, 0 dead
 singleton lock never held by 2 sessions: True       B3/handover VERDICT: PASS
```

**A flaw in my own probe, corrected:** my first holder count queried `pg_locks` without a database filter and reported "2 writers". PostgreSQL advisory locks are **per-database** — measured directly: two sessions in different databases both get `pg_try_advisory_lock` on the same key, a third in the first database is refused. All numbers above are re-measured with `database = current_database()`.

## B4 — rewind false positive: **FIXED**

Four scenarios, four fresh DBs, each doing a real restart (`new Database()` → `connect()` → `start_event_relay()`).

| scenario | patched | fea7395 |
|---|---|---|
| `DELETE FROM scrape_events` | gen unchanged, `rewinds_detected=0` | **new gen**, `gen_minted=2` |
| `DETACH`+`DROP` p0 (Phase 6's mechanism) | gen unchanged, `rewinds_detected=0` | **new gen**, `gen_minted=2` |
| `TRUNCATE` (no RESTART IDENTITY) | gen unchanged, `rewinds_detected=0` | — |
| `setval(seq, 1, false)` (restore-from-backup) | **new gen**, `rewinds_detected=1`, seq pushed to 13 | new gen |

In every scenario, `seq values published in run 1 AND reissued in run 2: none` — the thing the gen exists to protect.

## Cursor guarantee — no regression

verify1's matrix, unchanged code, same non-vacuity evidence:

```
outbox/lockstep  2000 rows  inversions=7000  skips=0  dups=0  never-returned=0  PASS
outbox/chaos     4000 rows  inversions=6636  skips=0  dups=0  never-returned=0  PASS
naive /lockstep  2000 rows  inversions=7000  skips=500  LOST 1750/2000 = 87.50%  FAIL (control)
naive /chaos     4000 rows  inversions=6574  skips=595  LOST 1209/4000 = 30.23%  FAIL (control)

paired_arms.py (one transaction feeds both arms, one poll loop reads both)
  outbox+relay  delivered=10000 of 10000  LOST=0      live_skip_polls=0
  naive cursor  delivered=7451  of 10000  LOST=2549   live_skip_polls=1318
  rows the naive cursor lost that the outbox arm DID deliver: 2549 / 2549      VERDICT: PASS
```

And a new case the fixes made necessary — the guarantee **while B3's recovery path is firing** (`new4_cursor_under_kills.py`, backend killed every 3 s):
```
writers=6x1200  relay backend kills=7   reconnects=6..7   tick_errors=18
NON-VACUITY commit-order inversions: 7106 (transactions=7200)
rows committed 7200 / in scrape_events 7200 / collected 7200
strictly increasing=True  monotonicity violations=0  duplicates=0  LIVE SKIP DETECTIONS=0
never returned=0  lost outbox->events=0  outbox left/dead=0/0
after settle: state=running holders=1                                    NEW-4 VERDICT: PASS
```

## New damage the fixes introduced

**NEW-1 (MEDIUM) — `_relay_recover()` treats *every* persistent tick failure as a connection failure.** `consec_tick_fail` counts all tick exceptions, so at `RELAY_RECOVER_AFTER` the relay closes a perfectly healthy connection, reopens, and re-takes the singleton lock. Reconnecting fixes none of the realistic triggers: `QueryCanceledError` under a slow DB (verify4's still-open MEDIUM 6), seq past the last partition (seen naturally in the B1 run), disk/WAL full. Induced with pure config — `command_timeout=1 s` plus a 3 s `BEFORE INSERT` sleep, i.e. MEDIUM 6's exact shape, connection alive throughout:

```
                                        patched                 fea7395
20 s window, tick_errors=15             relay_reconnects=5      relay_reconnects=0
holders==0 (100 Hz sampler)             9/1833 samples, 0.5%    0/1830 samples
longest contiguous unlocked run         ~20 ms                  0 ms
reconnects/minute at this failure rate  14.9                    0.0

with a rival instance polling for the lock (40 s):
  rival state=running   victim state=refused   lock holders now: 1
  were 2 sessions EVER holding the singleton lock: False
  … and the rival then began churning too ("第 1 次重连", "第 2 次重连")
```
Data safety is intact — never two writers, rows stay in the outbox (393 backlog, 0 dead-letters). The cost is relay **flapping between instances** during any sustained non-connection failure, and `relay_reconnects` — the counter B3 added to make connection death loud — becoming a noisy signal that fires on things that are not connections. Suggested fix: gate `_relay_recover` on the exception class (`InterfaceError` / `ConnectionDoesNotExistError` / `PostgresConnectionError`) and count non-connection tick failures separately.

**NEW-2 (LOW) — B2's refusal-log throttle is defeated by its own callee.** `_log_relay_refusal` (`common/pgdb/relay.py:1200`) throttles correctly, but `start_event_relay` (`:1067`) emits an **unthrottled** `logger.warning` on every attempt, and the new retry loop calls it every `RELAY_RETRY_SECONDS`:

```
retry=0.25s, ~80 refusal cycles in 20s:  from start_event_relay() (NOT throttled): 73
                                         from _log_relay_refusal() (throttled)   : 1
retry=5.0s (PRODUCTION), ~4 cycles/20s:  from start_event_relay() (NOT throttled): 5
                                         from _log_relay_refusal() (throttled)   : 1
```
≈15 WARNINGs/min ≈ 21,600/day per standby instance, for a condition the code itself documents as normal. At `fea7395` it was one message, once.

**Minor.** `_repair_foreign_range_checks` swallows every exception into a `logger.warning` — if the catalog query ever fails, the B1 self-heal silently does not run. Observed firing naturally during the kill probe: `分区 CHECK 体检失败（跳过本次修复）：InterfaceError: connection is closed`.

**Ruled out** (measured, all clean): connection leak in the retry loop (backends baseline=5 peak=5 final=5 over 80 refusal cycles); shutdown latency (`stop_event_relay` → `task.done=True` in 0.05 s against a 0.25 s period, 0.16 s against 5.0 s); residual backend or advisory lock after a refused standby stops; hot-looping when the database is unreachable (12 `_relay_open_conn` calls in 12 s at retry=1.0 s, 0.3 % of one core, `state=failed`, stop in 0.03 s).

## Gates

```
GATE 1 golden verify / sqlite     ✅ 64 步与基线完全一致   EXIT=0
GATE 2 golden verify / postgres   ✅ 64 步与基线完全一致   EXIT=0
GATE 3 pytest tests/ -q / sqlite      483 passed, 31 skipped, 1 warning in 149.78s
GATE 4 pytest tests/ -q / postgres    485 passed, 29 skipped, 1 warning in 163.94s
GATE 5 unittest discover / sqlite     Ran 79 tests ... OK (skipped=31)
GATE 6 unittest discover / postgres   Ran 79 tests ... OK (skipped=29)
sqlite_baseline.json: UNCHANGED     common/database.py: UNCHANGED
```

`git diff fea7395..HEAD -- tests/pgdb/test_relay.py | grep '^-'` is empty — the file is pure addition, no test deleted or weakened. B8's boxed scoping comment is at `tests/pgdb/test_relay.py:917` and records the measurement verbatim (`COMMIT-ORDER INVERSIONS via the real API : 0`); T-A/T-D/T-E are present at `:1103`, `:1179`, `:1281`.

**Scope note:** I re-attacked B1–B4 and the cursor guarantee. B5/B6/B7 were not re-attacked independently — B6's claim is covered by gates 5/6 passing on both backends.