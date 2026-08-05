Verification complete. All probes are in `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v2/`, all re-runnable, all scratch DBs dropped.

## Verdict

The sync API conforms to its contract. I drove it as a real consumer (`consumer.py` — §7's pull algorithm line for line, talking only HTTP) across ~700 pages in 10 probe scenarios and **never once lost a record silently**. Every requirement in my brief checks out. Two findings worth acting on, neither a data-loss bug.

## The snapshot attack (the one you asked me to try hardest to break)

I injected a `DELETE … WHERE seq <= 150` from a **separate committed session at the exact instant between the `MIN(seq)` read and the page query** (monkeypatching `sync._bounds` to fire the prune after its first call). Seeded 1..200, consumer parked at `after_seq=100`, so 101..150 is data it still needs. Four arms:

```
  A  as shipped (RR + re-check)          -> 200 rows=100 first=101   skipped=0
  B  READ COMMITTED, re-check live       -> 409 cursor_below_retention
  C  RR, re-check blinded                -> 200 rows=100 first=101   skipped=0
  D  READ COMMITTED + no re-check        -> 200 rows=50  first=151
                                            *** SILENTLY SKIPPED 50 seqs: 101..150 ***
```

Arm D is the counterfactual: the failure mode the design claims to close **is real and reachable**, so neither mechanism is decoration. As shipped (A), the consumer gets a complete pre-prune page — better than a 409, because REPEATABLE READ makes the prune invisible. The 409 is the fallback that fires the moment isolation degrades (B), driven by the post-page re-check logging `同步快照不稳定：页查询前后的 min_available_seq 不同（1 -> 151）`.

One nuance worth stating plainly: **under the shipped configuration the consumer does not get a 409 here, and shouldn't** — nothing was skipped. A 409 only appears when the pruned range is genuinely gone before the request starts (verified separately in P3f/P3g).

I also checked the *real* Phase 6 prune mechanism: `DROP TABLE scraper.scrape_events_p0` issued mid-request **blocked >3s behind the reader's ACCESS SHARE** while the sync request returned `200 count=20`. Retention cannot tear a page out from under an in-flight read — but a slow consumer therefore delays retention.

## Everything else in the brief

- **No record missed, writers active** — 14 batches / 224 results through the real write path → outbox → real relay → HTTP: 26 cycles, 312 pages, `MISSED: []`, `PHANTOM: []`, sets equal. A live per-page skip detector (independent connection, checks after *every* page that no row with `seq <= cursor` is unaccounted for) found **0 skips over 88 pages** with `OVERLAP=0`. Four concurrent consumers each ended holding exactly the table's 96 rows.
- **source_id dedup** — 1863 duplicate deliveries absorbed in P1, 150 in P6g; the same range pulled 5× is byte-identical; `source_id` unique and gen-prefixed for every row.
- **Both 409s, and NOT otherwise** — drove `after_seq` 0..60 against a deliberately holey stream (10..20, 40..50 present; 21..39 burned) and compared every response to the contract predicate: **0 mismatches**. Boundaries confirmed: `after_seq=8`→409, `9`→200, `50`→200, `51`→409. Neither 409 is switchable off by any parameter combination. 409 bodies carry the full diagnostic set and **never a `records` key**.
- **Empty is 200, never 404** — virgin stream, caught-up consumer, and pruned-to-empty, on all four endpoints. Pruned-to-empty correctly reports `min=51 max=50` so a routine retention pass does **not** trip §7's `max_seq < stored_max_seq_ever` hard stop.
- **ack** — monotonic (`10,25,7,0,25,60` → stored `10,25,25,25,25,60`), 60 concurrent random acks all returned ≥60 and the final value stayed 60; `61`→409 `ack_ahead_of_stream`; wrong/short/upper-cased gen→409, empty/None/int gen→422.
- **counts vs direct query** — agreed on 40 random sub-ranges plus 10 hand-picked edges, including `by_outcome` maps and hour buckets summing to `count`.
- **`/openapi.json`** — served spec is byte-identical to the pre-Phase-3 commit: `sha256=039ea9c075e76fbf9f2701cb02ec764fc30f914c97345295ca66a19285f04bf3`, 51 paths, no `/api/v1`, on both backends. Non-vacuity: rebuilding the same endpoints without `include_in_schema=False` makes all four appear.
- **SQLite unaffected** — golden 64/64, all four endpoints `503 event_stream_unavailable backend='sqlite'`, never 404/500.
- **Gates** (unchanged from the Phase 3 report): golden 64/64 both backends; pytest 483/31 sqlite, 485/29 postgres; unittest OK both; `test_sync_api.py` 39 passed.
- Bonus gaps their suite doesn't cover: **error paths return their pool connections** (75 × 409/422 responses, `pool idle 10 -> 10, size 10 -> 10`), and 20 parameter-fuzz cases produced **zero 5xx** — including `outcomes=ok'; DROP TABLE scraper.scrape_events;--` → 422, table intact.

## Finding 1 — MEDIUM: `/status.gen` is process-local and can disagree with the other three endpoints

`server/api/sync.py:646-647` serves `gen`/`instance_id` from `stats.get(...)`, which is `event_relay_metrics()` → `self._ev()["gen"]`, an **in-memory value written once by `_bootstrap_identity` at `connect()`**. `/records`, `/counts` and `/ack` all read `scraper.sync_meta` — and `sync_status` already has that `meta` in hand at line 601 and simply doesn't use it.

Reproduced with **no forged UPDATE** — the product code mints the gen itself. Instance A running; DB rolled back (rows + sequence, `max_seq_ever` intact — the `pg_restore`-of-an-older-dump shape); instance B connects, its rewind detector mints a new gen. A is still up:

```
  A.relay_state='running'   A in-memory gen still '757c4d3104fb'
  sync_meta.gen                       : 'd793ce278a7a'
  GET /status  gen='757c4d3104fb'     GET /records gen='d793ce278a7a'
  rows whose gen != sync_meta.gen     : 13/13     source_id prefixes: ['757c4d3104fb']
  GET /records top-level gen='d793ce278a7a' but record.gen values = ['757c4d3104fb']

  §7's tripwires evaluated against instance A:
    st.gen != stored_gen         -> False
    st.max_seq < stored_max_ever -> False   (18 vs 10)
    forced_prune_log             -> False
    ==> any §7 hard stop fires? False
    the only signal that does fire: POST /ack -> 409 gen_mismatch  (§7 never checks this)
```

So in the one scenario `gen` exists to catch, **neither §7 tripwire fires**, the response envelope's `gen` contradicts its own rows' `gen`, and the only loud signal is an ack 409 that the contract's own pseudocode discards. Phase 6's retention floor (`min(time floor, ack_seq)`) then stops advancing with nobody noticing.

Phase 3's half is a one-line fix — `/status` should use `meta.get("gen")`/`meta.get("instance_id")` like its three siblings. The deeper half (`common/pgdb/relay.py:1541` stamping rows from a never-refreshed `_ev()["gen"]`) is Phase 2 territory and out of Phase 3's scope. Requires two live processes plus a gen mint, which is why I'd call it MEDIUM rather than a blocker — but a rolling restart is exactly the deployment B2 was fixed to support.

## Finding 2 — for Phase 6: §7's mandated `OVERLAP` and an ack-based retention floor are incompatible unless the floor keeps slack

Running retention concurrently with writers and a conforming consumer, four arms, all with **zero silent loss**:

```
  1a floor==ack,     OVERLAP=20 : reason='cursor_below_retention'  loss=0
  1b floor==ack,     OVERLAP=0  : reason=None                      loss=0
  1c floor==ack-60,  OVERLAP=20 : reason=None                      loss=0
  2  floor==head-3,  OVERLAP=20 : reason='cursor_below_retention'  loss=0
```

Arm 2 is the desired behaviour: an emergency prune that outruns the consumer produces a 409, never a hole. But arm 1a shows that a floor set exactly at `ack_seq` makes the contract-mandated overlap re-read (`X = cursor - 200`) land below the retention window **every pass** — a guaranteed spurious hard-stop-and-full-reconcile. 1b and 1c isolate the cause to the overlap alone. In production `min(time_floor, ack_seq)` will normally be dominated by the time floor so this won't bind, but the constraint is real and I didn't find it written down: **Phase 6's floor must stay at least `OVERLAP` below `ack_seq`.** Worth a line in `docs/sync_contract.md` §7 and in the Phase 6 plan.

## Smaller notes, not defects

- `cursor_below_retention`'s documented false positive is real and I reproduced it cleanly: on a stream whose lowest seq is 10, a cursor at 5 gets a 409 even though 1..9 never existed and nothing was pruned. Correctly documented in §2.6 as the conservative direction.
- `collected_at='2026-08-05T00:31:24Z'` vs `recorded_at='2026-08-05T08:31:24Z'` on the same row — the 8-hour `crawl_time` timezone gap already listed in §10.3 as a Phase 4 item. Consistent with the disclosure.
- Every key the contract document shows is actually served: `/records` 17/17, `/status` 24/24, record object 24/24, nothing documented-but-absent. All eight timestamp fields match RFC 3339 UTC with trailing `Z`.
- Terminal-failure-only emission is worth knowing when reading counts: 224 ingested results produced 205 events, because retryable `fail_task` calls requeue instead of emitting. That's Phase 2 semantics (`common/pgdb/tasks.py` F2 branch), correct, and not a Phase 3 concern.

Repo untouched: working tree clean at `2e9c311`, `sqlite_baseline.json` and `common/database.py` verified unchanged, git worktree removed, zero scratch databases left.