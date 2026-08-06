No scratch DBs left, tree clean, `common/database.py` and `sqlite_baseline.json` unchanged.

## VERIFICATION 2 — PHASE 6 (RETENTION + ACK): PASSES ON ALL SIX MANDATED CLAIMS

Everything below is real partitions, real `DROP`, real HTTP through `server.app`. Probes: `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/p6v2/` (`rig.py`, `a1`…`a10`, `a4_run.sh`). I used `PG_EVENT_PARTITION_SPAN=1000` (set before importing `schema.py`) so a live stream turns over 10+ partitions instead of needing 200M rows.

### The headline claim, with a paired control

`a6_consumer.py` implements docs/sync_contract.md §7 **literally** (OVERLAP=200, limit=1000, one rewind per cycle, cursor only from `next_after_seq`, ack after every page). Producer = real `_emit_outbox` → real `_relay_drain_once` (real `nextval`, real partition maintenance), plus deliberate seq burning. Retention runs every cycle with `ack_slack_seq=200` — the tightest legal setting, so the floor sits *exactly* OVERLAP below `ack_seq`.

```
臂                             轮    生产    交付  409   闩锁    真丢     硬停
seed=11 实验组（P6-5 在）        31  1298  1298    0    0     0   None
seed=11 对照组（P6-5 废掉）        5   236   184    1    0     0    409  ('409','cursor_below_retention',1004,1174,False)
seed=101 实验组（P6-5 在）        31  1365  1365    0    0     0   None
seed=101 对照组（P6-5 废掉）       12   557   503    1    0     0    409  (…,1210,1363,False)
seed=202 实验组（P6-5 在）        31  1254  1254    0    0     0   None
seed=202 对照组（P6-5 废掉）       11   510   457    1    0     0    409  (…,2514,2656,False)
seed=303 实验组（P6-5 在）        31  1184  1184    0    0     0   None
seed=303 对照组（P6-5 废掉）        8   336   279    1    0     0    409  (…,1233,1384,False)
```

The only change between arms is `_visible_floor_after` → `-1` (P6-1's literal `max(seq) <= floor` left intact). Control 409s within 5–12 cycles on 4/4 seeds, with `forced_prune_log` **empty** — proving those are false positives, not real loss. Across the longer runs (5 seeds, serial and retention-concurrent-with-consumer): **9,204 records produced, 9,204 delivered, ~33,000 seqs burned, 35 partitions dropped, 0 spurious 409, 0 lost `source_id`.** Plus 5 hand-built collisions with the partition boundary placed at `ack−OVERLAP ± 1`: 0 false 409.

`a2_floor_fuzz.py`, 30 random layouts, same control:
```
30 轮随机布局：违反不变式 0 轮
对照组（废掉 _visible_floor_after）：违反 4 轮（其中 I4 = 4 轮）
  #4   floor=1500  裁=p0,p1  min(seq)-1 237 -> 2298，越过了 floor=1500
```

### The other five claims

- **Never-acked pruning (P6-2).** Fresh DB, `ack_seq` key absent → `dropped=2 forced=0`, partitions gone from `pg_class`, `n_dead_tup=0` (DROP not DELETE). Counterfactual with the plan's named trap `int(raw or 0)`: 0 dropped. The CHECK rejects `'0'/'00'/'-1'/' 5 '/'abc'/''`; a pre-existing `ack_seq='0'` self-heals (`phantom_ack_repaired=1`).
- **No partition dropped while holding rows above the floor.** Fuzz invariants I1–I6 all clean; targeted straddle case (`min<floor<max`) keeps every row. `stopped=would_break_overlap` fires in the wild — that guard is load-bearing, not decoration.
- **`min_available_seq` never cached (P6-4).** DROP between two calls on the same pool: `/records`, `/status`, `/counts` all jump `1 → 2010` in lockstep. All 6 `FORBIDDEN_META_KEYS` raise `RetentionInvariantError`. Counterfactual: the stale bound would return **200 with seq 16..2009 silently skipped**.
- **Latch survives restart (P6-3).** 4 forced entries; 4 separate OS processes (`pid=5114/5477/5482/6619`, `last_pass=None` each) all still see them. Per-entry ack works, is idempotent, unknown ids reported not errored; after full ack the flag drops and stays down across another restart. Entries are marked, not deleted. Overflow sacrifices only acknowledged entries (151 pending survive a 200 cap). 8 concurrent writes + 4 concurrent acks → 12 rows, no lost updates.
- **Ack monotonic + gen (P6-2/§5).** `1005→1010→5→1→1009→1010→2001` lands on 2001; 40 concurrent random acks land on the max; 13 rejection cases all correct (`gen_mismatch`, `ack_ahead_of_stream`, bool/str/float/negative/overflow → 422) and none mutate the stored value; `ack_seq: 0` is a 200 no-op writing **neither** `ack_seq` nor `ack_at`.

Also verified: `ensure_retention_schema` runs at `connect()`, `maybe_run_retention` is on the maintenance loop and throttles (3 calls → 1 pass), `DROP` yields to a long reader (`stopped=lock_timeout`, 0.34s, `/records` unblocked at 200, next pass completes it), 12 rounds of relay-write concurrent with retention → 0 dead letters, `SYNC_ACK_SLACK_SEQ` clamps to ≥200 even via test overrides.

### Six gates — no regression

```
golden sqlite      ✅ 64 步与基线完全一致    pytest sqlite     635 passed, 14 skipped
golden postgres    ✅ 64 步与基线完全一致    pytest postgres   637 passed, 12 skipped
unittest sqlite    Ran 157 — OK (skipped=14)  unittest postgres Ran 157 — OK (skipped=12)
```
(Cited only as "nothing broke". Per the brief, they are zero evidence for this phase — the 64 steps never touch `/api/v1/*` and never drop a partition.)

---

## THREE FINDINGS

**F1 — CONTRACT GAP, ship-blocking for the Walmart side. §7 defines no recovery from a 409, and the obvious choice deadlocks permanently.** §7 says `ALARM; full_reconcile(); STOP` but never says what `stored_cursor` becomes, while the loop head is an unconditional `X = max(0, stored_cursor - OVERLAP)`. Measured after a real forced prune (`min_available_seq=5001`):

```
 stored_cursor  说明                                第 1 轮
          5000  min_available_seq - 1（最自然的选择）   X=4800 409 cursor_below_retention  <<< 永久卡死
          5001  min_available_seq                   X=4801 409                          <<< 永久卡死
          5199  min_available + OVERLAP - 2（差一）    X=4999 409                          <<< 永久卡死
          5200  min_available + OVERLAP - 1         X=5000 200 收300条 首=5001   ← 唯一正确
          5201  min_available + OVERLAP             X=5001 200 收299条 首=5004   ← 静默跳过 5001
```
20 repeat cycles at 5000: all 409, cursor never moves. The **only** correct value is `min_available_seq + OVERLAP - 1`; one off in either direction is a permanent stall or a silently skipped record. Same trap hits a **brand-new consumer**: burn 37 seqs in a rolled-back first relay batch, `min(seq)=38`, retention never ran, and §7's `stored_cursor=0` → `X=0` → 409 forever. Fix is documentation, not code: add the reposition formula to §7 and §2.6.

**F2 — the forced-prune latch reports `est_rows` as 0 for partitions that destroyed hundreds of rows.** `_collect_partitions` uses `GREATEST(c.reltuples,0)`, which is 0 until ANALYZE. Real latch from the consumer-downtime run:
```
 id               区间    闩锁记的 est_rows   区间内真实条数
  1      [1174,1551]                0            251
  2      [2452,2630]                0            172
  …                                 …             …
  est_rows 与真实条数不符的条目: 6/6
```
`from_seq`/`to_seq`/`ack_seq_at_time` are exact (664/664 lost seqs covered), so the account is recoverable — but the one field an operator reads as "how much did we lose" says **0**. Confirmed stale-vs-real directly: `p4 est=200/real=296` before ANALYZE, `296/296` after. Same `reltuples` feeds `_hard_floor`'s `SYNC_MAX_EVENT_ROWS` gate, so that gate under-counts on un-analyzed partitions. Suggest `count(*)` on the partition just before `DROP` (it's about to be dropped; the scan cost is one-off and this is a data-loss record).

**F3 — `/status` advertises a floor computed by a different formula than the pass enforces.** `retention_observe()` passes `hard_floor=_hard_floor(...)` into `combine_floors`; `_retention_pass_locked()` passes `hard_floor=0` and handles emergencies iteratively via `_pressure()`. Under a row cap they disagree, and `droppable_now` lists a partition whose removal is actually a forced prune above `ack_seq` with no marking:
```
场景               /status.effective_floor  pass.soft_floor  /status.droppable_now      演练真会裁
无压力                              4801             4801     p0,p1,p2,p3            p0,p1,p2,p3
行数上限 = 100                      5299             4801     p0,p1,p2,p3,p4         p0,p1,p2,p3,p4!   (! = 越过 ack)
```
Directionally consistent, no data-safety impact, but the operator-facing number is not the enforced one. Minor: `_pressure`'s disk hysteresis latch lives in `self._rt()`, so a restart mid-drawdown stops at `floor` instead of continuing to `target` (measured with a pinned disk reading: same instance `'disk_target'`, fresh instance `None`) — thrash, not loss.

## Two things I checked rather than assumed

- The Phase 3 `_guard` docstring promises Phase 6 will replace `min(seq)` with "the highest actually-pruned seq" and drive the false-positive rate to zero. **Phase 6 did not do this** — `forced_prune_log` records only *forced* prunes, and `_guard` still compares against `min(seq)`. In practice P6-5 makes it moot for normal pruning (0 false 409 in 9,204 records), so I'd close the promise as satisfied-by-other-means rather than reopen it — but F1's bootstrap case is the residue of exactly that gap.
- The previous agent's flagged `/ack-prune` gen concern: I re-confirmed it is not a defect, and additionally that a stale gen 409s while the current gen clears the latch (`a4_run.sh` ackbad/ackone/ackall).