Cleanup done — `scraper_dev` intact, worktree removed, repo tree clean at `69886c6`.

## Verification 4 (adversarial) — results

**Gates (re-run at HEAD `69886c6`, unchanged tree):** golden sqlite `✅ 64 步与基线完全一致`, golden postgres `✅ 64 步与基线完全一致`, `pytest tests/ -q` → `427 passed, 6 skipped`, `DB_BACKEND=postgres` → `429 passed, 4 skipped`.

All probes live in `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v4/` and are re-runnable; every one drops its scratch DB.

---

# BLOCKER 1 — every partition from `p2` onward is unwritable. Already true on every fresh database.

`schema.event_next_partition_sql()` builds new partitions with `LIKE scraper.{template} INCLUDING ALL`, template = **the highest existing partition**. `INCLUDING ALL` implies `INCLUDING CONSTRAINTS`, which copies the template's own `_range` CHECK. `p0` is created with `PARTITION OF` and has no range CHECK, so `p1` is clean — but `p1 → p2` copies `scrape_events_p1_range`.

`e5_check_inherit.py`, production default `SPAN=20000000`, straight after `connect()`:

```
【1】连库(connect)之后，每个分区身上的 CHECK 约束：
    scrape_events_p0     scrape_events_marketplace_check CHECK ((marketplace = 'amazon.com'::text))
    scrape_events_p1     scrape_events_marketplace_check CHECK ((marketplace = 'amazon.com'::text))
    scrape_events_p1     scrape_events_p1_range     CHECK (((seq >= 20000000) AND (seq < 40000000)))
    scrape_events_p2     scrape_events_marketplace_check CHECK ((marketplace = 'amazon.com'::text))
    scrape_events_p2     scrape_events_p1_range     CHECK (((seq >= 20000000) AND (seq < 40000000)))   <-- 抄来的
    scrape_events_p2     scrape_events_p2_range     CHECK (((seq >= 40000000) AND (seq < 60000000)))

【3】往每个分区各插一行（seq 落在它自己的区间里）：
    p0 seq=1            写入 OK
    p1 seq=20000001     写入 OK
    p2 seq=40000001     **失败** CheckViolationError: new row for relation "scrape_events_p2" violates check constraint "scrape_events_p1_range"
```

Two contradictory CHECKs ⇒ `p2` accepts nothing, ever. The event stream stops permanently at **seq = 40,000,000**, and every partition created after that inherits the same poison.

**The failure mode is worse than the stall.** That exception is a `CheckViolationError` whose message does *not* contain `no partition of relation`, so `_is_row_fault()` returns `True` — the relay classifies healthy rows as poison pills. `e_partitions.py` with `SPAN=50`, real relay, real rows:

```
relay 批次失败（疑似队头毒丸），批量缩到 250：CheckViolationError: ... violates check constraint "scrape_events_p1_range"
...（500→250→…→1）
outbox 队头 id=81 已隔离进 scraper.scrape_outbox_dead（原因：relay 连续 5 次以单行批量失败）
outbox 队头 id=82 已隔离进 scraper.scrape_outbox_dead（原因：relay 连续 5 次以单行批量失败）
```

Perfectly good events get moved to the dead-letter table one at a time, forever. That is silent data loss from the consumer's point of view, not the designed loud stall.

`_create_partition`'s hard gate only asserts the `source_id` unique index — it does not look at CHECK constraints, so it passes. And `tests/pgdb/test_relay.py::test_partition_rollover_keeps_two_future_partitions` verifies bounds contiguity and index inheritance but **never writes a row into a newly created partition**, which is why this survived.

Fix is one word (`INCLUDING ALL EXCLUDING CONSTRAINTS` — indexes come from `INCLUDING INDEXES` separately, and the parent's `marketplace` CHECK is inherited at ATTACH anyway), plus extending the existing gate to assert exactly one `_range` CHECK.

---

# BLOCKER 2 — a rolling restart leaves **no** relay running at all

`start_event_relay()` returns `False` once, `run_event_relay()` returns, the lifespan task completes, and nothing ever retries. The code comment calls losing the lock "滚动部署时的正常状态" — but the new instance then never starts a relay for the rest of its life. `a_singleton.py` A3, driving the real `run_event_relay()` entrypoint:

```
  t0: OLD relay running
  t1: NEW.run_event_relay() 已 create_task；NEW.state=refused task.done=True
  t2: OLD 停机。库上持锁数=0
  t3: 等 4s 之后 NEW.state=refused task.done=True
      库上持锁数=0  {'outbox': 6, 'events': 0, 'dead': 0, 'dup_source_id': 0}
  >>> 滚动重启之后还有 relay 在跑吗？ 否 —— 事件流已死，outbox 只涨不落
```

Same class, second trigger — a single transient connect failure at boot (`h4_boot.py`):

```
  lifespan 记了一条日志: PostgresConnectionError: connection refused
  _relay_open_conn 被调用 1 次；task.done=True  relay_state='failed'
  6 秒后（库早就好了）: {'outbox': 20, 'events': 0, ...}  持锁数=0
  >>> 一次瞬时连库失败 = 本进程余生没有事件流: True
```

In both cases HTTP stays green and the outbox grows without bound. `start_event_relay` needs a retry loop around the refused/failed paths.

---

# HIGH 3 — the relay never reconnects, and reports `running` while dead

`a_singleton.py` A2 / `a5_mechanism.py` A6 — `pg_terminate_backend` on the relay's own backend:

```
  pg_terminate_backend(26667) -> True
  杀掉后库上持锁数  : 0
  A.relay_state     : running   task.done=False
  A 计数器 tick_errors=2 relayed=5
  >>> 5 条 post 行还在 outbox 里？ True
  B.start_event_relay() 现在 -> True  state=running     <- 另一个实例可以接管
  A 仍自称          : running
```

No rows lost (good), but the loop spins on a closed connection forever. The observability endpoint:

```
  健康时 : relay_state='running' outbox_depth=0  max_seq=3 tick_errors=0
  死掉后 : relay_state='running' outbox_depth=40 max_seq=3 tick_errors=2
           relay_lag_s=6.04  events_per_minute=27.2
  >>> 端点把一条已经死掉的 relay 报成 'running'
```

`outbox_depth` / `relay_lag_s` do tell the truth (they're re-read via the pool), but `relay_state` and `events_per_minute` both lie. The integration report specifically added `failed` so that "died" and "stopped" are distinguishable — this path bypasses it, because the death is inside `_relay_tick`, which is swallowed and backed off.

---

# HIGH 4 — the D-26 start/stop race fix has a hole

`start_event_relay` unconditionally does `st["stopping"] = False` before its first `await`. If `stop` lands *before* the start coroutine's first statement runs, that flag is wiped and the leak D-26 fixed comes back. `a_singleton.py` A4 sweeps the delay:

```
  delay=0.0     A.state=running   A.relay_conn=有   停机后残留持锁=1  B 能接管=False
  delay=0.0005  A.state=stopped   A.relay_conn=无   停机后残留持锁=0  B 能接管=True
  delay=0.002   A.state=stopped   A.relay_conn=无   停机后残留持锁=0  B 能接管=True
  delay=0.01    A.state=stopped   A.relay_conn=无   停机后残留持锁=0  B 能接管=True
```

Counterfactual (`a5_mechanism.py`) isolates the cause to that one line — the only change is refusing to start when `stopping` is already set:

```
  现状(HEAD)                   残留持锁=1  A.state=running  B 能接管=False
  反事实(不重置 stopping)          残留持锁=0  A.state=stopped  B 能接管=True
```

D-26's own test covers `delay > 0` only. The tested window is the wrong side of the race.

---

# HIGH 5 — a real DB restore is **not** detected as a rewind

T11(a) — "只回滚 DB：启动检出回退 → 铸新 gen" — does not hold for a consistent whole-database backup, because `sync_meta.max_seq_ever` is restored along with `scrape_events`, so `actual < ever` is never true. `f_gen.py` F4, with a real `pg_dump -Fc` / `pg_restore`:

```
  备份前: max(seq)=10  gen=8b0bc0cbe27a  max_seq_ever=10
  pg_dump rc=0
  备份后又产出到 max(seq)=25（消费者的游标已经推到这里）
  pg_restore rc=0
  恢复后: max(seq)=10  max_seq_ever=10  gen=8b0bc0cbe27a
  rewinds_detected=0  gen_minted=0
  >>> gen 变了吗？ False
  >>> 消费者游标停在 25，而流现在只到 10：seq 11..25 会被**第二次**发出去，内容不同、gen 相同
```

The detector only fires on an *inconsistent* restore — `scrape_events` truncated while `sync_meta` survives (F5, which does work: `事件流倒退：max(seq)=0 < max_seq_ever=8 ... 铸新 gen`). That is the case where the operator already knows. The one that silently corrupts a consumer is the one that goes undetected. `max_seq_ever` has to live somewhere the restore can't roll back with the data, or the consumer-side `max_seq` monotonicity check has to be the documented sole defence for DB-only rollback too (not just T11b).

---

# MEDIUM 6 — transient non-row faults dead-letter healthy events

`_is_row_fault` treats any `PostgresError` outside `_NOT_ROW_FAULT` as a row fault. `b6_quarantine.py`:

```
  _is_row_fault(QueryCanceledError)  = True     <- "canceling statement due to statement timeout"
  _is_row_fault(DeadlockDetectedError)= True
  _is_row_fault(asyncio.TimeoutError) = False
```

`PG_COMMAND_TIMEOUT=60` produces exactly `QueryCanceledError`. Injecting 24 of them against six flawless rows:

```
  {'outbox': 2, 'events': 0, 'dead': 4, 'dup_source_id': 0}
  死信: id=1 sid='GOOD:0' 原因='relay 连续 5 次以单行批量失败'
  死信: id=2 sid='GOOD:1' ...
  死信: id=3 sid='GOOD:2' ...
  死信: id=4 sid='GOOD:3' ...
```

A sustained slow-DB episode drains the head of the queue into `scrape_outbox_dead` at one event per ~5 ticks. The rows are preserved for manual replay, but they never reach `scrape_events`, so the consumer has no way to know.

---

# What held up under attack

**Crash matrix (`b_crash.py`) — zero loss, zero duplicates at all five points.** After DELETE/before INSERT, mid-INSERT (half the batch written), after INSERT/before COMMIT, COMMIT-landed-but-ack-lost, and a real `pg_terminate_backend` mid-COMMIT. Every scenario reconciles `outbox ∪ dead ∪ events` against the full expected source_id set:

```
B1 认领(DELETE)之后、INSERT 之前崩   丢失: 无   重复: 无
B2 INSERT 中途崩（半批已写）          丢失: 无   重复: 无
B3 整批 INSERT 成功、COMMIT 之前崩    丢失: 无   重复: 无
B4 COMMIT 真落库但 ack 丢失           丢失: 无   重复: 无
B5 COMMIT 途中 pg_terminate_backend   杀掉之后: outbox=20 events=0 dead=0  丢失: 无  重复: 无
```

B4 is the one that could have broken at-least-once: the relay believes it failed while the DELETE has already committed, so the rows are simply gone from the outbox and never re-claimed — no duplicate. `_is_row_fault(asyncio.TimeoutError) = False`, so it doesn't trigger quarantine either.

**Atomicity, write→outbox direction (`c_atomicity.py` C1).** Failure injected after the hook: outbox unchanged, `asin_data` empty, task back to `processing`, and the write connection is *not* poisoned — the next submit succeeds.

**Atomicity, outbox→write direction (C2/C3).** Confirmed by design and worth writing into the contract: an event-stream fault inside the caller's transaction destroys the real scrape. Dropping `scraper.scrape_outbox` makes `accept_success_result` raise and the collection is lost; an `emit` failure on the B3 path takes the *whole batch* of good results with it. The write connection itself recovers cleanly in both cases. The stale-path own-transaction (C4) correctly swallows its failure and preserves the golden-pinned `{'accepted': False, 'stale': True}`.

**Backlog (`d_backlog.py`).** 12 000 rows accumulated with no relay, then started:

```
  抽干 12000 条耗时 1.27s -> {'outbox': 0, 'events': 12000, 'dead': 0, 'dup_source_id': 0}
  抽干期间心跳跳了 24 次（19/s，上限 20/s）—— 事件循环没被饿死: True
  事件 12000 条  seq 严格递增=True  seq 空洞段数=0
  outbox id 顺序 == seq 顺序: True   丢失=0
```

**Poison-pill isolation converges** (`b7_long.txt`): 15 batch-shrinks, the *correct* row quarantined, stream resumes, all 6 healthy rows delivered — `死信表: [{'id': 4, 'sid': 'poison:1'}]`, `事件: ['pre:0','pre:1','pre:2','post:100','post:101','post:102']`.

**Singleton race + within-partition dedup.** Simultaneous `asyncio.gather` start: exactly one wins, one advisory-lock holder, 200 rows relayed with strictly increasing seq and zero duplicate source_ids. Duplicate `source_id` **inside one INSERT statement** is correctly swallowed by the untargeted `ON CONFLICT DO NOTHING` (`e_partitions.py` E3: 3 rows in, `RETURNING` 2, 1 row in the table). Overflow past the top partition behaves exactly as specified (E4): `_is_row_fault = False`, all 4 rows stay in the outbox, loud stall.

**Cross-partition `source_id` uniqueness does not hold** — confirmed, as documented in §3.1, not a new finding:
```
  同一分区内重复 source_id: 被拒 -> UniqueViolationError
  **另一个分区**里的同一个 source_id: **被接受**
```

**The singleton lock is genuinely load-bearing** (`g_misc2.py` G1b, deterministic). Two relays, A gets seq=1 uncommitted, B gets seq=2 and commits first:
```
  消费者 poll#1 (after_seq=0) -> [(2, 'Z:1')]   游标推到 2
  relay A 现在提交了
  消费者 poll#2 (after_seq=2) -> []
  库里共 2 条；消费者永远看不到的 seq = [1]
  >>> 单例锁一旦失效，已提交的行被**永久跳过**: True
```
This is why BLOCKER 2 matters in the safe direction (stall) and why nothing should ever weaken the lock.

**`gen`** (`f_gen.py`): stored per row ✅, `source_id` cannot collide across gens ✅ (`{gen}:{uuid4}`, prefix always equals the row's gen). A restart **does not** mint a new gen — `gen_minted = 0`, same value before and after. That is D-22 working as documented; flagging it only because the task brief asked me to confirm the opposite.

**No F1/F3 regression on the new code paths** (`h_f1f3.py`). Cancelling mid-`_emit_outbox` (inside the main transaction), mid-`emit_stale_event_own_tx` (the second transaction), and mid-`task_facts`: in all three, `ConnProxy._tx` is cleared, no server-side transaction remains, the D-15 abandoned-transaction reclamation logs and fires, the task rolls back to `processing`, no orphan outbox row, and the next submit succeeds. Phase 1 probes re-run at HEAD: `p1_poison` (6/6 accepted under 48 concurrent DataErrors), `p3_cancel`, `p4_wedge` (no wedge, no leaked tx), `p10_cancel_tx` (PG recovers), `p11_dirty` (`dirty-read shapes: NONE`), `p9_compound`, `p17_harm` — all unchanged. `p15_leak` hangs, but only in its `OPTB`/`OPTC` modes, which are rejected Phase-1 prototype patches; its `BASELINE` case passes, and it hangs identically at `e21e2c6` (verified in a worktree), so it is not a regression.

---

# Two measurements for Phase 3

**`recorded_at` is not monotonic with `seq`** (`g_misc2.py` G2b). `enqueued_at` is `now()` = transaction start, and the relay only sees committed rows, so a late-committing early transaction lands at a higher seq with an earlier timestamp:

```
    seq=1  LATE-enqueued    recorded_at=2026-08-05 06:05:46.728058+00:00
    seq=2  EARLY-enqueued   recorded_at=2026-08-05 06:05:46.224734+00:00
  >>> recorded_at 随 seq 单调: False
```

`recorded_at` is the only index on `scrape_events`. Any consumer that uses a time window as a cursor will skip rows. The contract must say `seq` is the only valid cursor.

**Write-path cost: about +24%.** Same machine, idle, `accept_results_batch` with 200 items, best of 3, alternating trees:

```
Phase1 e21e2c6   最快   924.8 ms (4624 µs/条)   事件流=False
Phase2 69886c6   最快  1056.9 ms (5285 µs/条)   事件流=True  outbox=600
Phase1 e21e2c6   最快   842.0 ms (4210 µs/条)
Phase2 69886c6   最快  1112.5 ms (5563 µs/条)
Phase1 e21e2c6   最快   782.7 ms (3914 µs/条)
Phase2 69886c6   最快   968.4 ms (4842 µs/条)
```

Roughly +1 ms per result, all of it inside `_write_lock`, so it comes straight off peak submission throughput (~220/s → ~180/s). Not a defect — the outbox INSERT plus the `task_facts` SELECT are both required by the design — but it should be a known number rather than a surprise.