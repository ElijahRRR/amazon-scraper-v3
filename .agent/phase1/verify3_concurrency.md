Repo untouched (`git status` clean). All probes live in `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/probes/` and each runs both backends in one process for direct comparison. Run any of them with:

```
PYTHONPATH=/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad:/home/user/amazon-scraper-v3 \
  /home/user/amazon-scraper-v3/.venv/bin/python <probe>.py
```

---

# CONCURRENCY REVIEW — 3 NEW races reproduced, 1 of them a permanent outage

The port kept `_write_lock` as a real lock and `_db` as one connection (D-2), so **the classic queue races are genuinely closed** — I could not produce a double dispatch, a lost lease, or a deadlock. Every race I found comes from one root cause the SQLite design never had to think about: **in PostgreSQL a transaction is sticky connection state, and any error inside it poisons everything else on that connection.** The `_op_lock` fix makes concurrent use of `_db` *not raise* — but it also makes concurrent use *share a transaction*, and that is where the damage is.

---

## F1 — NEW — BLOCKER. An unlocked read that fails destroys a concurrent locked write transaction

`ConnProxy._op_lock` serializes statements but not transactions, so an unlocked statement on `_db` executes **inside** whatever transaction a lock-holding coroutine currently has open. In SQLite that is harmless. In PG, if that statement errors, the writer's transaction is aborted.

The six unlocked `db._db` reads in app.py (1298, 2230, 2281, 2289, 2294, 2309) plus `list_callback_due` are all reachable this way.

`probes/p2_nul.py` direction A — attacker is verbatim `server/app.py:2281`, reached by `DELETE /api/results` with `{"search": "...\u0000..."}`; victim is `accept_results_batch` of 6 results:

```
===================== sqlite =====================
  [solo] app.py:2281 with NUL in search -> OK (no error)
  [concurrent] intruder raised: [] x0
  [concurrent] accept_results_batch -> {'ok': {'accepted': 6, 'stale': 0, 'failed': 0}}
  [after] tasks done=6 processing=0 pending=0 | asin_data rows=6
  [VERDICT] batch of 6 results: intact

===================== postgres =====================
  [solo] app.py:2281 with NUL in search -> CharacterNotInRepertoireError: invalid byte sequence for encoding "UTF8": 0x00
  [concurrent] intruder raised: ['CharacterNotInRepertoireError', 'InFailedSQLTransactionError'] x3
  [concurrent] accept_results_batch -> {'exc': 'InFailedSQLTransactionError: current transaction is aborted, ...'}
  [after] tasks done=0 processing=6 pending=0 | asin_data rows=0
  [VERDICT] batch of 6 results: LOST
```

A read-only request silently deleted a worker's entire result batch and left the tasks stuck in `processing`.

**NUL is not the only trigger, and it is not the scary one.** `probes/p3_cancel.py` uses cancellation instead — uvicorn/Starlette cancels the request coroutine when the HTTP client hangs up, and every one of those six reads sits in a request handler:

```
===== backend=sqlite =====
  client-disconnect cancellations: 0
  accept_results_batch -> {'ok': {'accepted': 6, 'stale': 0, 'failed': 0}}
  [after] done=6 processing=0 pending=0 | asin_data rows=6

===== backend=postgres =====
  client-disconnect cancellations: 2
  accept_results_batch -> {'exc': 'InFailedSQLTransactionError: current transaction is aborted, ...'}
  [after] done=0 processing=6 pending=0 | asin_data rows=0
```

Caveat I should be explicit about: to make the read slow enough to cancel mid-flight I used `pg_sleep` inside the statement. The mechanism (cancel → `57014` inside the tx → abort) is real and `command_timeout=60` reaches it the same way, but the *timing* of that particular demo is synthetic. The NUL variant above needs no timing at all.

**`PG_STRIP_NUL=1` does not fix this** — app.py builds its own params (`f"%{term}%"`) and passes them straight to `ConnProxy.execute`; they never touch `text_affinity`. Verified:

```
PG_STRIP_NUL active: True
  raw app.py:2281 with NUL -> CharacterNotInRepertoireError (NOT protected)
```

**Narrowing, in fairness:** a *client-side* asyncpg `DataError` (bad param type, e.g. `{"batch_id": "1"}` at app.py:2289) does **not** poison — it raises before anything reaches the wire (`probes/p1_poison.py`: 44 intruder DataErrors, victim still `{'accepted': 6}`). Only server-side errors and cancellations do.

---

## F2 — NEW. The writer's own error leaks to the background dispatcher as `InFailedSQLTransactionError`

Mirror image, same connection. `probes/p2_nul.py` direction B: a scraped title containing NUL (the documented open `PG_STRIP_NUL` decision) kills `accept_results_batch`, and the concurrent `list_callback_due` — the real `_timeout_task_loop` fallback scan — gets 25P02 instead of rows, because it wins `_op_lock` ahead of the writer's `ROLLBACK`:

```
sqlite:   accept_results_batch -> {'ok': {'accepted': 6, 'stale': 0, 'failed': 0}}
          list_callback_due: ok=29 errors=[] x0
postgres: accept_results_batch -> {'exc': 'CharacterNotInRepertoireError: ...0x00'}
          list_callback_due: ok=28 errors=['InFailedSQLTransactionError'] x1
```

This raises the stakes on the NUL decision: it is not just "that one row fails", it is "that row takes down whatever else is on the connection".

---

## F3 — NEW — HARD BLOCKER. One request permanently wedges the entire write path

`reclaim_dead_worker_tasks` (`common/pgdb/tasks.py:310`), `release_tasks` (`:461`), `prioritize_batch` (`:476`) and six raw blocks in app.py (1252, 1304, 1505, 2237, 2316, 2654) issue `BEGIN` with **no `try/except: ROLLBACK`**. The shape is copied faithfully from `common/database.py` — so this is *equivalence-correct*. The problem is that in SQLite those statements essentially never fail, and in PG they fail constantly. When one does, the exception escapes `async with self._write_lock`, the lock is released, and `ConnProxy._tx` stays set forever. Every later `BEGIN` from every coroutine hits the shim's own guard.

`probes/p5_wedge_app.py` — verbatim `server/app.py:1500-1512`, payload `{"worker_id":"w","task_ids":["1"]}` (a JSON string, no exotic bytes, no auth):

```
===== backend=sqlite =====
  POST /api/tasks/release {'task_ids': ['1']} -> 200, released=1
    [after] create_batch               -> OK 2
    [after] pull_tasks                 -> OK 1 tasks
    [after] accept_success_result      -> OK {'accepted': True, 'saved': True}
    [after] reclaim_dead_worker_tasks  -> OK 4

===== backend=postgres =====
  POST /api/tasks/release {'task_ids': ['1']} -> 500 DataError: invalid input for query argument $2: '1' ('str' object cannot be interpreted as an integer)
    [after] create_batch               -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束
    [after] pull_tasks                 -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束
    [after] accept_success_result      -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束
    [after] reclaim_dead_worker_tasks  -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束
    [after] READS still fine: get_total_asins=0, get_batches=1
```

Reads keep returning 200, so health checks and dashboards stay green while every write 500s.

Confirmed end to end over real HTTP through `tests/golden/harness.isolated_server` (`probes/p8_http.py`) — same story, `/api/batches`, `/api/progress`, `/api/results` all 200 while `/api/upload`, `/api/tasks/pull`, `/api/tasks/result`, `/api/tasks/result/batch` are dead. `probes/p4_wedge.py` shows the same via `release_tasks` with a NUL `worker_id`, and confirms `_write_lock` is released, `ConnProxy._tx` is still set, and `is_in_transaction()` is `True`.

### F3 × F1 — the pure-race version: the request that dies is innocent

`probes/p9_compound.py` 9a. `release_tasks` is called with a **100% valid payload**; the only thing that goes wrong is that an unrelated read-only request failed while it was in flight:

```
----- 9a compound wedge  [sqlite] -----
  release_tasks (payload is 100% valid) -> {'ok': 30}
  [after] create_batch -> OK 2

----- 9a compound wedge  [postgres] -----
  release_tasks (payload is 100% valid) -> {'exc': 'InFailedSQLTransactionError: current transaction is aborted, ...'}
  [after] create_batch -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束  *** WEDGED ***
```

This is why the golden harness cannot see any of it: it no-ops all four background loops and `TestClient` is strictly sequential, so a second in-flight operation never exists.

---

## F4 — PRE-EXISTING, not a phase blocker. Cancellation mid-transaction wedges both backends

The raw `BEGIN` paths catch `Exception`, not `BaseException`, so `CancelledError` escapes without rollback. `common/pgdb/batches.py` uses `_tx()` which *does* catch `BaseException` — the codebase is inconsistent, but so is `common/database.py`. `probes/p10_cancel_tx.py`:

```
===== backend=sqlite =====
    [after] create_batch -> OperationalError: cannot start a transaction within a transaction
===== backend=postgres =====
  ConnProxy._tx still set? True   server tx open? False
    [after] create_batch -> RuntimeError: 嵌套 BEGIN：上一个事务还没结束
```

Both wedge → pre-existing → does not block this phase. Worth noting for whoever fixes F3: on PG the **server side already rolled back** (`is_in_transaction() == False`); only the shim's `_tx` slot is stale. A one-line reconciliation in `ConnProxy` would make PG strictly better than SQLite here at zero behavioural risk to the SQLite path.

---

## F5 — PRE-EXISTING. Batch completion detection vs result submission

`get_batch_completion_status` (read pool, two statements, two snapshots) → `mark_batch_completed` (CAS, no re-check). A task created between them leaves the batch `completed` with open work. `probes/p9_compound.py` 9b — **byte-identical on both backends**:

```
sqlite:   marked completed at: [{'total': 20, 'done': 20, 'failed': 0, 'open': 0}]
          final batch status='completed'  final tasks={'pending': 1, ..., 'total': 21}
          batch is 'completed' while 1 tasks are still open -> True
postgres: (identical)
```

Not a port regression. `expand_batch_variants` and `/api/batches/{name}/retry` both reach it.

---

## F6 — multi-process only, out of Phase 1 scope, but the comment overstates the guarantee

`_save_result_inner_unlocked` is SELECT-then-INSERT with no advisory lock (`results_write.py` already flags this for Phase 1.5). Two `Database` instances writing the same new ASIN, `probes/p6_claim.py` 6e:

```
sqlite:   asin_data=21 (want 25) errors=['A OperationalError: database is locked', ...]
postgres: asin_data=25 (want 25) errors=['A UniqueViolationError: duplicate key ... "asin_data_asin_key"']
```

Both backends break under multi-process writers; PG at least didn't silently lose 4 rows. Single-process deployment is unaffected.

---

## What I tried to break and could not — verified clean

| Check | Result |
|---|---|
| **Task claim, 4 coroutines, one instance** (6a) | 60 pulled / 60 distinct / **0 double-dispatched**, both backends |
| **Task claim, 4 coroutines, two instances** (6b) | 60 / 60 / **0**, both backends |
| **`FOR UPDATE OF t SKIP LOCKED` actually works** (`p7_skiplocked.py`) | third connection locks tasks 1-5 → `pull_tasks` returns `[6,7,8,9,10]`, overlap `[]`, no blocking. The claim in `tasks.py` is true. (SQLite equivalent: `OperationalError: database is locked`.) |
| **Lease validation vs reclaim** (6c) | `accepted=1 stale=39 exc=0`, `done+stale==40` invariant holds — **identical numbers on both backends** |
| **`accept_results_batch` vs reclaim, two instances** (6d) | no deadlock, no `40P01`, no hang (60s bound) |
| **Connection/lock discipline drift** | AST diff of all 50 public methods: **0 real changes** in `_db` vs `read()` vs `_write_lock` choice. The 4 flagged diffs are `init_tables`/`maintenance_loop`/`wal_checkpoint` (intended) and one false positive from a comment in `list_callback_due` |
| **Lock-ordering cycle `_write_lock` ↔ asyncpg pool** | none — no path takes `read()` while holding `_write_lock`, and every `read()` context closes before `_write_lock` is acquired (app.py:1342, `_send_one_callback`). `_op_lock` never awaits `_write_lock`. |

---

## Bottom line

**F1, F2, F3 are NEW and introduced by the port; F3 blocks the phase.** None is a golden-sample diff, and none is reachable by the harness by construction — the harness disables the background loops and never has two requests in flight, which is exactly the precondition for all three.

Two of them are shim-level and fixable without touching `common/database.py` or the SQLite path:

- **F3** — `ConnProxy` knows the transaction is dangling; `_abort_dangling()` already exists but is only called from `close()`. Reconciling `_tx` against `is_in_transaction()` on `BEGIN` (or aborting a dangling tx there) turns a permanent outage into a single failed request. Note SQLite is *also* broken here (F4), so this is a strict improvement rather than a divergence — flagging it because "port bugs faithfully" would otherwise argue for leaving it.
- **F1/F2** — structural. As long as unlocked statements share the write connection with locked transactions, one failure is everyone's failure. The honest options are (a) route the seven unlocked reads to the read pool — a behaviour change from SQLite, since they'd stop seeing uncommitted data, or (b) make `_op_lock` cover the transaction rather than the statement — which changes the interleaving semantics the current comment deliberately preserves. This needs a decision, not a patch; I did not make it.

The NUL-byte question that was already open is now bigger than it looked: it is not only "does this row store", it is "does this row take down whichever transaction it lands in". F1 shows `PG_STRIP_NUL=1` would not even cover the app.py paths.