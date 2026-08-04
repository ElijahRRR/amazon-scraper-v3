Repo is clean, golden still 64/64 on both backends, no leaked scratch databases. Here are my findings.

## CONFIRMED DRIFTS

### 1. `DELETE /api/results` with a backslash in `search` deletes the **wrong rows** — silently

`server/app.py:2274` builds the pattern as `f"%{term}%"`. `pool.translate_sql` rewrites the *operand* (`d.title LIKE ?` → `ascii_lower(d.title) LIKE ascii_lower(?)`) but never touches the *pattern*, so PG's default `\` escape applies where SQLite's LIKE has no escape character at all. `pool.py:190` claims this rewrite "covers server/app.py:2274" — it covers case-folding, not escaping.

Rows: `B0BSL00001` title `back\slash`, `B0BSL00002` title `backslash`.

```
DELETE /api/results  {"search": "back\\slash"}
  sqlite   -> {"deleted":1}   remaining: B0BSL00002, 3, 4   (deleted the back\slash row — correct)
  postgres -> {"deleted":1}   remaining: B0BSL00001, 3, 4   (deleted the backslash row — wrong)

DELETE /api/results  {"search": "\\"}
  sqlite   -> {"deleted": 2}      postgres -> {"deleted": 0}
```

Both return `deleted:1`, so the caller cannot detect it. 3/3 reproducible. Note PG is also self-inconsistent: `GET /api/results?search=back\slash` returns `B0BSL00001` on both backends (`results_read._like_pattern` doubles backslashes correctly) — only the DELETE disagrees.

### 2. Payloads SQLite rejects are silently accepted by PG

`pool.text_affinity()` ends in `str(v)` for unrecognized types and has no int-range check. `POST /api/tasks/result` is unvalidated `await request.json()`.

| `review_count` value | sqlite | postgres |
|---|---|---|
| `["a","b"]` | `ProgrammingError` → **500**, nothing saved | **200**, stores `"['a', 'b']"` |
| `{"k":"v"}` | `ProgrammingError` → **500** | **200**, stores `"{'k': 'v'}"` |
| `9223372036854775808` | `OverflowError` → **500** | **200**, stores `'9223372036854775808'` |

Blast radius on `/api/tasks/result/batch` — one poisoned item among six:
```
sqlite:   HTTP 500, whole transaction rolled back. /api/progress -> done 0, processing 7, completion_rate 0.0
postgres: HTTP 200 {"accepted":6}.                 /api/progress -> done 6, processing 1, completion_rate 85.7
```

### 3. `-0.0`, `NaN`, `Infinity` are reachable from JSON — the code says they aren't

`pool.py:118` states these are *"JSON 到不了，记录备查"*. That's wrong: Python's `json.loads` (what `request.json()` uses) accepts `NaN`/`Infinity`/`-Infinity` literals, and `-0.0` / `1e400` are ordinary JSON numbers.

| sent | sqlite stores | postgres stores |
|---|---|---|
| `-0.0` | `'0.0'` | `'-0.0'` |
| `NaN` | `null` | `'nan'` |
| `Infinity` / `1e400` | `'Inf'` | `'inf'` |

Visible in `/api/results/{asin}`, `/api/results`, and CSV export. `content_hash` is unaffected (computed in Python pre-bind), so the divergence is invisible to change detection.

### 4. `/api/batches/{name}/errors` returns a different row **set**, not just order

`app.py:1385` — `ORDER BY updated_at DESC LIMIT 200`, no tiebreaker, and `updated_at` is second-resolution: `accept_results_batch` stamps every item in one submit with the same `now`. 260 tasks failed in a single batch submit:

```
sqlite   failed_tasks[0]=B0TIE00260 ... returns ids 61-260
postgres failed_tasks[0]=B0TIE00001 ... returns ids 1-200
60 of the 200 rows differ.   3/3 reproducible.
```

Sharp contrast: `/api/batches/{id}/failures` (`get_batch_failures`) has `ORDER BY updated_at DESC, id DESC` and the port additionally added `NULLS LAST` — byte-identical on both. The inline duplicate of that query in app.py was left alone, and since D-4 forbids forking app.py the shim can't reach it. Same class: `app.py:1378` (`GROUP BY error_type ORDER BY cnt DESC`, already deferred in OWNERSHIP.md) and `app.py:341` (`ORDER BY updated_at DESC LIMIT 30`, background completion rescan).

### 5. `get_pending_screenshots` — `LIMIT` with no `ORDER BY` (latent)

`SELECT * FROM screenshots WHERE batch_id=? AND status='pending' LIMIT ?`. SQLite scans in rowid order; PG in heap order, which shifts once rows are UPDATEd. 20 screenshots, 8 churned done→pending, `limit=5`:
```
sqlite -> [S001,S002,S003,S004,S005]      postgres -> [S009,S010,S011,S012,S013]
```
It's in `PUBLIC_API` but has no caller in `server/` or `worker/` today, so it is latent rather than live.

### 6. `create_batch(None)` — sqlite `0`, postgres `NotNullViolationError`

Same `INSERT OR IGNORE`-swallows-NOT-NULL class that OWNERSHIP.md documents for `create_tasks`, but `create_batch` isn't listed. Not reachable from app.py today (`batch_name` always falls back to a generated string).

## Areas I attacked and could NOT break

Recording these so nobody repeats the work:

- **LIKE / search paths** — 45 probes end-to-end through `/api/results?search=` (ASCII + Unicode case, Cyrillic/Greek/Turkish-dotless/full-width, `%`/`_`/backslash, comma multi-term, both the <3 and ≥3 char branches): 0 diffs. I also independently validated the premise the port rests on — SQLite's FTS5 **trigram** fast path and the plain-LIKE slow path return identical row sets (trigram folding is ASCII-only, so `'CAFÉ'` does not match `'%café%'` on either path). Replacing FTS5 with `ascii_lower + LIKE` is sound.
- **Schema** — column set, order, nullability, defaults, PK/UNIQUE constraints match across all 7 tables.
- **identity burning on conflict** — 3 new / 3 all-conflicting / 4 mixed produce ids `1,2,3,8,9` on both. The `unnest(...) ON CONFLICT DO NOTHING` translation preserves burn exactly.
- **rowcount semantics** — 15 probes covering no-op updates, missing row, missing batch, unknown task/lease, `"0 rows matched"` vs `"no such row"`: identical.
- **NULL ordering** — the port added `NULLS LAST/FIRST` in 5 places. I could not construct a reachable NULL in any of those sort keys (every writer sets `updated_at`; DDL defaults cover the insert paths). Defensive, not load-bearing.
- **Integer division / rounding** — all ratios computed in Python; PG returns `int` (not `Decimal`) for `COUNT`/`SUM` over integer columns. `get_progress`'s unknown-status extra key behaves identically.
- **Broad flows** — seller discovery (incl. case-dedup, empty/None asins, stale replay), callbacks (attempt/reset/due-scan/max-attempts), reclaim + auto-retry + fail + release + prioritize, change detection and `asin_changes`, `iter_results` export, `get_change_stats`: 0 diffs.
- **D-8's deliberately-preserved COUNT crash** reproduces identically — 500 for `search=Golden&cursor=3`, 200 for `search=Go&cursor=3`.

## Assessment

Findings 1 and 2 are the ones I'd gate on: #1 destroys the wrong data with no signal to the caller, #2 turns a hard 500 into silent acceptance of malformed data (and inverts the batch-atomicity contract). #4 is a real response-shape change for an existing consumer. #3 and #5 are lower-impact but #3 contradicts a comment that would stop a future reader from looking.

Notably, four of the six live in the raw SQL inside `server/app.py` rather than in `common/pgdb/`. The shim intercepts `?`→`$n` and the LIKE operand, but not pattern escaping and not `ORDER BY` determinism — so D-4 ("don't fork app.py") is exactly where the port's residual risk sits. The pgdb modules themselves held up against everything I could construct.

Repro scripts (all reusable, both backends): `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/` — `rig.py` (in-process dual-backend differ), `httprig.py` (HTTP-level differ over the golden harness), `scen_delsearch.py`, `scen_types.py`, `scen_batchpoison.py`, `scen_ties.py`, `t05_unstable.py`, `t06_types.py`.