All checks complete, tree clean. Here is verification 3.

---

# Verification 3 — non-regression & cheat audit of `fea7395..2e9c311`

**Verdict: no regression, no cheat.** Zero test node ids lost on either runner, the golden harness is byte-identical in all 7 files, `common/database.py` is untouched, and all five mutation families still bite. Four findings are reported at the end — none is a defect in the delivered work, but two of them are consumer-facing and one is a real coverage hole.

---

## 1. Four-runner matrix

**HEAD `2e9c311`** (`/tmp/…/scratchpad/v3audit/matrix_head.log`):

```
=== GATE 1: golden verify / sqlite ===        ✅ 64 步与基线完全一致          EXIT=0
=== GATE 2: golden verify / postgres ===      ✅ 64 步与基线完全一致          EXIT=0
=== GATE 3: pytest tests/ -q / sqlite ===     483 passed, 31 skipped, 1 warning in 167.00s   EXIT=0
=== GATE 4: pytest tests/ -q / postgres ===   485 passed, 29 skipped, 1 warning in 173.22s   EXIT=0
=== GATE 5: unittest discover -s tests / sqlite ===    Ran 79 tests in 0.676s   OK (skipped=31)   EXIT=0
=== GATE 6: unittest discover -s tests / postgres ===  Ran 79 tests in 3.987s   OK (skipped=29)   EXIT=0
```

**Baseline `fea7395`**, same venv, same box (`matrix_base.log`) — I re-measured rather than trusting the reports:

```
=== BASE GATE 1: golden verify / sqlite ===   ✅ 64 步与基线完全一致          EXIT=0
=== BASE GATE 2: golden verify / postgres === ✅ 64 步与基线完全一致          EXIT=0
=== BASE GATE 3: pytest / sqlite ===          427 passed, 6 skipped, 1 warning in 141.67s     EXIT=0
=== BASE GATE 4: pytest / postgres ===        429 passed, 4 skipped, 1 warning in 147.66s     EXIT=0
=== BASE GATE 5: unittest discover / sqlite ===   Ran 51 tests ... OK (skipped=6)             EXIT=0
=== BASE GATE 6: unittest discover / postgres === Ran 51 tests ... FAILED (errors=26, skipped=4)  EXIT=1
```

B6 independently reproduced at baseline (26 errors, postgres only, unittest only) and green at HEAD on all four cells.

## 2. Node-id accounting — 0 lost

```
pytest --collect-only     base/sqlite: 433   base/postgres: 433
                          head/sqlite: 514   head/postgres: 514
LOST (sqlite):   0        LOST (postgres): 0
ADDED, by file:  14 tests/pgdb/test_relay.py   39 tests/pgdb/test_sync_api.py
                  1 tests/test_delivery_parse.py  24 tests/test_long_description.py
                  3 tests/test_runner_parity.py           (= 81)

unittest discover ids     base=51  head=79   LOST: 0
ADDED: 1 test_delivery_parse · 24 test_long_description · 3 test_runner_parity   (= 28)
```

**Skip accounting**, +25 on both backends, fully attributed:

```
base sqlite skips (6)  -> head sqlite skips (31)
  new: tests/test_long_description.py  ×23   (selectolax / lxml 未安装)
       tests/test_delivery_parse.py     ×1   (the newly-added test in a skipUnless(_HAS_SLX) class)
       tests/test_runner_parity.py      ×1   (PytestOnlySuitesTests announcing the 292 pytest-only cases)
  every baseline skip is still present; postgres list = sqlite list minus the 2 pg-only skips
```

**Nothing was weakened or deleted.** `git diff fea7395..HEAD -- tests/` contains **zero** removed lines matching `assert|def test_|skipUnless|skipIf|pytest.mark.skip|xfail`; `tests/pgdb/test_relay.py` has 0 deleted lines. Exactly one skip marker was added to a pre-existing test — `@unittest.skipUnless(_HAS_DP)` on `test_date_as_direct_text_node` — and I reproduced that it was a **deterministic failure**, not a flake, before the gate (see §6).

## 3. Diff audit of the protected surface

```
tests/golden/README.md                     IDENTICAL   (fea7395 == HEAD == worktree)
tests/golden/__init__.py                   IDENTICAL
tests/golden/harness.py                    IDENTICAL
tests/golden/run.py                        IDENTICAL
tests/golden/samples/sqlite_baseline.json  IDENTICAL   blob 2f5e82b45b94bd482006b9e80a9cc228641e1e41
tests/golden/scenario.py                   IDENTICAL
tests/golden/test_golden.py                IDENTICAL
common/database.py                         IDENTICAL   blob d19e04bbf65bef1cd11c64cfcea7f0c89f90254d
files added/removed under tests/golden/:   (none)
```

Blob-level identity, so the question "did scenario.py/run.py gain backend conditionals" is answered structurally: they gained nothing at all.

**SQLite is still a genuine no-op for the event stream** — proven with an exploding `MetaPathFinder` rather than grep (`v3audit/sqlite_purity.py`): `DB_BACKEND=sqlite`, any import of `common.pgdb` or `asyncpg` raises, then the full 64-step verify runs in-process:

```
import server.app                 : OK, no banned import
✅ 64 步与基线完全一致
banned imports attempted during the whole run: (none)
common.pgdb in sys.modules : False
asyncpg     in sys.modules : False
golden verify return code  : 0
```

`server/app.py`'s new unconditional `include_router` adds exactly one route object and shadows nothing:

```
base routes: 64   head routes: 65
route-table diff: 64a65  > _IncludedRouter(...)          # the only delta
/openapi.json paths starting /api/v1 : []
sentinel — pre-existing /api/worker/sync present : True   total openapi paths: 51
SQLITE GET  /api/v1/sync/records -> 503   GET /status -> 503
       GET  /api/v1/sync/counts?from_seq=0&to_seq=10 -> 503   POST /ack -> 503
```

## 4. Mutation tests — the harness still has teeth

Phase-3 trio, run on a `git archive HEAD` copy, **both backends** (`v3audit/mutate.sh`):

```
CONTROL   sqlite 39 passed          postgres 39 passed
M-A  _snapshot repeatable_read -> read_committed     sqlite 2 failed/37   postgres 2 failed/37
M-B  delete the post-page lower-bound re-check       sqlite 2 failed/37   postgres 2 failed/37
       FAILED test_bounds_and_page_share_one_repeatable_read_snapshot
       FAILED test_the_post_page_recheck_is_load_bearing
M-C  _window -> naive COALESCE(...,0)                sqlite 4 failed/35   postgres 4 failed/35
```

I also ran the **relay** counterfactual the reports claimed but did not paste: HEAD's `tests/pgdb/test_relay.py` against **baseline** `common/pgdb/`:

```
11 failed, 32 passed          # 32 = 29 pre-existing + the 3 T-A/T-D/T-E guards, which pass at base by design
FAILED test_every_partition_carries_exactly_its_own_range_check     (B1)
FAILED test_every_partition_actually_accepts_a_row                  (B1)
FAILED test_seq_crossing_a_partition_boundary_lands_and_is_readable (B1)
FAILED test_a_partition_poisoned_by_the_old_recipe_gets_repaired    (B1)
FAILED test_partition_gate_rejects_a_contradictory_check            (B1)
FAILED test_retention_emptying_the_table_is_not_a_rewind            (B4)
FAILED test_seq_high_water_does_not_count_an_unused_sequence        (B4)
FAILED test_rolling_restart_ends_with_exactly_one_relay             (B2)
FAILED test_a_transient_connect_failure_at_boot_is_retried          (B2)
FAILED test_relay_reconnects_after_its_own_backend_is_terminated    (B3)
FAILED test_relay_steps_down_if_someone_else_took_the_lock_meanwhile (B3)
```

Not vacuous. Parser mutations are in §6.

---

## 5. The parser change: exactly what `long_description` now contains

13-case corpus through the **real engines** (isolated `--target` overlay; the shared venv was never touched and is still `selectolax: False lxml: False dateparser: False`). Probes: `v3audit/ld_cases.py`, `ld_probe.py`, `ld_report.py`, `ld_loss.py`, `e2e_parse.py`.

```
case                       slx changed  lxml changed  AFTER slx==lxml
aplus_modules              True         False         True
br_and_text                False        False         True
container_is_last          False        False         True
empty_container_jsonld     True         False         True
inline_markup              True         False         True
mixed_content_prefix       True         False         True
nested_deep                True         False         True
plain_text_container       True         False         True
realistic                  True         False         True
realistic_price_changed    True         False         True
short_leaves               False        False         True
table_inside_container     True         False         True
wrapper_a_span             True         False         True

BEFORE cross-engine agreement: 3/13      AFTER: 13/13
```

**The lxml path is bit-identical in 13/13 cases** — the fallback engine's output did not move at all. Only the selectolax (production) path changed, and every changed value now equals lxml exactly.

Full `parse_product()`, production engine, on a realistic page:

```
--- BEFORE (fea7395) ---
{"long_description": "The M80D is a 4K smart monitor that doubles as a\nstreaming TV. Slim, matte finish, built for any desk.\n$549.99\nIn Stock\n2431 ratings\n\n[Image: https://m.media-amazon.com/images/I/71xRPjIS8LL._AC_SL1500_.jpg]\n\nManufacturer\nSamsung Electronics\nBest Sellers Rank\n#312 in Electronics", "price": null, "stock_status": "In Stock", "rating": "N/A"}
--- AFTER  (HEAD) ---
{"long_description": "The M80D is a 4K smart monitor that doubles as a\nstreaming TV. Slim, matte finish, built for any desk.", "price": null, "stock_status": "In Stock", "rating": "N/A"}
```

No other field moved. `slow_hash` consequence:

```
  BEFORE slx   549.99=v1:93326eff…  429.00=v1:7567a418…  STABLE=False
  AFTER  slx   549.99=v1:d23c9894…  429.00=v1:d23c9894…  STABLE=True
  BEFORE lxml  STABLE=True    AFTER lxml  STABLE=True
cross-engine on the identical page: BEFORE AGREE=False   AFTER AGREE=True
```

I re-derived the mechanism from selectolax 0.4.11 directly rather than taking it on report:

```
container.traverse() ->   div 'Bullet lead text bullet tail textn'
                          ul  … li … span …
                          div 'FOREIGN $549.99'      <- OUTSIDE the container
                          span 'FOREIGN $549.99'     <- OUTSIDE the container
li.iter()            -> [('span', …)]                 # direct children only
li.iter(include_text=True) -> [('-text', 'Bullet lead text '), ('span', …)]
Node equality: li == li ? True   li is li ? False      # so the old `c != node` filter did work
```

### Is it strictly an improvement? Almost — one honest exception

Of the 10 changed cases, 9 are pure removal of foreign text, de-duplication, or restored word spacing. The exception is **mixed content**: a `<li>`/`<td>` carrying its own text *plus* a text-tag descendant. `v3audit/ld_loss.py`:

```
### li_with_nested_span
  BEFORE slx : 'Bullet lead textbullet tail text\nbullet tail text'
  AFTER  slx : 'bullet tail text'
  lxml AFTER : 'bullet tail text'      lxml unchanged by patch : True
  words in HTML but NOT in AFTER output: ['lead']
    -> lxml (the reference engine) ALSO drops them: True   lxml-missing=['lead']

### td_with_nested_span   — same shape, same verdict
### container_direct_text_plus_p / aplus_module_heading_text_plus_nested / p_with_nested_span_tail
     BEFORE slx == AFTER slx  (the drop already existed on both engines; unchanged)
### no_mixed_content_control  words lost: (none)
```

So: **real in-container text can now be dropped that the old selectolax kept** — but (a) the old code kept it glued to the following text with no separator (`'Bullet lead textbullet tail text'`), i.e. already corrupt, and (b) **lxml has always dropped it**, so this is the pre-existing leaf-only heuristic in the shared algorithm, now propagated to the production engine rather than introduced. Net: the fix removes 8 foreign lines per realistic page and can lose one lead-in fragment on mixed-content markup. I would call it a large improvement with a known, pre-existing residual — not "strictly" an improvement.

### Golden samples / export columns

**Golden: provably unaffected, by a stronger test than the one in the fix report.** That report showed `worker.parser` is never *imported*. I poisoned both functions so that being imported is not enough — they must never be *called*:

```
worker/parser.py:718   raise AssertionError("POISON: _slx_parse_long_description was called on the golden path")
worker/parser.py:2039  raise AssertionError("POISON: _parse_long_description was called on the golden path")

poisoned tree, golden verify / sqlite                        ✅ 64 步与基线完全一致  EXIT=0
poisoned tree, golden verify / postgres                      ✅ 64 步与基线完全一致  EXIT=0
poisoned + BOTH ENGINES INSTALLED (overlay), sqlite          ✅ 64 步与基线完全一致  EXIT=0
```

The baseline holds only the literal the harness posts (`tests/golden/scenario.py:64`):

```
  long_description   x28   value="a long description"
  long_description   x1    value="长描述"          # the export header label
  distinct values: 2
```

**Export: affected, and it is the live column.** `long_description` → `common/config.py:233` `"长描述"` → `EXPORT_COLUMN_ORDER` (`:248`), written verbatim by `_export_xlsx_streaming` / `_export_csv_streaming` from the DB column. It is also in `SLOW_HASH_FIELDS` (`common/slowhash.py:113`) and is a column in both `common/database.py:498` and `common/pgdb/schema.py:140`. So the change reaches erpAPI's 长描述 cell and the event stream's `slow_hash`, on **both backends**.

---

## 6. B7 — independently reproduced, and the gate has teeth

At `fea7395` under the overlay (selectolax importable, dateparser absent):

```
  File ".../base/tests/test_delivery_parse.py", line 85, in test_date_as_direct_text_node
    self.assertNotEqual(date, "N/A")
AssertionError: 'N/A' == 'N/A'
Ran 9 tests in 0.003s
FAILED (failures=1, skipped=2)          <- the exact skip arithmetic verify1 reported
```

At HEAD, same overlay: `Ran 10 tests ... OK (skipped=3)`, with `test_date_as_direct_text_node_no_dateparser ... ok`. The replacement is not decorative — mutation **M5** (delete the `data-csa-c-delivery-time` attribute-direct read) turns it red.

Parser mutation matrix, overlay ON, both backends (`v3audit/mutate_parser.sh`):

```
CONTROL, overlay ON    sqlite 31 passed, 3 skipped     postgres 31 passed, 3 skipped
CONTROL, overlay OFF   sqlite  6 passed, 28 skipped    postgres  6 passed, 28 skipped
M1 subtree walk -> container.traverse()  [B5 root cause]   9 failed  / 9 failed
M2 _slx_iter_descendants -> node.iter()  [leaf guard]      3 failed  / 3 failed
M3 (text(deep=True) or "").strip() -> text(strip=True)     3 failed  / 3 failed
M4 stray .traverse() elsewhere in parser.py [source guard] 1 failed  / 1 failed
M5 break the delivery attribute-direct read                1 failed  / 1 failed
```

---

## Findings

**F1 — the B5 regression guard is 2-of-5 effective in the environment this repo actually runs.** The venv has no selectolax (the production engine, `requirements.txt:12`), so 23 of the 24 new parser tests skip. I measured which mutations survive a default-venv run:

```
M1 (literal revert of the fix)  -> 1 failed   caught, but only by the AST source guard
M4 (stray .traverse())          -> 1 failed   caught by the same guard
M2 (leaf guard regression)      -> 6 passed, 28 skipped    GREEN — invisible
M3 (word-gluing regression)     -> 6 passed, 28 skipped    GREEN — invisible
M5 (delivery attribute read)    -> 6 passed, 28 skipped    GREEN — invisible
```

Only regressions that literally reintroduce the token `.traverse()` are caught. Any semantically equivalent regression ships green. The fix author flagged this and explicitly did not action it; I confirm the numbers and agree it is the single highest-value follow-up — adding `selectolax` + `dateparser` to `requirements-dev.txt` turns 6 passed/28 skipped into 31 passed/3 skipped with zero failures.

**F2 — the parser change is a one-time full-catalog `slow_hash` flip, and it is not written down anywhere a consumer will look.** `grep` over `common/pgdb/OWNERSHIP.md`, `.agent/pg_migration_plan.md` and `docs/sync_contract.md` finds no mention of B5, `long_description` semantics, or `traverse`. The only record is commit `24c498a`'s message. Consequence, measured: the first re-scrape after deploy changes `long_description` for every product whose page has *anything* after the description container (in my corpus, unchanged only in the `container_is_last` case), which flips `slow_hash` for effectively the whole catalog at once — exactly the class of event `docs/sync_contract.md` teaches the consumer to treat as "slow attributes changed". Worth a D-number and a line in the contract before Phase 4 turns `completeness_ok` on.

**F3 — the parser change alters production data on the SQLite deployment too.** It is the only change in `fea7395..HEAD` that does. This does not violate the standing rule (that rule covers `common/database.py` and the golden baseline, both verified byte-identical), but "SQLite path unchanged" is no longer true of the *product* — only of the storage layer. Stating it so nobody infers otherwise from the green golden gate.

**F4 — two cosmetic observations, neither a defect.**
- `GET /api/v1/sync/counts` on SQLite returns `422` when required query params are missing, because FastAPI validation runs before the backend check; with valid params it correctly returns `503`. Harmless — it is never `404`, which is the code the contract cares about.
- The bounded walk is much faster on realistic pages but ~1.8× slower on one adversarial shape where `any()` cannot short-circuit:

```
                                        BEFORE      AFTER
A+ 120 modules + 200 tail nodes         16.32 ms    1.82 ms
300 x (td > 40-deep <a> chain)          11.03 ms   19.66 ms   <- the one regression
3000-deep <div> chain                  534.69 ms   22.41 ms
```

Note the BEFORE outputs on rows 1–2 were truncated at the 10 000-char cap by absorbed junk, so the two columns are not doing equal work; 19.66 ms on a synthetic 12 000-node table is not a production concern.

## Cleanup

```
git status --porcelain   -> (empty)          HEAD 2e9c311baba6b2046e7d17b5afddb11e0558e7f2
git worktree list        -> /home/user/amazon-scraper-v3  2e9c311 [claude/walmart-api-db-refactor-7oergd]
databases                -> scraper_dev                    (no scratch DBs created or left)
venv                     -> selectolax: False lxml: False dateparser: False   (never modified)
```

The engines were used only via an isolated `--target` overlay at `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/slx_overlay` (`PYTHONPATH=$OV`), so the shared venv was never mutated — the thing that caused verify1's phantom flake.

Re-runnable probes, all in `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v3audit/`: `matrix.sh`, `matrix_base.sh`, `mutate.sh`, `mutate_parser.sh`, `sqlite_purity.py`, `ld_cases.py`, `ld_probe.py`, `ld_report.py`, `ld_loss.py`, `e2e_parse.py`, `routes.py`, `perf.py`, plus the captured logs `matrix_head.log`, `matrix_base.log`, `nodes_*.txt`, `skips_*.txt`, `ld_before.json`, `ld_after.json`.