Audit complete. Tree clean, worktrees removed, scratch DB dropped.

---

# Verification 3 — Non-regression + invisible-change audit

## 1. Six-cell gate matrix (real output)

```
=== GATE 1: golden verify (sqlite) ===     ✅ 64 步与基线完全一致        EXIT=0
=== GATE 2: golden verify (postgres) ===   ✅ 64 步与基线完全一致        EXIT=0
=== GATE 3: pytest (sqlite) ===      635 passed, 14 skipped, 1 warning in 170.94s   EXIT=0
=== GATE 4: pytest (postgres) ===    637 passed, 12 skipped, 1 warning in 182.37s   EXIT=0
=== GATE 5: unittest (sqlite) ===    Ran 157 tests in 1.425s — OK (skipped=14)      EXIT=0
=== GATE 6: unittest (postgres) ===  Ran 157 tests in 4.823s — OK (skipped=12)      EXIT=0
```

All six green. Every builder's reported counts reconcile: 635+14 = 637+12 = **649**, exactly the collected node count.

## 2. Diff audit — clean

| Check | Result |
|---|---|
| `common/database.py` | **Byte-identical.** Blob SHA `d19e04bb…` at both `a34e0c6` and `HEAD`; not in `--name-status` |
| `sqlite_baseline.json` | **Byte-identical.** Blob `2f5e82b4…` at both |
| `tests/golden/` whole tree | `git ls-tree -r` diff is **empty** — no backend conditionals *could* have been added to `scenario.py`/`run.py`; they were not touched at all |
| Tests deleted | none (`--diff-filter=D` empty) |
| Skips/xfails **removed** | none |
| Skips **added** | 21 lines, all conditional on `selectolax`/`lxml` presence |
| Working tree | clean, matches `HEAD` |

**Node ids: 514 → 649. Lost: 0. Added: 135.**

```
=== LOST (at a34e0c6 but not at HEAD) ===
--- end LOST (empty above = none lost) ---
=== ADDED, by file ===
  44 tests/test_engine_not_found.py    31 tests/pgdb/test_retention.py    1 tests/test_runner_parity.py
  33 tests/test_parser_quality.py      26 tests/pgdb/test_phase4_fields.py
```

The +3 skip delta (11→14, 9→12) is fully accounted for: `test_parser_quality.py:506/542/567`, all `lxml 未安装`. I verified `lxml` is genuinely absent from the venv. **Consequence worth flagging: the entire lxml-fallback branch of D-60 is skipped in the standing gates.** The parser agent tested it via a scratch `--target` install, but no gate protects it — and §6.6 (below) is specifically about that path.

The two modified pre-existing test files both **strengthen**: `test_session_slot.py` makes 5 unconditional stubs conditional; `test_runner_parity.py` adds an AST guard. Nothing weakened.

## 3. Mutation tests — 8/9 killed, 9th explained

Each fix reverted one at a time in an isolated worktree (baselines: parser 30 passed, parity 3, phase4 26, retention 31):

```
M1  D-58 manufacturer: exact -> substring        2 failed, 28 passed
M2  D-59 sorted() -> list(set)                   2 failed, 28 passed
M3  D-61 crawl_time: RFC3339 Z -> naive          4 failed, 26 passed
M6  D-53 _stub_if_missing -> _stub               1 failed,  2 passed
M4  D-40 relay ignores payload._outcome          4 failed, 22 passed
M5  D-43 404 write protection off                2 failed, 24 passed
M7  D-54 blank zip counts as authoritative      26 passed   <-- SURVIVED
M8  P6-2 ack floor allows 0                      2 failed, 29 passed
M9  P6-5 overlap slack -> 0                      5 failed, 26 passed
```

**M7 investigated, and the integration report is vindicated.** I mutated the wrong thing. Their claimed mutation — restoring pre-D-54 "outbox answers level 3 itself" — *is* killed, with exactly the assertion they cited:

```
E  - 90210
E  + 10001
FAILED test_zip_requested_prefers_the_task_row_then_the_worker_meta
FAILED test_blank_task_zip_consults_the_meta_before_the_payload_zip
```

My mutation hit the `str(z).strip() != ""` half of the shared predicate, which survives because `relay.py:1240` independently treats blank as "not provided" (`if zip_requested is None or not str(zip_requested).strip()`). I confirmed the mutation really does flip the helper (`HEAD: False` → `MUTANT: True` on a genuinely-stored `''`), so this is redundancy, not a defect — but that half of the predicate is uncovered.

## 4. Invisible changes — the part that matters

First, the blind spot is real and I measured it. Both scrubbing paths erase the crawl_time change — the volatile-key path *and* the general `_TS_RE` substring path, so it would be invisible even under a non-volatile key:

```
old scrubbed: {'crawl_time': '<VOLATILE>', 'site': 'US', 'note': 'scraped at <TS>'}
new scrubbed: {'crawl_time': '<VOLATILE>', 'site': 'US', 'note': 'scraped at <TS>'}
identical after scrub: True
```

*(Aside: blind spot #3 in the brief is stale — `harness.py:389 _numeric_but_not_bool` now blocks bool/int swaps.)*

I ran **real HTML through the real parser in both trees** (7 fixtures, both on the production selectolax path) and diffed all 43 `EXPORTABLE_FIELDS`. Every change found:

| # | Change | Consumer observes before → after | Documented? |
|---|---|---|---|
| 1 | **crawl_time format** | `'2026-08-05 20:50:44'` (naive UTC+8) → `'2026-08-05T12:50:44Z'` | ✅ D-61, contract §0.1 row 6, §6.2 |
| 2 | **manufacturer** | `'3 years and up'` → `'Acme Industrial Ltd.'` | ✅ D-58 |
| 3 | **upc_list order** | 8 distinct orderings across 8 hash seeds → 1 | ✅ D-59 |
| 4 | **seller_id / seller_name** (Amazon-sold) | `'N/A'/'N/A'` → `'AMAZON'/'Amazon.com'` | ✅ D-60 |
| 5 | **zip_code semantics** | page line1=99999, requested 90210 → was `'99999'`, now `'90210'` + `_zip_observed='10001'`, `_zip_verify='mismatch'` | ✅ D-55 |
| 6 | **4 fields on early-exit paths** | captcha/empty-HTML: **absent** → present as `'N/A'` | ❌ **see finding** |
| 7 | **404 payload** | 48 keys incl. `title='[商品不存在]'` → 26 keys, slow fields omitted | ✅ D-57, contract §10 item 4 |
| 8 | **404 server write** (PG only) | hashes/baseline/`asin_changes` written → not written | ✅ D-43 |
| 9 | **site** | `'US'` → `'US'` at all 7 fixtures — genuinely unchanged | ✅ D-44 |

Hash impact, measured and isolated — **`review_hash` does not move, so the §6.5 re-review gate is unaffected**; `slow_hash` moves, and `manufacturer` alone is the cause:

```
case         review old / new          flip    slow old / new            flip
FULL         v1:b45ecffbf6 / same      False   v1:c94121ec1c -> 5f6c784fda  True
THIRD_PARTY  v1:b45ecffbf6 / same      False   v1:c94121ec1c -> 5f6c784fda  True

--- isolate which single field moves slow_hash on FULL ---
  manufacturer -> new      FLIP=True
  upc_list order -> new    FLIP=False   (slowhash _LIST_FIELDS normalizes order)
  crawl_time -> new        FLIP=False   (excluded from both hashes)
  seller_id/name -> new    FLIP=False   (outside both hashes)
```

The contract's pre-existing one-time-flip warning (§6.5, present at `a34e0c6`) prescribes "prepare a one-time suppression, or re-baseline after deploy" — operationally correct for the manufacturer contribution too, and its stated reason (`review_hash` unaffected) I verified still holds. Its itemized cause list is now incomplete (names only `long_description`), which is a minor accuracy nit, not a gap in the guidance.

## 5. FINDING — one undocumented invisible change (must fix)

**`docs/sync_contract.md:687-689` documents the exact behaviour D-60 reversed, and is unchanged from `a34e0c6`.** It stands under the heading 已知的形状约束 (known shape constraints):

> - **lxml 回退路径与全部早退路径上，`rating` / `review_count` / `seller_id` / `seller_name` 这 4 个字段在 `payload` 里是「缺席」** —— 不是 `null`，更不是旧值。用 `key in payload` 判断，不要用 `payload.get(k) is None`。

The contract hands the consumer a discriminator and tells it which one to use. That discriminator is now constant-true:

```
----- BEFORE (a34e0c6) -----
  captcha page      `key in payload` -> False   values={'rating': '<ABSENT>', ...}
  empty html        `key in payload` -> False   values={'rating': '<ABSENT>', ...}
----- AFTER  (HEAD)    -----
  captcha page      `key in payload` -> True    values={'rating': 'N/A', ...}
  empty html        `key in payload` -> True    values={'rating': 'N/A', ...}
```

A consumer that implemented the documented rule silently flips from "not measured, keep my stored value" to "measured, write `N/A`". Golden cannot see it (the harness never imports the parser). No doc anywhere states the new shape — `grep` across `docs/` and `.agent/MIGRATION_STATUS.md` returns only this stale paragraph.

**Severity, stated honestly:** bounded but real. The named paths (captcha/blocked/404/parse-fail) all carry `outcome != 'ok'`, and §6.3 already keeps those out of `products`, so a rule-following consumer limits the damage to snapshots. But §6.6 presents this as a general payload-shape rule with no such scoping, and the lxml fallback *can* produce `outcome == 'ok'`. The builders knew — D-60 flags the 404 interaction and the engine agent listed it as "Other findings #3" — it just never reached the consumer-facing document.

**Fix:** replace that bullet with the new invariant (the four fields are now always present; `'N/A'` means "not obtainable this scrape", not "unchanged"), and add a row to the §0.1 changelog. It belongs in the same release as D-60, since it describes D-60's wire effect.

## 6. Minor notes

- **lxml branch is unprotected by the gates** (3 new skips). D-60's engine-agnostic rewrite is exactly what those skipped tests cover.
- `_task_zip_is_authoritative`'s blank-string half is uncovered — redundant with `relay.py:1240`, so harmless, but the D-54 test pins relay's arbitration rather than the outbox predicate.
- D-39's "not one CHECK added" holds for the event columns; the single new CHECK is Phase 6's `sync_meta.ack_seq` guard on a different table, installed under try/except.

## 7. Cleanup

```
=== worktrees ===  /home/user/amazon-scraper-v3  5f91166 [claude/walmart-api-db-refactor-7oergd]
=== scratch databases remaining ===  scraper_dev
=== tree status ===  (empty = clean)
common/database.py               9e39c33676dbd20d8edcf6077bfc852c   (unchanged)
tests/golden/samples/sqlite_baseline.json  35d19ae3e2de6eb7548548a51fce80b6  (unchanged)
```

Both audit worktrees removed; one scratch DB I created (`scraper_try_v3zip`) dropped.

**Verdict: non-regression passes cleanly. One documentation defect found in the invisible-change class — it is a doc-only fix, but per the phase's own standard it must land before the Walmart side integrates.**