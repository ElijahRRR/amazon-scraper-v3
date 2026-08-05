All six gates green, tree clean, worktree removed, no scratch DBs left.

## Phase 4 verification — all 9 items reproduced at `a34e0c6` and confirmed fixed at HEAD

Method: a git worktree at `a34e0c6`, my own HTML fixtures (`/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v1/fixtures.py`) written by reverse-engineering the parser's selectors — deliberately **not** the builders' fixtures, since those are the defendant's own evidence. Every number below is from real HTML through the real parser.

**I did not cite the golden gate as evidence for anything, and I verified the three blind spots myself rather than taking them on faith:** `crawl_time` is in `_VOLATILE_KEYS` (`tests/golden/harness.py:34`) and both formats scrub to `<VOLATILE>` identically; `grep` for `worker.parser|worker.engine|AmazonParser` across `tests/golden/` returns **zero hits**, and `scenario.py:36 _product()` is a hand-written dict.

### Per item

| | Baseline `a34e0c6` | HEAD |
|---|---|---|
| **P4-1** | all 6 zip cases → `zip_code` = requested, no observed/verify keys | `confirmed`/`mismatch`/`assumed`/`unverified` all reachable; ZIP+4 → `11201` |
| **P4-2** | degraded and empty-blocks pages byte-identical in every parsed field | `8` vs `15` — distinguishable |
| **P4-3** | — | title/brand/category_tree/upc_list/image_urls/manufacturer all survive a 404, both backends |
| **P4-4** | order decides: age-first→`'Acme Industrial Ltd.'`, mfr-first→`'3 years and up'` | both→`'Acme Industrial Ltd.'` |
| **P4-5** | 12 distinct orderings / 12 processes | 1 |
| **P4-6** | four keys **absent** from the dict on lxml | present, correct, on both engines |
| **P4-7** | `'2026-08-05 20:31:33'` | `'2026-08-05T12:31:33Z'` stored verbatim, read back raw |
| **P4-8** | `site='US'` | unchanged — recommendation stands |
| **P4-9** | — | `selectolax`/`lxml`/`None` |

The breadcrumb requirement specifically: page with breadcrumb block deleted → `_completeness=14`, bit0 = **0**, while `manufacturer='Acme Industrial Ltd.'`, `model_number='A2637'` and 2 image URLs all parse fine. Empty breadcrumb block → `15`, bit0 = 1, `category_tree=''`. The distinction holds.

End-to-end re-review gate, real HTML → parser → HTTP → relay → `/api/v1/sync/records`, contract §6.5 executed literally: **naive hash compare = 2 false re-reviews, conjunctive gate = 0.** `good` and `good again` return the identical hash `v1:e5a950d31e245974404b37b2aeb`.

### Two things the brief and the builders understated

**The P4-1 defect was worse than "returns None almost always."** The call site is `result["zip_code"] = self._slx_parse_zip_code(tree) or zip_code` — when line1 *does* yield digits the observed value **wins over the requested one**, and the old regex is a bare `(\d{5})` with no zero-width guards. A 10-digit account number in line1 produced:

```
BASE  line1 有 10 位长数字串   zip_code='12345'   (请求的是 10001)
HEAD  line1 有 10 位长数字串   zip_code='10001'  observed='10001'  verify='confirmed'
```

That is a **fabricated** value in the consumer's grouping key `(asin, marketplace, zip_requested)`, not merely a missing observation.

**P4-5 has a masking path worth knowing.** My first fixture used `dimensionValuesDisplayData`, which routes through `_parse_twister` (insertion-ordered) and never touches `list(set(...))` — it showed 1 distinct ordering even at baseline. `_parse_variation_asins` is only the *fallback*. I rebuilt the fixture without twister to reach it. I also audited all four `set(...)`→join sites (`parser.py:905, 1912, 1926, 2421`); all sort.

### Damage hunt — no regressions found

Full field-by-field diff, 11 page shapes × 2 engines, BASE→HEAD: **0 changes outside the declared Phase 4 scope** on both paths. Cross-engine at HEAD, the only diffs are `_parse_engine` itself plus one `stock_status` divergence on a garbage page — which is **identical at `a34e0c6`**, i.e. pre-existing, not Phase 4 damage. (P4-9 earned its keep immediately: that is exactly the mystery it makes attributable.)

Measured one-time transition cost, which nobody had quantified:

```
review_hash 翻转 0/11      slow_hash 翻转 2/11
```

The 2 are precisely the pages where `manufacturer` was previously polluted — the fix, not damage. `upc_list` reordering flips **nothing**, because `slowhash._LIST_FIELDS` sorts it. `crawl_time` is in neither hash. So Phase 4 triggers **zero** re-review storm, and the `slow_hash` moves are covered by the existing §6.5 blanket warning.

Three behaviour changes I confirmed are real, all disclosed or benign:
- The four P4-6 fields **are** clobbered to `"N/A"` on a 404 (both builders flagged this; `_NOT_FOUND_PRESERVED_FIELDS` deliberately excludes them as fast-changing).
- Captcha/blocked **cannot** clobber them — those branches `continue` to retry, and retry exhaustion calls `_submit_result(..., None, success=False)`. The `_is_parse_failure` weakness is real but unreachable from the engine's captcha path.
- `_`-prefixed keys do not leak: `asin_data` has none, and no `zip_observed`/`completeness`/`parse_engine` columns — they live only in the event stream.

### One small documentation gap

`D-60` documents the `separator=" "` seller fix in terms of Amazon-self-sold pages returning `("N/A","N/A")`. It also silently changes **third-party** seller names split across nodes: `'AcmeStoreUS'` → `'Acme Store US'`. `seller_name` is in `EXPORTABLE_FIELDS` (43 columns), so exported data changes; it is in neither hash, so re-review impact is zero. Worth a line in the changelog, not a blocker. HEAD matches lxml semantics, which is the repo's stated reference.

### Gates (my own run, full matrix)

```
### golden sqlite      ✅ 64 步与基线完全一致
### golden postgres    ✅ 64 步与基线完全一致
### pytest sqlite      635 passed, 14 skipped
### pytest postgres    637 passed, 12 skipped
### unittest sqlite    Ran 157 tests — OK (skipped=14)
### unittest postgres  Ran 157 tests — OK (skipped=12)
```

`common/database.py` and `tests/golden/samples/sqlite_baseline.json`: **0 changed lines** vs `a34e0c6`. Worktree removed, `git status` clean, no scratch databases remain.

Probe scripts (absolute paths, all under `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/v1/`): `fixtures.py`, `p41_zip.py`, `p42_completeness.py`, `p42_rereview.py`, `p44_manufacturer.py`, `p45_one.py`, `p45_fallback.py`, `p46_lxml.py`, `p47_wire.py`, `dump_all.py`, `diffdump.py`, `hashflip.py`, `gates.txt`.