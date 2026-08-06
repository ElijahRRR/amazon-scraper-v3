Verification complete. Final report:

## HASH DETERMINISM — PASSES, verified across 5 separate processes

Two independent probes. The stronger one drives the **real `worker/parser.py`**, where the original defect actually lives (`_slx_parse_upc`, `worker/parser.py:740`, `",".join(list(set(...)))`), with a live control:

```
PYTHONHASHSEED per process: ['695425565', '2035525363', '323946140', '847877000', '1397871145']
engine: selectolax
raw parser output that IS seed-dependent (the defect being neutralised):
  seed=695425565   upc_list=887276789170,887276789194,887276789132,887276789187,8872767892…
  seed=2035525363  upc_list=887276789149,887276789118,887276789200,887276789217,8872767891…
  distinct upc_list orderings across the 5 processes: 5/5
hash agreement across the 5 processes:
  [OK ] review_hash    STABLE (1 value)      STABLE
  [OK ] slow_hash      STABLE (1 value)      STABLE
  [OK ] content_hash   FLAPS (5 values)      control: expected to FLAP
```

Byte-identical HTML, 5 interpreters: legacy `content_hash` produced **5 distinct values**, `review_hash`/`slow_hash` produced **1**. The documented defect is real and the fix neutralizes it end-to-end. Also verified: 11 payload shapes (unicode/NUL/non-string scalars/empty/absent) agree on the **canonical object**, not just the hex; and the hash is invariant across the jsonb round-trip the relay actually performs (`common/pgdb/relay.py:1295` hashes the round-tripped `result`) — 6/6 including duplicate-key collapse, key reordering, floats, 10^18 ints, emoji.

## FINDING 1 (severe) — `slow_hash` changes on *every scrape* under the production engine

30-day sequence through the real parser, one genuine identity change (day-20 retitle):

```
engine=selectolax   review_hash changed  7 times, slow_hash changed 29 times on days [2..30]
engine=lxml         review_hash changed  7 times, slow_hash changed  7 times
=> slow false-change rate: 28 extra / 29 transitions   (selectolax)
```

Root cause is **not** in `common/slowhash.py`. It is `worker/parser.py:677` `_slx_parse_long_description`, which uses selectolax `Node.traverse()`. That is **not subtree-bounded** — minimal repro:

```
container.html = <div id="productDescription"><p>REAL DESCRIPTION TEXT HERE ok</p></div>
nodes yielded by container.traverse():
   tag=div    text='REAL DESCRIPTION TEXT HERE ok'
   tag=p      text='REAL DESCRIPTION TEXT HERE ok'
   tag=div    text='$549.99'          <- outside the container
   tag=span   text='In Stock'         <- outside the container
```

So `long_description` absorbs price, stock, rating, BSR and raw CDN image URLs, and `long_description ∈ SLOW_HASH_FIELDS`:

```
=== price 429
    long_description
      was: '…for any desk. [image: …71xrpjis8ll._ac_sl1500_.jpg] $549.99 in stock 2431 ratings…'
      now: '…for any desk. [image: …71xrpjis8ll._ac_sl1500_.jpg] $429.00 in stock 2431 ratings…'
```

The lxml counterpart `_parse_long_description` uses `container.iter()` and is correct. Consequences: (a) `slow_hash` is pure noise on the production engine — `selectolax>=0.3.21` is `requirements.txt:12` and `_USE_SELECTOLAX` is true whenever importable; (b) the same page hashes differently per engine (`e39f77f0…` vs `6bca6648…`), so an engine rollout resets every product's `slow_hash` at once. `review_hash` is engine-stable (`b8e5c73e…` under both) because `long_description` is slow-only.

## FINDING 2 — soft degradation flips `review_hash`

Confirmed through the real parser, with the canonical-object diff:

```
  !! BREADCRUMB DROPPED
       root_category_id was '172282'   now None
       category_tree was  ['electronics','computers & accessories','monitors']   now None
  !! gl_product_group_type absent
       product_type was   'personal_computer'   now None
```

Three degraded days cost **6 false `review_hash` changes** (two per episode — into and out of degradation). Same class: `brand` falling through its 4-level chain to `Visit the SAMSUNG Store` or `N/A`, and `variant_attributes` losing dimension names.

This is currently **latent, not live**, and the reason matters: `_COMPLETENESS_UNMEASURED = 0` (`common/pgdb/relay.py:213`, written at `:1308`) makes `completeness_ok` false for every Phase-2 row, so §4.3's conjunctive gate never fires — which also means no *real* review ever fires. It goes live the moment Phase 4 sets completeness bits. Whoever lands Phase 4 must handle degradation, or the first correct completeness bitmap ships a false-review storm.

## FINDING 3 (minor) — image ID case-collapse, a missed change

`extract_image_ids` casefolds before extracting the ID, but Amazon image IDs are case-sensitive:

```
  71xRPjIS8LL -> ['71xrpjis8ll']
  71XRpjis8ll -> ['71xrpjis8ll']
  two DISTINCT Amazon image ids collapse to one value: True
```

Low probability, but it is a false negative (hash fails to change on a real change), which is the worse direction.

## FINDING 4 (minor) — 5 invisible-character classes flip the hash

Strict pairwise, same visible string: **U+200B** ZWSP, **U+00AD** soft hyphen, **U+FEFF** BOM, **U+200D** ZWJ, **U+200E** LRM all produce different hashes. NFKC does not remove them and Python's `\s` does not match them. Amazon does emit soft hyphens and bidi marks. (Correctly invariant: NFC/NFD, NBSP, narrow NBSP, fullwidth, ligature, case, whitespace, NUL, ™, superscripts.)

## Everything else the gate got right

74-case dict-level battery plus 14 real-parser cases. No flap on: price/list-price/stock/OOS/BSR/rating/review-count/seller-rotation/FBA/delivery/shipping/`crawl_time`/zip/`variation_asins`, and the four carried-forward fields going absent on the lxml path. No flap on CDN churn: size suffix, host swap, http/https, protocol-relative, cache-buster query, ordering, extra size variants of the same image, legacy `._AC_,0,0_.jpg` comma transforms — while a genuinely added or removed photo does flip `slow_hash`. Order-invariance holds for `upc_list`/images/variant-attr keys; order-*sensitivity* correctly holds for `bullet_points` and `category_tree` (a path, not a set). Square brackets: `[2-Pack] Storage Bins`, `[Upgraded]`, `[商品不存在]的替代品` all survive; only exact sentinels null out. Unlike the legacy `"|".join`, the structured-JSON digest has no delimiter-injection ambiguity.

## Method note

My real-parser probe initially reported **11/14 OK vacuously** — neither `lxml` nor `selectolax` was installed, so every parse returned `[HTML解析失败]`, every hash was equal, and every "must not flip" case passed. I added a sensitivity guard that refuses to report unless the baseline parse yields a real page. Worth flagging: `/home/user/amazon-scraper-v3/tests/test_slowhash.py` (83 passing, genuinely good on determinism and sentinels) never exercises the parser, so it cannot see Findings 1 or 2 — every fixture is a hand-built dict of what the parser is *assumed* to emit.

## Baselines and environment

```
✅ 64 步与基线完全一致        (sqlite)
✅ 64 步与基线完全一致        (DB_BACKEND=postgres)
427 passed, 6 skipped, 1 warning in 136.40s
tree clean, HEAD=69886c6
worker parser deps installed: 0 (0 = restored to original)
```

427/6 is the Phase-2 baseline (the 268/4 in the brief is Phase-1, before Phase 2 added tests); I changed no repo files, so nothing regressed. I temporarily `pip install`ed `lxml`+`selectolax` to exercise the parser and **uninstalled both** — confirmed above. My scratch DB `scratch_hashroundtrip_v2` was dropped by its own probe; `scraper_dev` is the only database left. One `scraper_try_*` DB appeared mid-run but the repo's own harness (`/home/user/amazon-scraper-v3/tests/pgdb/helpers.py:71`) cleaned it up.

Probes, all re-runnable, in `/tmp/claude-0/-home-user-amazon-scraper-v3/f7028041-143a-57f3-92e9-2368e0262c89/scratchpad/`: `probe_a_procs.py` (5-process, synthetic), `probe_b_gate.py` (74-case battery), `probe_c_unicode.py` (pairwise unicode), `probe_d_realparser.py` (real parser, `FORCE_LXML=1` for the A/B), `probe_e_sequence.py` (30-day flap rate), `probe_f_jsonb.py` (round-trip), `probe_g_e2e_procs.py` (5-process end-to-end with live control).

**Bottom line:** determinism is solid and the cross-process defect is genuinely fixed. `review_hash` is a sound gate today, with one latent degradation flap to close before Phase 4 arms it. `slow_hash` is currently unusable on the production engine — but the bug is in `worker/parser.py:677`, not in `common/slowhash.py`, and fixing `traverse()`→bounded iteration repairs it without touching the hash spec.