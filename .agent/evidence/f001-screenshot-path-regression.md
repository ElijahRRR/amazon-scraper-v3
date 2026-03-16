# F-001 Evidence

- Baseline repro:
  - Created an isolated temp DB.
  - Ran `update_screenshot_status(..., status='done')` before the first `save_result(...)` for a new ASIN.
  - Observed `repr(row.get('screenshot_path')) == None` before the fix.

- Post-fix race verification:
  - Re-ran the same temp-DB repro.
  - Observed `'/static/screenshots/race-test/BTEST123456.png'`.

- Regression test:
  - Command: `python3 -m unittest test_database_screenshot_path.py`
  - Result: `Ran 2 tests in 0.927s` and `OK`

- Notes:
  - Local `data/scraper.db` did not include `B00006IF89`, `B0000AVCAW`, or `B00006F71L`, so no direct repair of those rows was executed in this workspace.
