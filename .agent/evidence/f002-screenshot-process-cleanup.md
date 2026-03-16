# F-002 Evidence

- Syntax validation:
  - `python3 -m py_compile worker/engine.py worker/screenshot.py`
  - Result: pass

- Focused process-group cleanup repro:
  - Spawned a synthetic screenshot-like subprocess tree using `start_new_session=True`.
  - Injected that process into `Worker._screenshot_process` / `_screenshot_pgid`.
  - Ran `await worker._stop_screenshot_process()`.
  - Output:
    - `before_count: 2`
    - `after_count: 0`
  - Interpretation: stop now clears the whole process group, not just the top-level Python wrapper.

- Root-cause note:
  - The screenshot child previously relied on immediate `sys.exit()` in signal handlers, while the parent only terminated the Python wrapper process. That combination could leave browser descendants behind across restarts.

- Real isolated batch run:
  - Ran a dedicated temp server on `127.0.0.1:8898` with an isolated DB and a 2-ASIN screenshot-enabled batch.
  - Observed screenshot subprocess `pgid=44105` with `pg_members=2` while rendering; there was no unbounded growth in browser processes.
  - Sent SIGINT to the validation worker and observed screenshot child logs:
    - `收到停止请求，准备退出当前截图循环 (SIGTERM)`
    - `截图进程退出`
  - Final worker output reported `final_group_counts: {44105: 0}`.
  - Separate functional issue remains: the batch gate released while `screenshots` rows stayed `pending`, matching the existing review findings.
