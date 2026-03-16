# F-003 Screenshot Cache Isolation

## Baseline reproduction

- Command:
  - `ps -Ao pid,ppid,pgid,command | rg 'worker/screenshot.py|run_worker.py|http://127.0.0.1:8899'`
- Result:
  - Found multiple orphaned `worker/screenshot.py http://127.0.0.1:8899 ...` processes with `ppid=1`.
- Impact:
  - Those orphan screenshot workers were still scanning the shared `worker/screenshot_cache/html` directory and stealing HTML files from new validation workers.

## Intermediate repro with diagnostics

- Added diagnostic logs in `worker/screenshot.py`.
- Isolated validation against temp server `http://127.0.0.1:8897` reproduced:
  - `截图源文件在占用前消失: B0D7ZTSYHX`
- Conclusion:
  - The missing-file race was not inside the current child after claim; another process was consuming the same shared cache directory.

## Fix

- Namespaced screenshot cache directories by `server_url + worker_id`.
- Passed the isolated screenshot base directory to the child via `SCREENSHOT_BASE_DIR`.
- Kept `.processing` state and server row-count guards from the earlier state-machine fix.

## Verification

- Static checks:
  - `python3 -m py_compile worker/engine.py worker/screenshot.py`
  - `python3 -m unittest test_database_screenshot_path.py test_screenshot_state_machine.py`
- Real isolated batch validation:
  - Batch: `isolated_verify_1773658597`
  - Screenshot cache dir:
    - `/Users/nextderboy/Projects/amazon-scraper-v3/worker/screenshot_cache/http___127.0.0.1_8897__isolated-1773658597`
  - Observed child lifecycle:
    - Claimed both files as `.processing`
    - Uploaded `B0D7ZTSYHX` and `B0025U0ZMQ`
    - Logged `批次完成: isolated_verify_1773658597 (done=2 failed=0 total=2)`
  - Final DB state:
    - both screenshot rows `status='done'`
  - Final process-group state:
    - `FINAL ... None 0`

## Evidence excerpts

- `占用截图源文件: B0D7ZTSYHX -> B0D7ZTSYHX.processing`
- `占用截图源文件: B0025U0ZMQ -> B0025U0ZMQ.processing`
- `截图上传结果: B0D7ZTSYHX ok=True`
- `截图上传结果: B0025U0ZMQ ok=True`
- `FINAL ... {'status': 'done' ...}, {'status': 'done' ...} ... None 0`
