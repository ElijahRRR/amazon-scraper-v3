# F-004 Staged Excel Load Test

## Input

- Source file:
  - `/Users/nextderboy/Desktop/测试文件夹/小测试_副本.xlsx`
- Unique ASIN count extracted:
  - `500`

## Environment

- Restarted local server on `http://127.0.0.1:8899`
- Started worker:
  - `python3 run_worker.py --server http://127.0.0.1:8899 --worker-id loadtest-stage1`

## Stage 1: first 100 ASINs

- Batch:
  - `stage1_excel100_1773658969`
- Upload result:
  - `batch_id=1`
  - `total_asins=100`
- Final verified progress:
  - task progress: `done=100 failed=0 total=100`
  - screenshot progress: `done=100 failed=0 total=100`
- Screenshot lifecycle evidence:
  - gate log: `⏸️ 等待截图完成后再拉取新任务（1 个批次待处理）`
  - child log: `批次完成: stage1_excel100_1773658969 (done=100 failed=0 total=100)`
  - gate release log: `📸 截图批次已上传: {'stage1_excel100_1773658969'}`

## Stage 2: remaining 400 ASINs

- Batch:
  - `stage2_excel400_1773659068`
- Upload result:
  - `batch_id=2`
  - `total_asins=400`
- Final verified progress:
  - task progress: `done=400 failed=0 total=400`
  - screenshot progress: `done=400 failed=0 total=400`
- Final gate evidence:
  - child log: `批次完成: stage2_excel400_1773659068 (done=400 failed=0 total=400)`
  - API check: `/api/progress?batch_id=2` => `{"pending":0,"processing":0,"done":400,"failed":0,"total":400,...}`
  - API check: `/api/batches/stage2_excel400_1773659068/screenshots/progress` => `{"pending":0,"processing":0,"done":400,"failed":0,"total":400}`

## Post-restart 404 verification

- Restarted worker after the stage-2 batch completed:
  - old worker session `66510` exited cleanly and its screenshot child logged `收到停止请求`
  - new worker session `99971` started on the patched code
- Verification batch:
  - `verify_404_screenshot_1773660030`
  - `batch_id=3`
  - ASINs:
    - `B000F6RWW8`
    - `B000LRX9XM`
- Final verified progress:
  - task progress: `done=2 failed=0 total=2`
  - screenshot progress: `done=2 failed=0 total=2`
- Worker/screenshot evidence:
  - worker log: `ASIN B000LRX9XM 商品不存在 (404)`
  - worker log: `ASIN B000F6RWW8 商品不存在 (404)`
  - child log: `截图完成: B000LRX9XM (88KB)`
  - child log: `截图完成: B000F6RWW8 (88KB)`
  - child log: `批次完成: verify_404_screenshot_1773660030 (done=2 failed=0 total=2)`

## Current process snapshot

- One worker process:
  - `run_worker.py --server http://127.0.0.1:8899 --worker-id loadtest-stage1`
- One screenshot subprocess:
  - `worker/screenshot.py http://127.0.0.1:8899 1 4`

## Stability observations

- No recurrence of:
  - disappearing HTML before claim caused by shared cache theft
  - multiple screenshot subprocesses spawning for the same worker
  - screenshot rows stuck pending after local batch completion in stage 1
- Early stage-2 fetches still hit proxy-side `CONNECT tunnel failed, response 429`, but retries recovered and successful task/screenshot throughput continued.

## Useful live objects

- Server session id:
  - `23362`
- Worker session id:
  - `99971`
