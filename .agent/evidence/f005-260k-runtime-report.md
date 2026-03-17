# F-005 Runtime Observation Report

## Target

- Batch: `batch_20260316_193458`
- Batch id: `1`
- Total tasks: `260932`
- Monitor started: `2026-03-16T11:47:55Z`
- Monitor ended: `2026-03-16T16:10:34Z`
- Snapshot count: `263`

## Outcome

- Final progress: `completed=257762 failed=3170 pending=0 processing=0`
- Average completed per minute: `962.16`
- Peak completed per minute: `1802`
- Average online workers observed: `4`
- Peak worker CPU (single process): `100.0%`
- Peak server CPU: `36.6%`

## Stability Summary

- Cumulative `429` proxy errors: `15052`
- Cumulative blank-page retries: `1959`
- Cumulative block detections: `645`
- Cumulative request timeouts: `17989`
- Cumulative terminal task failures: `45724`
- Cumulative session init failures: `298`

## Suspected Issues

- Proxy-side `429` bursts were sustained and were the most common source of startup instability and retries.
- Workers continued to encounter `[页面为空]` retries under load, which suggests either upstream throttling or HTML readiness problems still surface during high concurrency.
- Block detection and session rotation remained active during the run, so throughput depends on how quickly the proxy/session pool recovers after a bad burst.

## Notable Events

- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0G2GLSJ7N 核心字段全部缺失，疑似降级页面 (尝试 2/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0FPKSPJ5V 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B07Q1G5JWQ 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [ERROR] ASIN B0FB7282LN 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0GDWJ5RRT 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [ERROR] ASIN B08KSKS2PJ 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0033JRP26 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0DSQQ157P 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0FLJM4CMM 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:36 [WARNING] ASIN B0DQDBRL9L 核心字段全部缺失，疑似降级页面 (尝试 2/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0FJW9B76J 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0CLRNN1W4 核心字段全部缺失，疑似降级页面 (尝试 1/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0FB7XYG9L 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0GGQ697TT 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [ERROR] ASIN B0CDZP9BQW 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [ERROR] ASIN B0F9X6ZRSJ 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0F7HFZ8VG 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0FM2C4VYF 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [ERROR] ASIN B0C7MW99YQ 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [ERROR] ASIN B0FLY2M481 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B07FBD4ZG7 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [ERROR] ASIN B0D9LYBTMY 采集失败 (已重试 3 次) [parse_error]
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0D95Y8Z89 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:37 [WARNING] ASIN B0C3YHGGZR 核心字段全部缺失，疑似降级页面 (尝试 3/3)
- `2026-03-16T16:09:34Z` `worker_stage4.log`: 00:08:38 [WARNING] ASIN B0D3WRCDCY 核心字段全部缺失，疑似降级页面 (尝试 1/3)

## Artifacts

- Snapshot log: `.agent/monitor/f005_snapshots.jsonl`
- State file: `.agent/monitor/f005_state.json`
- Runtime logs: `.agent/runtime_logs`

