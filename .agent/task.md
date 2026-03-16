# Task Definition

- Mode: build
- Task: Monitor the live 260k-item batch with 4 workers and produce a completion-time stability report
- Constraints:
  - Do not interrupt or reset the active large batch unless monitoring is otherwise impossible.
  - Collect evidence from live server/worker behavior, not just static code inspection.
  - Persist monitoring state so the report can still be generated after a long run.
- Acceptance Criteria:
  - Identify the active target batch and expand to 4 workers total.
  - Record minute-level snapshots of batch progress, worker registry state, process health, and live log anomalies until the batch completes.
  - Generate a report at completion focused on stability, throughput, suspected bugs, and operator-visible issues.
- Out of Scope:
  - Product/business decisions about which optimization strategy to apply after the report is delivered.
