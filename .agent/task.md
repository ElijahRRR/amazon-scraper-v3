# Task Definition

- Mode: review
- Task: Review the full current codebase after the March 18 task-lifecycle changes
- Constraints:
  - Focus on correctness regressions, state-machine holes, API contract mismatches, and missing coverage.
  - Prefer reproduced findings with file/line references over speculation.
  - Treat the current `main` branch as the review target, not the earlier remote-runtime monitoring task.
- Acceptance Criteria:
  - Run baseline verification for the current codebase.
  - Re-review the task lifecycle across `worker/engine.py`, `server/app.py`, and `common/database.py`.
  - Record active findings and evidence in `.agent/review_list.json` and `.agent/evidence/`.
  - Produce a findings-first review for the user.
- Out of Scope:
  - Implementing fixes unless the user explicitly asks for remediation.
