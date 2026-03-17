# Task Definition

- Mode: review
- Task: Review the full codebase for correctness bugs, cross-module contract mismatches, and coverage gaps
- Constraints:
  - Focus on behavior-affecting issues in the current codebase, not style.
  - Prefer reproduced findings with concrete file/line references.
  - Keep the review centered on `worker/`, `server/`, and `common/` runtime flows.
- Acceptance Criteria:
  - Run a baseline verification pass.
  - Inspect the end-to-end flow across worker task pulling, server coordination, database persistence, screenshot handling, and export.
  - Record active findings and evidence in `.agent/review_list.json` and `.agent/evidence/`.
  - Produce a concise findings-first review for the user.
- Out of Scope:
  - Implementing fixes unless the user explicitly asks for remediation.
