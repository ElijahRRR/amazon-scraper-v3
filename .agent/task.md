# Task Definition

- Mode: build
- Task: Verify the newly added server-triggered worker soft-restart functionality
- Constraints:
  - Prefer end-to-end verification over static inspection only.
  - Keep the verification isolated from any long-running production batch state.
  - If a defect is found, fix only what is necessary to make the restart path usable and re-verify.
- Acceptance Criteria:
  - A server-side restart request reaches the target worker through `/api/worker/sync`.
  - The worker performs `_soft_restart()` successfully, recreates its session, and remains online.
  - Verification includes concrete server responses and worker log evidence.
- Out of Scope:
  - Broader worker-management redesign beyond validating this restart feature.
