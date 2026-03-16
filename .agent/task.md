# Task Definition

- Mode: build
- Task: Prevent screenshot subprocess browser leaks and CPU spikes during stop/restart
- Constraints:
  - Keep screenshot architecture as a subprocess; do not fold Playwright back into the main worker loop.
  - Fix lifecycle from the stop/restart path first, not by weakening screenshot gating.
- Acceptance Criteria:
  - Restarting or stopping the screenshot subprocess also reaps its descendant browser processes.
  - Screenshot child handles SIGTERM/SIGINT by requesting graceful shutdown instead of immediate `sys.exit()`.
  - Screenshot workers do not share a global cache directory that allows another subprocess to steal pending HTML files.
  - Verification includes a focused process-group cleanup repro plus syntax validation.
- Out of Scope:
  - Redesigning screenshot retry semantics or batch-completion policy in this change.
