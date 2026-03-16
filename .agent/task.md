# Task Definition

- Mode: build
- Task: Fix screenshot_path overwritten after screenshot upload
- Constraints:
  - Keep screenshot uploads authoritative; do not reintroduce result-payload ownership of `screenshot_path`.
  - Limit changes to the screenshot-path race and user-visible result rendering path.
- Acceptance Criteria:
  - `save_result` preserves a completed screenshot path when the screenshot upload arrives before the first `asin_data` insert.
  - Results APIs return a valid screenshot path when `asin_data.screenshot_path` contains an invalid placeholder such as `"None"` but `screenshots.file_path` is present.
  - Verification includes an automated regression test and a direct race repro script.
- Out of Scope:
  - One-off repair of production data not present in the local `data/scraper.db`.
