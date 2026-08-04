"""pytest 入口：让黄金样本能进 CI / 常规 `pytest tests/`。

  pytest tests/golden/

`test_scenario_is_deterministic` 是**前置守卫**：它一旦失败，
`test_matches_baseline` 的结果就没有意义（差异全是噪声，真 bug 会被淹掉）。
"""
from __future__ import annotations

import json
import os

import pytest

from tests.golden.harness import diff_steps
from tests.golden.run import BASELINE, capture


@pytest.mark.skipif(not os.path.exists(BASELINE),
                    reason="基线未录制，先跑 python -m tests.golden.run record")
def test_matches_baseline():
    with open(BASELINE, encoding="utf-8") as f:
        golden = json.load(f)["steps"]
    diffs = diff_steps(golden, capture(strict=False))
    assert not diffs, "对外行为与基线不一致：\n" + "\n".join(f"  {d}" for d in diffs)


@pytest.mark.slow
def test_scenario_is_deterministic():
    diffs = diff_steps(capture(), capture())
    assert not diffs, "场景不确定，基线不可信：\n" + "\n".join(f"  {d}" for d in diffs)
