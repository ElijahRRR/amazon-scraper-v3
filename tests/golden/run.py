"""黄金样本 CLI。

  # 对当前实现录制基线（SQLite 版跑一次，PG 移植期间不要再跑）
  python -m tests.golden.run record

  # 校验当前实现是否仍与基线一致（移植期间反复跑）
  python -m tests.golden.run verify

  # 连跑两次 record 比对自身，证明场景是确定性的（不写盘）
  python -m tests.golden.run selfcheck
"""
from __future__ import annotations

import json
import os
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from tests.golden import scenario                       # noqa: E402
from tests.golden.harness import (                      # noqa: E402
    Recorder, diff_steps, isolated_server,
)

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")
BASELINE = os.path.join(SAMPLE_DIR, "sqlite_baseline.json")


def capture(strict: bool = True) -> List[dict]:
    with isolated_server() as (client, ctx):
        rec = Recorder(client, ctx.tmp_root, strict=strict)
        scenario.run(rec)
        return rec.steps


def cmd_record() -> int:
    steps = capture()
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    with open(BASELINE, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "steps": steps}, f,
                  ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    print(f"✅ 已录制 {len(steps)} 步 → {BASELINE}")
    return 0


def cmd_verify() -> int:
    if not os.path.exists(BASELINE):
        print(f"❌ 基线不存在：{BASELINE}\n   先跑 `python -m tests.golden.run record`")
        return 2
    with open(BASELINE, encoding="utf-8") as f:
        golden = json.load(f)["steps"]

    # 非 strict：跑完全程，一次给出全部差异
    actual = capture(strict=False)
    diffs = diff_steps(golden, actual)
    if not diffs:
        print(f"✅ {len(actual)} 步与基线完全一致")
        return 0
    print(f"❌ 与基线有 {len(diffs)} 处差异：\n")
    for line in diffs:
        print(f"  {line}")
    return 1


def cmd_selfcheck() -> int:
    """场景本身必须是确定性的。这一步失败说明夹具还有没擦干净的不可重复源，
    此时录出来的基线是废的——移植时会报一堆假差异，把真差异淹掉。"""
    first = capture()
    second = capture()
    diffs = diff_steps(first, second)
    if not diffs:
        print(f"✅ 两次独立运行完全一致（{len(first)} 步），场景是确定性的")
        return 0
    print(f"❌ 场景不确定：两次运行有 {len(diffs)} 处差异\n")
    for line in diffs:
        print(f"  {line}")
    return 1


def main() -> int:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    return {
        "record": cmd_record,
        "verify": cmd_verify,
        "selfcheck": cmd_selfcheck,
    }.get(cmd, lambda: (print(__doc__), 2)[1])()


if __name__ == "__main__":
    raise SystemExit(main())
