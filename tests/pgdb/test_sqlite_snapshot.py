"""tests/pgdb/test_sqlite_snapshot.py —— PG 侧对 SQLite 裁判快照（Phase 0.2）。

与隔壁那 88 条差分用例的区别只有一个：**裁判在盘上，不在同一次运行里**。

  * 差分用例：同一次运行里现跑一遍 SQLite，两边比。SQLite 夹具一旦没了，网也没了；
    而且两边同时改错（比如共用的 _seed 出问题）它是绿的。
  * 这里：期望值是 `tests/pgdb/snapshots/*.json`，录制于**改动之前**。
    Phase 1 的三条查询改写号称「可证明等价」—— 等价性推理错了，这里会红。
    黄金基线不会（它的搜索样本全是单一大小写 ASCII、brand 恒 GoldenBrand）。

**两套并存，本轮不拆。** 没有删 seeded_sqlite 夹具、没有改任何既有差分用例：
SQLite 去留未决，删夹具等于提前替它做决定。

一条用例内部循环比全部 93 条（而不是 parametrize 成 93 条），是为了共用一个临时 PG 库：
这些 case 一行都不写库，93 个 scratch database 只会多花约 50s（实测每条 setup ~0.55s），
买不到任何隔离。红的时候一次列出全部差异，也比逐条红更好读。
"""
from __future__ import annotations

import os

import pytest

pytest.importorskip("asyncpg", reason="tests/pgdb 需要 asyncpg")

from tests.pgdb import sqlite_snapshot as S  # noqa: E402


def _skip_if_pg_unreachable(e: BaseException) -> None:
    if "asyncpg" in type(e).__module__ or "Connect" in type(e).__name__:
        pytest.skip(f"PostgreSQL 不可达: {type(e).__name__}: {e}")
    raise e


@pytest.mark.asyncio
async def test_results_read_matches_sqlite_snapshot():
    """93 条读方法调用，PG 侧必须与录制的 SQLite 返回逐字节相同。

    红了先假定是 PG 改错了 —— 快照不是「我们认为对的」，是 SQLite 当时的真实返回。
    真要改期望值，先说明改的是哪条行为，再
    ``python -m tests.pgdb.sqlite_snapshot record``（只在 SQLite 侧跑）。
    """
    assert os.path.exists(S.RESULTS_READ_SNAPSHOT), S.RESULTS_READ_SNAPSHOT
    golden = S.load(S.RESULTS_READ_SNAPSHOT)
    assert golden["expected_source"] == "sqlite"
    try:
        actual = await S.capture_results_read_pg()
    except Exception as e:  # noqa: BLE001
        _skip_if_pg_unreachable(e)
        return
    # 条数也钉住：用例清单被删掉几条时，只比交集会静默变绿
    assert sum(len(v) for v in actual.values()) == 93
    problems = S.diff_results_read(golden["groups"], actual)
    assert not problems, ("PG 与 SQLite 快照有 %d 处不符:\n  " % len(problems)
                          + "\n  ".join(problems))


@pytest.mark.asyncio
async def test_admin_matches_sqlite_snapshot():
    """test_admin.py 的 scenario() 57 步，PG 侧必须与录制的 SQLite 输出逐条相同。"""
    assert os.path.exists(S.ADMIN_SNAPSHOT), S.ADMIN_SNAPSHOT
    golden = S.load(S.ADMIN_SNAPSHOT)
    assert golden["expected_source"] == "sqlite"
    try:
        actual = await S.capture_admin_pg()
    except Exception as e:  # noqa: BLE001
        _skip_if_pg_unreachable(e)
        return
    problems = S.diff_admin(golden["steps"], actual)
    assert not problems, ("PG 与 SQLite 快照有 %d 处不符:\n  " % len(problems)
                          + "\n  ".join(problems))


def test_snapshot_case_list_still_matches_the_diff_tests():
    """快照的 case 清单是从既有差分用例的 parametrize 里读出来的。

    这条守的是「有人加了差分用例但没重录快照」：那时快照会少几条，
    上面两条用例的 key 比对会红，但红在别处不好读，这里先说清楚。
    """
    cases = S.build_cases()
    golden = S.load(S.RESULTS_READ_SNAPSHOT)["groups"]
    missing = [f"{g}[{c}]" for g, c, *_ in cases if c not in golden.get(g, {})]
    extra = [f"{g}[{c}]" for g, m in golden.items()
             for c in m if (g, c) not in {(x[0], x[1]) for x in cases}]
    assert not missing and not extra, (
        f"快照与用例清单对不上：缺 {missing}；多 {extra}\n"
        "  加/删差分用例之后要重录：python -m tests.pgdb.sqlite_snapshot record")


def test_snapshot_header_is_present():
    """文件头是这份快照的一半价值：写明裁判是 SQLite，以及每批夹具数据守的是什么。"""
    for path in (S.RESULTS_READ_SNAPSHOT, S.ADMIN_SNAPSHOT):
        head = "\n".join(S.load(path)["__header__"])
        assert "录制品" in head and "不是手写的期望值" in head, path
        assert "SQLite" in head, path
    head = "\n".join(S.load(S.RESULTS_READ_SNAPSHOT)["__header__"])
    for token in ("B0BSLASH05", "D-16", "B0ACCENT06", "B0ACCENT07", "D-5",
                  "B0PCT08", "Pct_Brand", "D-8"):
        assert token in head, f"文件头漏了 {token}"
