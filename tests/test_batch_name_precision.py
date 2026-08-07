"""批次名构造点的守卫 —— Phase 4.7 的行为改动本来没有网。

Phase 4.5-4.8 审计的变异 M3 实测：把 `_batch_name` 改回分钟精度，
**八道命令全绿**（golden 两列 + pytest 两列 + unittest 两列 + tests/pgdb）。
也就是说 4.7 那条有意的行为改动完全没有回归保护 ——
将来谁把它改回去、或新加第 6 个调用点时又手写一份分钟精度的 f-string，
门禁不会响。

而它要防的缺陷是**真的**（审计实测复现）：
    create_batch('dup_name') 连调两次 -> 两次都返回 id=1
    两次 create_tasks 各插 1 条     -> 该批次任务总数 = 2，batches 总行数 = 1
即撞名时新 ASIN 被**并进上一个批次**，接口照样回 200 和那个既有的 batch_id。
所以「同一分钟内自动触发 + 手动触发 = 什么都不做」只在 ASIN 清单没变时成立。

写成 unittest.TestCase 是为了让四道 runner 门（pytest × 2 列、
unittest discover × 2 列）都盖得到 —— 裸 test_ 函数会在 unittest 那两列被静默跳过。
"""
import re
import unittest

from server.app import _batch_name

#: <prefix>_YYYYMMDD_HHMMSS —— 日期 8 位、时间**必须 6 位**（秒精度）。
_BATCH_NAME_RE = re.compile(r"^(?P<prefix>.+)_\d{8}_\d{6}$")


class BatchNamePrecisionTests(unittest.TestCase):

    def test_is_second_precision(self):
        """时间段必须是 6 位。分钟精度（4 位）在这里当场红。"""
        for prefix in ("batch", "sellers", "all", "auto_daily", "auto_我的任务"):
            with self.subTest(prefix=prefix):
                name = _batch_name(prefix)
                m = _BATCH_NAME_RE.match(name)
                self.assertIsNotNone(
                    m, f"{name!r} 不是 <prefix>_YYYYMMDD_HHMMSS —— "
                       f"若时间段只有 4 位，说明有人把秒精度改回了分钟精度")
                self.assertEqual(m.group("prefix"), prefix)
                # 显式再钉一次位数，免得上面的正则哪天被放宽
                self.assertEqual(len(name.rsplit("_", 1)[-1]), 6, name)

    def test_all_call_sites_go_through_the_helper(self):
        """仓库里不许再出现手写的批次名格式串。

        这条比格式断言更要紧：4.7 收的是**五处各写各的**，
        新加第六处时若又手写一份，格式断言看不见，这条能看见。
        """
        import pathlib

        # 不带 %S 的日期时间格式串 = 分钟精度批次名的特征
        minute_fmt = re.compile(r"%Y%m%d_%H%M(?!%S)")
        root = pathlib.Path(__file__).resolve().parent.parent
        offenders = []
        for py in list((root / "server").rglob("*.py")) + list((root / "common").rglob("*.py")):
            text = py.read_text(encoding="utf-8")
            for i, line in enumerate(text.splitlines(), 1):
                if minute_fmt.search(line):
                    offenders.append(f"{py.relative_to(root)}:{i}: {line.strip()}")
        self.assertEqual(offenders, [], "发现分钟精度的批次名格式串:\n  " + "\n  ".join(offenders))


if __name__ == "__main__":
    unittest.main()
