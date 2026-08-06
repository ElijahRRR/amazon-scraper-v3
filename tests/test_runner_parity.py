"""两个 runner 的等价性看守（B6）。

本仓库有两个 runner，两个都必须在两个后端上是绿的：

    python -m pytest tests/ -q
    python -m unittest discover -s tests

它们看到的东西并不一样，而差异曾经悄悄地把一个真缺陷藏起来（B6）：
``conftest.py`` 只有 pytest 读，于是一个「只在 pytest 下生效的修复」让
``pytest`` 全绿、``unittest discover`` 在 Postgres 下 26 个 error。
本文件把两条容易再次滑掉的性质钉成用例——它们是 ``unittest.TestCase``，
所以两个 runner 都会执行，不存在「只有一边看得到」。

看守一：没有任何测试依赖「当前线程事件循环」这个全局槽位。
看守二：``unittest discover`` 收不到的用例，必须**说出来**，不许无声消失。
看守三：没有任何测试在模块级**无条件**把一个真的装得上的模块换成桩件（D-53）。

同一个病根的第三次发作：**测试结果不许是收集顺序的函数。**
"""
from __future__ import annotations

import ast
import asyncio
import importlib
import os
import subprocess
import sys
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


def _iter_test_sources():
    """tests/ 下所有 .py 的 (相对路径, 源码)。"""
    for root, dirs, files in os.walk(_TESTS_DIR):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for fn in sorted(files):
            if not fn.endswith(".py"):
                continue
            path = os.path.join(root, fn)
            with open(path, encoding="utf-8") as fh:
                yield os.path.relpath(path, _TESTS_DIR), fh.read()


class EventLoopOwnershipTests(unittest.TestCase):
    """没有任何测试可以依赖 asyncio 的「当前线程事件循环」全局槽位。

    背景：任何一句 ``asyncio.run(...)`` 结束时都会关闭自己的循环并把该槽位置空。
    本仓库有好几处正当的 ``asyncio.run``（golden 建/删库、事件流抽干、
    pytest-asyncio 的每个 async 用例）。一旦有用例读那个槽位，它是否通过就成了
    「收集顺序 × 后端 × runner」的函数——这正是 B6 的成因。
    """

    def test_no_test_file_calls_asyncio_get_event_loop(self):
        """静态看守：tests/ 里不许出现 ``asyncio.get_event_loop()`` 调用。

        用 AST 而不是 grep，所以注释和文档字符串里提这个名字不算数——本仓库
        好几处注释正是在解释「为什么不要用它」，不该被自己的看守绊倒。
        """
        offenders = []
        for relpath, src in _iter_test_sources():
            if relpath == os.path.basename(__file__):
                continue
            tree = ast.parse(src, filename=relpath)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                fn = node.func
                # asyncio.get_event_loop()  /  get_event_loop()
                name = None
                if isinstance(fn, ast.Attribute):
                    name = fn.attr
                elif isinstance(fn, ast.Name):
                    name = fn.id
                if name == "get_event_loop":
                    offenders.append(f"{relpath}:{node.lineno}")
        self.assertEqual(
            offenders, [],
            "这些地方调用了 asyncio.get_event_loop()，它读的是进程级全局槽位，"
            "任何 asyncio.run(...) 都会把它置空 —— 这是 B6 的成因，pytest 侧还会被 "
            "conftest 夹具盖住，只有 unittest discover 会翻红。\n"
            "要跑协程请用 asyncio.run(...)，或像 tests/test_session_slot.py 的 "
            f"run() 那样自持一个循环。违规处：{offenders}",
        )

    def test_session_slot_run_helper_survives_a_cleared_event_loop(self):
        """行为看守：把槽位清空（``asyncio.run`` 留下的正是这个状态），

        ``test_session_slot.run()`` 仍然要能跑协程。还原这条断言就等于还原 B6：
        在修复前，这里会抛 ``RuntimeError: There is no current event loop``。
        """
        mod = None
        for name in ("test_session_slot", "tests.test_session_slot"):
            try:
                mod = importlib.import_module(name)
                break
            except ImportError:
                continue
        self.assertIsNotNone(
            mod, "找不到 test_session_slot 模块——两个 runner 的模块名都试过了")

        previous = None
        try:
            previous = asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            previous = None

        # 敌对条件：一句 asyncio.run(...) 跑完之后现场就是这样。
        asyncio.set_event_loop(None)

        async def _probe():
            await asyncio.sleep(0)
            return 42

        try:
            self.assertEqual(mod.run(_probe()), 42)
        finally:
            if previous is not None and not previous.is_closed():
                asyncio.set_event_loop(previous)


class ModuleStubLeakTests(unittest.TestCase):
    """桩件是用来「补上没装的」，不是「顶掉装了的」（D-53）。

    背景（Phase 4 收口实测）：``tests/test_session_slot.py`` 在模块级无条件
    ``_stub("worker.parser", AmazonParser=object)``。而 pytest 在**收集期**就
    import 全部测试文件，``worker/engine.py:325`` 的
    ``from worker.parser import AmazonParser`` 又是**模块级绑定**——于是先被收集的
    那个文件说了算，``tearDownModule`` 还原 ``sys.modules`` 也换不回已经绑好的名字::

        pytest tests/test_session_slot.py tests/test_engine_not_found.py  -> 25 failed
        pytest tests/test_engine_not_found.py tests/test_session_slot.py  -> 75 passed

    默认字母序恰好是安全的那一种，所以六道门全绿、缺陷完全不可见——与 B6
    （conftest 只有 pytest 读）、D-27（事件循环全局槽位）是同一个病根的第三次发作。

    看守方式是 AST 而不是「跑一遍看红不红」：那样只有在**不利的收集顺序**下才会
    翻红，等于把看守本身也变成收集顺序的函数。
    """

    #: 只查这几个包——桩第三方库（httpx / aiofiles / curl_cffi）是正当的，
    #: 本仓库自己的模块被顶掉才是问题。
    _OWN_PACKAGES = ("worker.", "common.", "server.")

    @staticmethod
    def _is_really_importable(modname):
        """在**干净的子进程**里判断这个模块能不能 import。

        用子进程而不是当场 ``importlib.import_module``：本用例的运行时机不确定，
        当场 import 会往 ``sys.modules`` 里塞东西，等于用一个顺序依赖去查另一个
        顺序依赖。``find_spec`` 也不行——``worker/session.py`` 文件在（有 spec），
        但它模块级 ``from curl_cffi import ...``，本环境装不上，桩它是正当的。
        """
        proc = subprocess.run(
            [sys.executable, "-c", f"import {modname}"],
            cwd=os.path.dirname(_TESTS_DIR),
            capture_output=True, timeout=120,
        )
        return proc.returncode == 0

    def test_no_test_file_unconditionally_stubs_an_importable_own_module(self):
        offenders = []
        for relpath, src in _iter_test_sources():
            if relpath == os.path.basename(__file__):
                continue
            tree = ast.parse(src, filename=relpath)
            # **只**看模块体的直接子语句。包在 try/except ImportError 里的桩
            # （tests/test_engine_not_found.py 对 worker.session 就是这么写的）
            # 是有条件的，正是我们希望的写法，不算违规。
            for node in tree.body:
                if not (isinstance(node, ast.Expr)
                        and isinstance(node.value, ast.Call)):
                    continue
                call = node.value
                fn = call.func
                name = fn.attr if isinstance(fn, ast.Attribute) else (
                    fn.id if isinstance(fn, ast.Name) else None)
                if name != "_stub":          # _stub_if_missing 是对的，放行
                    continue
                if not (call.args and isinstance(call.args[0], ast.Constant)
                        and isinstance(call.args[0].value, str)):
                    continue
                target = call.args[0].value
                if not target.startswith(self._OWN_PACKAGES):
                    continue
                if self._is_really_importable(target):
                    offenders.append(f"{relpath}:{node.lineno} -> {target}")

        self.assertEqual(
            offenders, [],
            "这些地方在模块级**无条件**把一个真的装得上的本仓库模块换成了桩件。\n"
            "pytest 在收集期 import 全部测试文件，被顶掉的模块会被其它文件的模块级 "
            "`from X import Y` 绑死，`tearDownModule` 还原不回来 —— 于是用例是否通过"
            "取决于收集顺序（D-53 实测：25 failed vs 75 passed，只差文件顺序）。\n"
            "改用 `_stub_if_missing(...)`：装了就用真的，没装才补桩。\n"
            f"违规处：{offenders}",
        )


class PytestOnlySuitesTests(unittest.TestCase):
    """``unittest discover`` 收不到的用例，必须留下声音。

    ``tests/pgdb/``、``tests/golden/``、``tests/test_slowhash.py`` 全部是
    pytest 函数式用例（裸 ``def test_*`` + 夹具），``unittest`` 的加载器只认
    ``TestCase`` 子类，所以**一条都收不到**。这是结构性的、早于 Phase 2 就存在的
    事实，不是回归；但它意味着

        python -m unittest discover -s tests   ->   OK

    这行绿字覆盖的用例数远小于全仓库。谁把它当成「仓库是绿的」就会漏掉整个
    Postgres 存储层。所以这里显式 skip 并报数，而不是让它们无声消失。

    要真正跑这些用例，只有 pytest：``python -m pytest tests/ -q``
    （两个后端都要跑，见仓库 gate）。
    """

    #: 这些路径下的用例 unittest 结构性收不到。
    _PYTEST_ONLY = ("pgdb", "golden", "test_slowhash.py")

    def _count_function_style_tests(self):
        counts = {}
        for relpath, src in _iter_test_sources():
            head = relpath.split(os.sep)[0]
            key = head if head in self._PYTEST_ONLY else None
            if key is None and relpath in self._PYTEST_ONLY:
                key = relpath
            if key is None:
                continue
            tree = ast.parse(src, filename=relpath)
            n = sum(
                1 for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name.startswith("test")
            )
            counts[key] = counts.get(key, 0) + n
        return counts

    def test_pytest_only_suites_are_announced_not_silently_skipped(self):
        counts = self._count_function_style_tests()
        total = sum(counts.values())
        detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        # 收不到就是收不到，不假装能跑；但要报出规模和跑法。
        self.skipTest(
            f"{total} 个函数式用例（{detail}）是 pytest 夹具驱动的，"
            "unittest 的加载器只认 TestCase 子类，结构性收不到。"
            "跑它们只能用：python -m pytest tests/ -q"
            "（以及 DB_BACKEND=postgres 再跑一遍）。"
        )


if __name__ == "__main__":
    unittest.main()
