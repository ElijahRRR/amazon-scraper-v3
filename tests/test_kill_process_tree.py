"""
跨平台进程树终止 kill_process_tree 的单元测试（纯 stdlib，桩掉重依赖）。

覆盖之前 Windows 上的真 bug：os.killpg / os.getpgid / signal.SIGKILL 都是 POSIX-only，
在 Windows 上会抛 AttributeError 让关停链崩溃、并把 Chromium 变孤儿累积吃内存。
这里验证：
  - POSIX：pgid 存在 → 走 os.killpg（SIGTERM=优雅 / SIGKILL=强杀）
  - Windows：无 killpg → 走 taskkill /F /T /PID（唯一能连带杀 Chromium 后代的办法）
  - 任何底层异常都不冒泡（会回退，绝不 raise）
  - ProcessLookupError 视作成功；无 pid/pgid 时返回 False
"""
import os
import sys
import types
import signal
import unittest
from unittest import mock

_REPO_ROOT = os.environ.get("REPO_ROOT") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
if not os.path.isdir(os.path.join(_REPO_ROOT, "worker")):
    _REPO_ROOT = os.getcwd()
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules.setdefault(name, m)
    return m


# 只桩掉 engine 模块级 import 的重依赖（本环境未必装）；subprocess/os/signal 用真的。
_stub("aiofiles")
_stub("httpx", AsyncClient=object)
_stub("worker.proxy", get_proxy_manager=lambda: object())
_stub("worker.session", AmazonSession=object)
_stub("worker.parser", AmazonParser=object)
_stub("worker.metrics", MetricsCollector=object)
_stub("worker.adaptive", AdaptiveController=object, TokenBucket=object)

import logging
logging.disable(logging.CRITICAL)

import worker.engine as eng  # noqa: E402
from worker.engine import kill_process_tree, Worker  # noqa: E402


class KillProcessTreeTests(unittest.TestCase):
    def test_posix_group_sigterm(self):
        """POSIX + pgid + force=False → killpg(pgid, SIGTERM)，不碰 os.kill。"""
        calls = []
        fake_os = types.SimpleNamespace(
            killpg=lambda pg, sig: calls.append(("killpg", pg, sig)),
            kill=lambda pid, sig: calls.append(("kill", pid, sig)),
        )
        with mock.patch.object(eng, "os", fake_os), \
                mock.patch.object(eng, "_IS_WINDOWS", False):
            ok = kill_process_tree(pid=111, pgid=111, force=False)
        self.assertTrue(ok)
        self.assertEqual(calls, [("killpg", 111, signal.SIGTERM)])

    def test_posix_group_sigkill_force(self):
        """POSIX + force=True → killpg(pgid, SIGKILL)。"""
        calls = []
        fake_os = types.SimpleNamespace(
            killpg=lambda pg, sig: calls.append(("killpg", pg, sig)),
            kill=lambda pid, sig: calls.append(("kill", pid, sig)),
        )
        with mock.patch.object(eng, "os", fake_os), \
                mock.patch.object(eng, "_IS_WINDOWS", False):
            ok = kill_process_tree(pid=111, pgid=111, force=True)
        self.assertTrue(ok)
        self.assertEqual(calls, [("killpg", 111, eng._sigkill())])

    def test_windows_uses_taskkill_tree(self):
        """Windows（无 killpg）→ taskkill /F /T /PID <pid>，且不回退到 os.kill。"""
        runs = []
        kills = []
        fake_os = types.SimpleNamespace(  # 注意：没有 killpg 属性
            kill=lambda pid, sig: kills.append((pid, sig)),
        )
        fake_subprocess = types.SimpleNamespace(
            run=lambda *a, **k: runs.append((a, k)),
        )
        with mock.patch.object(eng, "os", fake_os), \
                mock.patch.object(eng, "subprocess", fake_subprocess), \
                mock.patch.object(eng, "_IS_WINDOWS", True):
            ok = kill_process_tree(pid=222, pgid=None, force=True)
        self.assertTrue(ok)
        self.assertEqual(len(runs), 1)
        argv = runs[0][0][0]
        self.assertEqual(argv[:3], ["taskkill", "/F", "/T"])
        self.assertIn("222", argv)
        self.assertEqual(kills, [])  # taskkill 成功就不该再 os.kill

    def test_killpg_exception_never_raises_falls_back(self):
        """底层 killpg 抛任意异常 → 不冒泡；POSIX 下回退到 os.kill(pid)。"""
        kills = []

        def boom(*_a):
            raise RuntimeError("boom")

        fake_os = types.SimpleNamespace(
            killpg=boom,
            kill=lambda pid, sig: kills.append((pid, sig)),
        )
        with mock.patch.object(eng, "os", fake_os), \
                mock.patch.object(eng, "_IS_WINDOWS", False):
            ok = kill_process_tree(pid=333, pgid=333, force=True)
        self.assertTrue(ok)
        self.assertEqual(kills, [(333, eng._sigkill())])

    def test_process_lookup_is_success(self):
        """进程已不在（ProcessLookupError）视作成功，不回退、不报错。"""
        def gone(*_a):
            raise ProcessLookupError()

        called = {"kill": 0}
        fake_os = types.SimpleNamespace(
            killpg=gone,
            kill=lambda *_a: called.__setitem__("kill", called["kill"] + 1),
        )
        with mock.patch.object(eng, "os", fake_os), \
                mock.patch.object(eng, "_IS_WINDOWS", False):
            ok = kill_process_tree(pid=444, pgid=444)
        self.assertTrue(ok)
        self.assertEqual(called["kill"], 0)

    def test_nothing_to_kill_returns_false(self):
        """无 pid/pgid → 无操作，返回 False。"""
        self.assertFalse(kill_process_tree(pid=None, pgid=None))

    def test_sigkill_helper_has_fallback(self):
        """_sigkill()：有 SIGKILL 用 SIGKILL，否则退回 SIGTERM（Windows 无 SIGKILL）。"""
        self.assertEqual(eng._sigkill(), getattr(signal, "SIGKILL", signal.SIGTERM))


class ProcTreeRssTests(unittest.TestCase):
    def test_rss_none_without_psutil(self):
        """没装 psutil 时 _proc_tree_rss_mb 返回 None（RSS 闸门/诊断优雅降级）。"""
        with mock.patch.object(eng, "psutil", None):
            self.assertIsNone(Worker._proc_tree_rss_mb(os.getpid()))

    def test_rss_none_for_empty_pid(self):
        self.assertIsNone(Worker._proc_tree_rss_mb(None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
