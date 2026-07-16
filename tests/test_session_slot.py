"""
SessionSlot 单元测试（纯 stdlib/asyncio 桩件）。

worker.engine 依赖 aiofiles/httpx/curl_cffi 等（本环境未安装），故在导入前把
engine.py 的模块级依赖用最小 fake 注入 sys.modules，从而加载**真实**的
SessionSlot 代码，再用 FakeWorker + FakeSession 驱动它验证：
  - ensure_ready：懒建 session、epoch 变化强制重建、session 失活自动重建
  - ensure_zip：仅在邮编不同时 POST，且只影响本 slot
  - note_success / should_rotate_proactive：主动轮换计数（阈值下沉到 slot）
  - rotate：冷轮换、5s 防抖、重置计数、设置宽限期、上报被封
  - rotate_on_empty：空标题累计到阈值触发轮换；宽限期内不重复轮换
  - 隔离性：一个 slot 轮换只换它自己的 session
"""
import os
import sys
import types
import asyncio
import unittest

# 让本仓库根目录可导入（真实的 worker / common 包，均为空 __init__ + 纯净 config）。
# 优先按本文件位置推断仓库根（tests/ 的上一级），回退到 REPO_ROOT / cwd。
_REPO_ROOT = os.environ.get("REPO_ROOT") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
if not os.path.isdir(os.path.join(_REPO_ROOT, "worker")):
    _REPO_ROOT = os.getcwd()
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ── 只桩掉 engine 模块级需要、但本环境未装的重依赖子模块 ────────────────────
def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m

_stub("aiofiles")
_stub("httpx", AsyncClient=object)
# worker / common 用真实包（空 __init__）；common.config 只依赖 os，直接用真实的。
_stub("worker.proxy", get_proxy_manager=lambda: object())
_stub("worker.session", AmazonSession=object)
_stub("worker.parser", AmazonParser=object)
_stub("worker.metrics", MetricsCollector=object)
_stub("worker.adaptive", AdaptiveController=object, TokenBucket=object)

# 屏蔽 basicConfig 噪声
import logging
logging.disable(logging.CRITICAL)

from worker.engine import SessionSlot  # noqa: E402


# ── Fakes ───────────────────────────────────────────────────────────────────
class FakeSession:
    _counter = 0

    def __init__(self, zip_code="10001"):
        FakeSession._counter += 1
        self.id = FakeSession._counter
        self.zip_code = zip_code
        self._ready = True
        self.closed = False
        self.change_zip_calls = []
        self.change_zip_ok = True
        self.resend_calls = 0
        self.resend_ok = True

    def is_ready(self):
        return self._ready and not self.closed

    async def change_zip_code(self, target, verify=True):
        self.change_zip_calls.append((target, verify))
        if self.change_zip_ok:
            self.zip_code = target
        return self.change_zip_ok

    async def resend_zip_code(self):
        self.resend_calls += 1
        return self.resend_ok

    async def close(self):
        self.closed = True


class FakeProxyManager:
    def __init__(self):
        self.blocked_reports = 0

    async def report_blocked(self):
        self.blocked_reports += 1


class FakeWorker:
    """只暴露 SessionSlot 用到的接口。"""
    def __init__(self, rotate_every=1000, empty_threshold=15, zip_code="10001"):
        self._restart_epoch = 0
        self._rotate_every = rotate_every
        self._empty_title_rotate_threshold = empty_threshold
        self.zip_code = zip_code
        self.proxy_manager = FakeProxyManager()
        self.created_sessions = []
        self.create_returns_none = False
        self._create_delay_seen = []

    async def _create_session_with_retry(self, delay=5):
        self._create_delay_seen.append(delay)
        if self.create_returns_none:
            return None
        s = FakeSession(zip_code=self.zip_code)
        self.created_sessions.append(s)
        return s


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class SessionSlotTests(unittest.TestCase):
    def setUp(self):
        FakeSession._counter = 0

    # ── ensure_ready ────────────────────────────────────────────────────────
    def test_ensure_ready_lazy_build(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        self.assertIsNone(slot.session)
        ok = run(slot.ensure_ready())
        self.assertTrue(ok)
        self.assertIsNotNone(slot.session)
        self.assertEqual(len(w.created_sessions), 1)
        # 再次调用应复用，不新建
        run(slot.ensure_ready())
        self.assertEqual(len(w.created_sessions), 1)

    def test_ensure_ready_rebuild_on_dead_session(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        first = slot.session
        first._ready = False  # session 失活
        ok = run(slot.ensure_ready())
        self.assertTrue(ok)
        self.assertIsNot(slot.session, first)
        self.assertTrue(first.closed)  # 旧的被关闭
        self.assertEqual(len(w.created_sessions), 2)

    def test_ensure_ready_epoch_change_forces_rebuild(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        first = slot.session
        w._restart_epoch += 1  # 软重启
        run(slot.ensure_ready())
        self.assertIsNot(slot.session, first)
        self.assertTrue(first.closed)
        self.assertEqual(slot._restart_epoch, w._restart_epoch)

    def test_ensure_ready_create_fail_returns_false(self):
        w = FakeWorker()
        w.create_returns_none = True
        slot = SessionSlot(w)
        ok = run(slot.ensure_ready())
        self.assertFalse(ok)
        self.assertIsNone(slot.session)

    # ── ensure_zip ──────────────────────────────────────────────────────────
    def test_ensure_zip_same_zip_no_post(self):
        w = FakeWorker(zip_code="10001")
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        ok = run(slot.ensure_zip("10001"))
        self.assertTrue(ok)
        self.assertEqual(slot.session.change_zip_calls, [])  # 无 POST

    def test_ensure_zip_different_zip_posts(self):
        # 显式固定 standalone，不依赖全局默认（默认已改为 on_fetch）
        import common.config as appcfg
        old = getattr(appcfg, "ZIP_VERIFY_MODE", "on_fetch")
        appcfg.ZIP_VERIFY_MODE = "standalone"
        try:
            w = FakeWorker(zip_code="10001")
            slot = SessionSlot(w)
            run(slot.ensure_ready())
            ok = run(slot.ensure_zip("90210"))
            self.assertTrue(ok)
            # standalone 模式：change_zip_code(verify=True)
            self.assertEqual(slot.session.change_zip_calls, [("90210", True)])
            self.assertEqual(slot.session.zip_code, "90210")
        finally:
            appcfg.ZIP_VERIFY_MODE = old

    def test_ensure_zip_on_fetch_mode_skips_verify(self):
        import common.config as appcfg
        old = getattr(appcfg, "ZIP_VERIFY_MODE", "standalone")
        appcfg.ZIP_VERIFY_MODE = "on_fetch"
        try:
            w = FakeWorker(zip_code="10001")
            slot = SessionSlot(w)
            run(slot.ensure_ready())
            ok = run(slot.ensure_zip("90210"))
            self.assertTrue(ok)
            # on_fetch 模式：change_zip_code(verify=False)，不发独立验证 GET
            self.assertEqual(slot.session.change_zip_calls, [("90210", False)])
        finally:
            appcfg.ZIP_VERIFY_MODE = old

    def test_ensure_zip_post_failure(self):
        w = FakeWorker(zip_code="10001")
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        slot.session.change_zip_ok = False
        ok = run(slot.ensure_zip("90210"))
        self.assertFalse(ok)

    def test_ensure_zip_empty_target_noop(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        ok = run(slot.ensure_zip(""))
        self.assertTrue(ok)
        self.assertEqual(slot.session.change_zip_calls, [])

    # ── repost_zip（on_fetch 未生效时同 session 重发 POST）─────────────────────
    def test_repost_zip_success_reuses_session(self):
        w = FakeWorker(zip_code="90210")
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        old = slot.session
        ok = run(slot.repost_zip("90210"))
        self.assertTrue(ok)
        self.assertIs(slot.session, old)          # 不换 session
        self.assertEqual(slot.session.resend_calls, 1)
        self.assertEqual(w.proxy_manager.blocked_reports, 0)  # 不上报被封

    def test_repost_zip_failure_returns_false(self):
        w = FakeWorker(zip_code="90210")
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        slot.session.resend_ok = False
        ok = run(slot.repost_zip("90210"))
        self.assertFalse(ok)
        self.assertEqual(slot.session.resend_calls, 1)

    def test_repost_zip_no_session_returns_false(self):
        w = FakeWorker()
        slot = SessionSlot(w)  # 未 ensure_ready，session=None
        ok = run(slot.repost_zip("90210"))
        self.assertFalse(ok)

    def test_repost_zip_empty_target_returns_false(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        ok = run(slot.repost_zip(""))
        self.assertFalse(ok)
        self.assertEqual(slot.session.resend_calls, 0)

    # ── 主动轮换计数 ─────────────────────────────────────────────────────────
    def test_note_success_proactive_threshold(self):
        w = FakeWorker(rotate_every=3)
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        self.assertFalse(slot.should_rotate_proactive())
        slot.note_success()
        slot.note_success()
        self.assertFalse(slot.should_rotate_proactive())
        slot.note_success()
        self.assertTrue(slot.should_rotate_proactive())

    def test_rotate_resets_success_counter(self):
        w = FakeWorker(rotate_every=3)
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        slot.note_success(); slot.note_success(); slot.note_success()
        self.assertTrue(slot.should_rotate_proactive())
        run(slot.rotate(reason="主动轮换"))
        self.assertFalse(slot.should_rotate_proactive())

    # ── rotate ──────────────────────────────────────────────────────────────
    def test_rotate_swaps_session_and_reports_blocked(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        old = slot.session
        run(slot.rotate(reason="被封锁"))
        self.assertIsNot(slot.session, old)
        self.assertTrue(old.closed)
        self.assertEqual(w.proxy_manager.blocked_reports, 1)
        self.assertGreater(slot._grace_until, 0)

    def test_rotate_debounce_5s(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        run(slot.rotate(reason="第一次"))
        after_first = slot.session
        # 立即再次轮换应被 5s 防抖跳过
        run(slot.rotate(reason="第二次"))
        self.assertIs(slot.session, after_first)  # 未变
        self.assertEqual(w.proxy_manager.blocked_reports, 1)

    def test_rotate_debounce_expires(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        run(slot.rotate(reason="第一次"))
        first_rotated = slot.session
        # 把上次轮换时间往前拨 6s，绕过防抖
        slot._last_rotate_time -= 6
        run(slot.rotate(reason="第二次"))
        self.assertIsNot(slot.session, first_rotated)
        self.assertEqual(w.proxy_manager.blocked_reports, 2)

    # ── rotate_on_empty（空标题机制1+2）──────────────────────────────────────
    def test_rotate_on_empty_single_trigger(self):
        w = FakeWorker(empty_threshold=15)
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        old = slot.session
        # 单次空标题（未到累计阈值）：机制1 立即轮换
        run(slot.rotate_on_empty("B0TEST", reason="标题为空"))
        self.assertIsNot(slot.session, old)
        self.assertEqual(slot._empty_title_count, 0)  # 轮换后清零

    def test_rotate_on_empty_grace_period_no_double_rotate(self):
        w = FakeWorker(empty_threshold=15)
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        run(slot.rotate_on_empty("B0TEST", reason="标题为空"))  # 设置宽限期
        rotated = slot.session
        reports = w.proxy_manager.blocked_reports
        # 宽限期内再来一次：只累计计数，不轮换
        run(slot.rotate_on_empty("B0TEST2", reason="标题为空"))
        self.assertIs(slot.session, rotated)
        self.assertEqual(w.proxy_manager.blocked_reports, reports)
        self.assertEqual(slot._empty_title_count, 1)

    def test_rotate_on_empty_accumulate_to_threshold(self):
        w = FakeWorker(empty_threshold=3)
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        # 制造"过了宽限期 + 未达防抖"环境：直接调低阈值并绕过防抖/宽限
        # 第一次触发机制1轮换
        run(slot.rotate_on_empty("A1", reason="空"))
        # 绕过防抖与宽限期，让后续 empty 能累计并在阈值处轮换
        slot._last_rotate_time -= 10
        slot._grace_until -= 10
        run(slot.rotate_on_empty("A2", reason="空"))  # count=1
        slot._last_rotate_time -= 10
        slot._grace_until -= 10
        run(slot.rotate_on_empty("A3", reason="空"))  # count=2
        slot._last_rotate_time -= 10
        slot._grace_until -= 10
        pre = slot.session
        run(slot.rotate_on_empty("A4", reason="空"))  # count=3 → 达阈值轮换
        self.assertIsNot(slot.session, pre)

    # ── 隔离性 ───────────────────────────────────────────────────────────────
    def test_two_slots_independent_rotation(self):
        w = FakeWorker()
        a = SessionSlot(w)
        b = SessionSlot(w)
        run(a.ensure_ready())
        run(b.ensure_ready())
        a_sess, b_sess = a.session, b.session
        run(a.rotate(reason="被封锁"))
        # a 换了，b 不受影响
        self.assertIsNot(a.session, a_sess)
        self.assertIs(b.session, b_sess)
        self.assertFalse(b_sess.closed)

    # ── close ───────────────────────────────────────────────────────────────
    def test_close(self):
        w = FakeWorker()
        slot = SessionSlot(w)
        run(slot.ensure_ready())
        s = slot.session
        run(slot.close())
        self.assertTrue(s.closed)
        self.assertIsNone(slot.session)


if __name__ == "__main__":
    unittest.main(verbosity=2)
