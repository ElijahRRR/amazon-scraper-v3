"""tests/ 全树共用的夹具。

目前只有一件事：**每个用例跑完把"当前事件循环"补回去**。

为什么需要它（这是实测出来的，不是预防性的）：

``tests/test_session_slot.py:147`` 用的是
``asyncio.get_event_loop().run_until_complete(coro)``。Python 3.11 下，一旦
当前线程的事件循环被置空，这个调用就抛
``RuntimeError: There is no current event loop in thread 'MainThread'``，
那 31 个用例会成片挂掉。

而置空它的路径有好几条，都是正当用法：

* ``asyncio.run(...)`` —— 结束时关闭循环并把当前循环置空。
  ``tests/golden/harness.py`` 的 ``_pg_scratch_db`` 建库/删库就是这么跑的，
  ``tests/test_golden_with_relay.py`` 抽干事件流也是。
* ``pytest-asyncio`` —— 每个 async 用例新建一个循环，结束时关闭并置空。

于是结果变成**收集顺序的函数**：``tests/pgdb/conftest.py`` 里原本有一份同样的
修复，靠的是 pgdb 用例排在 ``test_session_slot`` 前面、跑完顺手把循环补上。
Phase 2 新增的 ``tests/test_event_stream_endpoint.py`` /
``tests/test_golden_with_relay.py`` 按字母序恰好落在 ``tests/pgdb/`` 之后、
``tests/test_session_slot.py`` 之前，那份修复就够不着了——实测 26 个用例转红：

    DB_BACKEND=postgres pytest tests/test_golden_with_relay.py tests/test_session_slot.py
      -> 26 failed, 6 passed        （RuntimeError: There is no current event loop）

所以把修复提到全树级别，而不是让每个新测试文件自己记得。``tests/pgdb/conftest.py``
里那份保持原样：它带着自己那段来龙去脉，重复一次没有代价。

正确的长远解法是把 ``test_session_slot.py`` 从已废弃的 ``get_event_loop()`` 迁走，
但那是 31 个用例的改动，与 Phase 2 无关，不在这里顺手做。
"""
from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(autouse=True)
def _restore_current_event_loop():
    yield
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
