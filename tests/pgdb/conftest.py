"""tests/pgdb 的公共夹具。

只在 PostgreSQL 可达时跑；连不上就 skip，这样 SQLite-only 的开发机上
``pytest`` 依然是绿的。

⚠ 这里**不能**在导入/收集期跑 ``asyncio.run(...)`` 做连通性探测：
它结束时会把当前事件循环置空，而 tests/test_session_slot.py 里的
``asyncio.get_event_loop().run_until_complete(...)`` 依赖那个循环存在——
实测会让那 31 个用例全挂。所以探测挪进夹具里，用例真要用库时才连。
"""
from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

pytest.importorskip("asyncpg", reason="tests/pgdb 需要 asyncpg")

from tests.pgdb.helpers import scratch_database  # noqa: E402


@pytest.fixture(autouse=True)
def _restore_current_event_loop():
    """跑完把"当前事件循环"补回去，别殃及后面的同步用例。

    pytest-asyncio 每跑一个 async 用例就新建一个事件循环、结束时关闭它并把
    当前循环置空。而 tests/test_session_slot.py 用的是
    ``asyncio.get_event_loop().run_until_complete(...)``，在收集顺序上排在
    tests/pgdb 之后 —— 不补这一手，那 31 个用例会以
    "RuntimeError: There is no current event loop" 全挂。
    这是本目录自己造成的副作用，就在本目录消化掉，不去改别人的测试。
    """
    yield
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


# 仓库用的是 pytest-asyncio 的 **strict** 模式（pytest.ini 里没有
# asyncio_mode=auto——实测打开它会让 tests/test_session_slot.py 的 31 个用例全挂）。
# 所以：async 夹具用 @pytest_asyncio.fixture，async 用例逐个 @pytest.mark.asyncio。
@pytest_asyncio.fixture
async def pgdb(request):
    """一个全新的、已建表的 common.pgdb.Database；用例结束即删库。"""
    label = request.node.name[:24]
    try:
        cm = scratch_database(label)
        db = await cm.__aenter__()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"PostgreSQL 不可达，跳过 pgdb 用例: {type(e).__name__}: {e}")
        return
    try:
        yield db
    finally:
        await cm.__aexit__(None, None, None)


@pytest_asyncio.fixture
async def pgconn(pgdb):
    """pgdb 的裸 asyncpg 写连接（要直接验证落库内容时用）。"""
    return pgdb._write_conn
