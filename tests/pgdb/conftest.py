"""tests/pgdb 的公共夹具。

只在 PostgreSQL 可达时跑；连不上就 skip，这样 SQLite-only 的开发机上
``pytest`` 依然是绿的。

⚠ 这里**不能**在导入/收集期跑 ``asyncio.run(...)`` 做连通性探测：它结束时会
关闭自己的循环。探测因此挪进夹具里，用例真要用库时才连——这条约束独立于下面
那段历史，仍然有效（收集期建库会让「连不上 PG 就 skip」退化成「收集就炸」）。

历史（B6）：这里曾有一个 autouse 夹具 ``_restore_current_event_loop``，
在每个用例后把「当前线程事件循环」补回去，服务的是
``tests/test_session_slot.py`` 里依赖该全局槽位的 ``run()`` 助手。
那个依赖已在源头去掉（见该文件 ``run()`` 上方注释），此夹具随之成为死代码并被删除。
它同时也是个陷阱：``conftest.py`` 只有 pytest 读，``unittest discover`` 不读，
留着它等于让 pytest 比 unittest 更宽容，缺陷只在 unittest 侧现形。
删除前后都实测过，见 ``tests/conftest.py`` 的模块文档。
"""
from __future__ import annotations

import pytest
import pytest_asyncio

pytest.importorskip("asyncpg", reason="tests/pgdb 需要 asyncpg")

from tests.pgdb.helpers import scratch_database  # noqa: E402


# 仓库用的是 pytest-asyncio 的 **strict** 模式（pytest.ini 里没有 asyncio_mode=auto）。
# 所以：async 夹具用 @pytest_asyncio.fixture，async 用例逐个 @pytest.mark.asyncio。
#
# 这行原本还带一句「实测打开 asyncio_mode=auto 会让 test_session_slot 的 31 个用例全挂」。
# 那句话现在**不成立了**，一并更正：auto 模式让 pytest-asyncio 给每个 async 用例
# 建/关一个循环，于是更频繁地把「当前事件循环」槽位置空——挂掉的根因是那个已被
# 移除的全局槽位依赖（见 tests/conftest.py），不是 auto 模式本身。修好之后实测：
#
#   pytest tests/ -q                       -> 444 passed, 31 skipped
#   pytest tests/ -q -o asyncio_mode=auto  -> 444 passed, 31 skipped   （逐条同结果）
#
# 记录事实而已，**不是**在建议改模式：strict 是显式声明，仍然是这里的选择。
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
