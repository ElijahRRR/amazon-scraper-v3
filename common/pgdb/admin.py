"""common/pgdb/admin.py —— SQLite 专有维护接口的 PG 侧等价物（全部实现完毕）。

OWNS（4 个方法）:
    run_startup_optimize   database.py:372   -> ANALYZE / 日志，不再是 PRAGMA optimize
    maintenance_loop       database.py:385   -> 空转（PG 没有 WAL checkpoint 饥饿问题）
    start_maintenance      database.py:402   -> **同步方法**（app.py:171 不 await）
    wal_checkpoint         database.py:426   -> 恒返回 None

这四个在 PG 下没有语义，但**公开面必须存在**：
  * server/app.py:171 ``db.start_maintenance(checkpoint_interval=120)`` —— 不 await，
    所以它必须是 ``def`` 而不是 ``async def``。
  * server/app.py:174 ``asyncio.create_task(db.run_startup_optimize())``。
  * server/app.py:306-310 ``res = await db.wal_checkpoint(mode)`` 之后读
    ``res[0] / res[1] / res[2]``，但外面裹了 ``if res:``，返回 None 是安全的。
  * tests/golden/harness.py 会按**类属性**把 start_maintenance 与
    run_startup_optimize 换成 no-op（已改成按 DB_BACKEND 选到的类打补丁）。

⚠ 这两个方法**绝不能**去拿 ``self._write_lock``。
SQLite 版用的是 ``_write_lock("optimize")`` / ``_write_lock("checkpoint")``，
一旦记录了样本，``/api/_debug/lock-stats`` 的 waits/holds 就会多出
``optimize`` / ``checkpoint`` 两个 key —— 基线里只有
{accept_results_batch, other, pull_tasks} 三个，多一个就是"新增字段"差异。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class AdminMixin:
    """只定义方法，绝不定义 __init__。"""

    async def run_startup_optimize(self):
        """SQLite 的 ``PRAGMA optimize`` 在 PG 里对应 ANALYZE（刷新规划器统计）。

        失败只告警，与 SQLite 版一致——它绝不能挡住启动。
        """
        try:
            conn = self._write_conn
            if conn is None:
                return
            await conn.execute("ANALYZE")
            logger.info("✅ 启动期 ANALYZE 完成")
        except Exception as e:  # noqa: BLE001
            logger.warning("启动期 ANALYZE 异常: %s", e)

    async def maintenance_loop(self, checkpoint_interval: int = 120):
        """PG 没有 WAL 文件膨胀 / checkpoint 饥饿的问题（autovacuum + 后台
        checkpointer 由服务端负责）。保留协程形状，纯空转，永不返回。"""
        logger.info("🔧 维护协程启动（PG 后端：无 WAL checkpoint 需求，仅空转）")
        while True:
            await asyncio.sleep(checkpoint_interval)

    def start_maintenance(self, checkpoint_interval: int = 120):
        """**同步**方法（app.py:171 不 await）。幂等。"""
        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(
                self.maintenance_loop(checkpoint_interval))

    async def wal_checkpoint(self, mode: str = "PASSIVE") -> Optional[tuple]:
        """PG 无对应物，恒返回 None。

        两个调用点都已用 ``if res:`` / ``if res and res[0] == 1:`` 守住
        （server/app.py:307、common/database.py:396），返回 None 是安全的。
        """
        return None
