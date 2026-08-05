"""common/pgdb —— common/database.py 的 PostgreSQL 版实现（**增量**，不改原文件）。

对外只暴露一个东西：``Database``，公开面与 ``common.database.Database`` 逐字相同
（50 个属性：签名、默认值、返回形状、async/sync 性质全部一致）。

    from common.pgdb import Database     # 直接用
    from common.dbfactory import create_database   # 按 DB_BACKEND 选后端（推荐）

--------------------------------------------------------------------------
组装
--------------------------------------------------------------------------
MRO 里 PoolMixin **必须**排第一：__init__ / _db / read / _write_lock / _tx
都由它提供，其余 mixin 只准定义方法、绝不定义 __init__，也绝不自己建连接。

每个方法**有且只有一个** mixin 定义（单一真源）。谁拥有什么见 OWNERSHIP.md，
下面的 _assert_single_owner() 在导入时就把这条规则变成硬约束。
"""
from __future__ import annotations

from common.pgdb.admin import AdminMixin
from common.pgdb.batches import BatchesMixin
from common.pgdb.media import MediaMixin
from common.pgdb.pool import PoolMixin
from common.pgdb.relay import EventStreamMixin
from common.pgdb.results_read import ResultsReadMixin
from common.pgdb.results_write import ResultsWriteMixin
from common.pgdb.retention import RetentionMixin
from common.pgdb.schema import SchemaMixin
from common.pgdb.tasks import TasksMixin


class Database(PoolMixin, SchemaMixin, BatchesMixin, TasksMixin,
               ResultsWriteMixin, ResultsReadMixin, MediaMixin, AdminMixin,
               EventStreamMixin, RetentionMixin):
    """异步 PostgreSQL 数据库管理器 v3（与 SQLite 版公开面等价）。

    ``EventStreamMixin``（Phase 2 事件流）与 ``RetentionMixin``（Phase 6
    保留期 + ack 闩锁）排在最后，且它们的方法**一个都不在 PUBLIC_API 里**
    —— 那个元组是与 SQLite 的对等面契约，事件流与保留期都是 PG 独有的增量，
    不该出现在里面。两道导入期自检对此是安全的：``_assert_api_complete``
    只查"少了没有"，``_assert_single_owner`` 只遍历已在 PUBLIC_API 里的名字，
    所以新方法能干净地导进来，同时仍然受重复定义检查保护。

    ``RetentionMixin`` 排在 ``EventStreamMixin`` 之后是有意的：它复用后者的
    ``_seq_high_water()``（判「这个分区还会不会收到新行」的唯一正确判据），
    自己一个字都不重复实现。
    """


# ------------------------------------------------------------------
# 公开面自检：导入即执行，实现者写错了立刻炸，而不是等黄金校验才发现
# ------------------------------------------------------------------

#: common/database.py Database 的 50 个公开属性。
#: 这份清单是契约，改动它 = 改动 API。
PUBLIC_API = (
    # 生命周期 / 基础设施
    "__init__", "connect", "_open_read_pool", "read", "run_startup_optimize",
    "maintenance_loop", "start_maintenance", "close", "wal_checkpoint",
    "init_tables",
    # 批次
    "create_batch", "get_batches", "get_batch_by_name", "expand_batch_variants",
    "get_batch_completion_status", "mark_batch_completed", "list_callback_due",
    "mark_callback_attempt", "reset_callback_for_retry",
    # 任务
    "create_tasks", "pull_tasks", "reclaim_dead_worker_tasks",
    "auto_retry_failed_tasks", "fail_task", "release_tasks", "prioritize_batch",
    "get_progress",
    # 卖家（F-009）
    "create_seller_batch", "accept_seller_discovery_result",
    "get_seller_batch_progress",
    # 结果写入
    "accept_success_result", "accept_results_batch", "accept_failed_result",
    "_save_result_inner_unlocked", "save_result", "save_results_batch",
    # 截图路径辅助
    "_get_done_screenshot_path", "_get_done_screenshot_paths",
    "_hydrate_screenshot_paths",
    # 结果读取
    "get_batch_failures", "get_results", "get_result_by_asin", "get_asin_changes",
    "iter_results",
    # 截图
    "get_pending_screenshots", "update_screenshot_status", "get_screenshot_progress",
    # 统计
    "get_total_asins", "get_all_asins", "get_change_stats",
)

_MIXINS = (PoolMixin, SchemaMixin, BatchesMixin, TasksMixin, ResultsWriteMixin,
           ResultsReadMixin, MediaMixin, AdminMixin, EventStreamMixin,
           RetentionMixin)


def _assert_api_complete():
    missing = [n for n in PUBLIC_API if not hasattr(Database, n)]
    if missing:
        raise AssertionError(f"common.pgdb.Database 缺少公开方法: {missing}")


def _assert_single_owner():
    """同一个方法只准被一个 mixin 定义。

    多重继承下重复定义不会报错，只会被 MRO 静默遮蔽——那正是"改了 A 文件却
    没生效"这类事故的来源，所以在导入期就把它挡掉。
    """
    seen = {}
    dupes = []
    for mixin in _MIXINS:
        for name in vars(mixin):
            if name.startswith("__") or name not in PUBLIC_API:
                continue
            if name in seen:
                dupes.append(f"{name}: {seen[name].__name__} 与 {mixin.__name__}")
            else:
                seen[name] = mixin
    if dupes:
        raise AssertionError("方法被多个 mixin 重复定义（单一真源被破坏）:\n  "
                             + "\n  ".join(dupes))


_assert_api_complete()
_assert_single_owner()

__all__ = ["Database", "PUBLIC_API"]
