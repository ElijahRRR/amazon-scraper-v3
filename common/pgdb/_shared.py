"""common/pgdb/_shared.py —— 与 SQLite 实现**共享**的纯 Python 符号（唯一真源）。

本模块**只做再导出**，不得定义任何常量/函数的副本。

理由（规格里已经论证过，这里复述以免后来人"顺手抄一份"）：

* ``LOCK_STATS`` / ``record_stage`` —— ``/api/_debug/lock-stats``
  在 server/app.py:2625 里是 ``from common.database import LOCK_STATS``，
  按**模块全局对象**读。pgdb 若自己建一份，那个端点永远返回空容器，
  黄金基线 step 56 立刻炸（waits/holds/stage_timings 七个 key 全部"字段消失"）。
* ``ASIN_DATA_FIELDS`` / ``_ASIN_DATA_COLUMN_SET`` —— 驱动 ``_save_result_inner_unlocked``
  的动态列清单与 ``iter_results`` 的投影白名单。分叉 = 两个存储后端写入的列集悄悄不同。
* ``_compute_content_hash`` / ``_compute_title_bullets_hash`` —— md5 over ``"|".join(...)``。
  字段表分叉 = 每一条 hash 变、每一条 asin_changes 变。
* ``_is_parse_failure`` / ``_normalize_screenshot_path`` / ``_NA_VALUES`` ——
  server_reject 语义，**含有意保留的 bug**，必须逐字共享。
* ``_fail_cap`` / ``NO_AUTO_RETRY_ERROR_TYPES`` / ``LIMITED_RETRY_ERROR_TYPES`` ——
  重试上限策略，app.py:1239 直接 import。
* ``_parse_price_float`` 等四个比较器 —— 变动检测；app.py:1807 直接 import 第一个。

导入 common.database 的代价：它 import aiosqlite（venv 里已装），无副作用、不建连接。

**约束：本文件下方不得出现任何 ``def`` / 赋值新对象。只有 import。**
"""
# flake8: noqa: F401  —— 全部是有意的再导出
from common.database import (
    # ---- 重试策略 ----
    LIMITED_RETRY_ERROR_TYPES,
    NO_AUTO_RETRY_ERROR_TYPES,
    NO_RETRY_ERROR_TYPES,
    _fail_cap,
    # ---- 锁仪表（必须与 SQLite 实现共用同一个全局容器）----
    LOCK_STATS,
    TimedLock,
    _NamedLockCtx,
    _record_wait,
    _record_hold,
    record_stage,
    # ---- 解析失败 / 截图路径归一 ----
    _NA_VALUES,
    _normalize_screenshot_path,
    _is_parse_failure,
    # ---- 变动比较器 ----
    _parse_price_float,
    _compare_price,
    _compare_stock_qty,
    _compare_stock_status,
    # ---- hash ----
    _HASH_FIELDS,
    _TITLE_BULLETS_FIELDS,
    _compute_content_hash,
    _compute_title_bullets_hash,
    # ---- asin_data 列清单 ----
    ASIN_DATA_FIELDS,
    _ASIN_DATA_COLUMN_SET,
)

__all__ = [
    "LIMITED_RETRY_ERROR_TYPES",
    "NO_AUTO_RETRY_ERROR_TYPES",
    "NO_RETRY_ERROR_TYPES",
    "_fail_cap",
    "LOCK_STATS",
    "TimedLock",
    "_NamedLockCtx",
    "_record_wait",
    "_record_hold",
    "record_stage",
    "_NA_VALUES",
    "_normalize_screenshot_path",
    "_is_parse_failure",
    "_parse_price_float",
    "_compare_price",
    "_compare_stock_qty",
    "_compare_stock_status",
    "_HASH_FIELDS",
    "_TITLE_BULLETS_FIELDS",
    "_compute_content_hash",
    "_compute_title_bullets_hash",
    "ASIN_DATA_FIELDS",
    "_ASIN_DATA_COLUMN_SET",
]
