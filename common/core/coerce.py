"""common/core/coerce.py —— HTTP 边界上的类型强转（唯一真源）。

``as_int`` 原先在 ``common/pgdb/pool.py``，但那个模块是模块级 ``import asyncpg``：
一个纯函数被锁在只有 PG 后端才能 import 的模块里，谁都不能安全地复用它。
搬到这里之后 ``pool.py`` 改为再导出，导入名与调用点一字不动。
"""
from typing import Any, Optional


def as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    """HTTP 边界上的整数强转（task_id / lease_epoch / batch_id）。

    SQLite 会把 ``id IN ('1')`` 里的 '1' 按列 affinity 转成 1 并命中；
    asyncpg 直接 raise。这类值全部来自未经校验的 ``await request.json()``。
    """
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return default


__all__ = ["as_int"]
