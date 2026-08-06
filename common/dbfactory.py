"""common/dbfactory.py —— 存储后端开关。

    DB_BACKEND=sqlite  （默认，未设置时也是它）-> common.database.Database
    DB_BACKEND=postgres                        -> common.pgdb.Database

用法：

    from common.dbfactory import create_database
    db = create_database()
    await db.connect()

设计约束：
* **未设置或设为 sqlite 时，行为与移植前逐字节相同**：走的是同一个
  ``common.database.Database`` 类、同一个构造调用，连 import 都不会碰 asyncpg。
* pgdb 是**惰性** import 的。只有真的选了 postgres 才会加载 asyncpg，
  所以 SQLite 部署不需要装 asyncpg。
* 反过来，pgdb 内部会 import common.database（为了共享 LOCK_STATS /
  ASIN_DATA_FIELDS / 比较器等纯 Python 符号），这是有意的：分叉一份副本
  就等于制造两个真源。见 common/pgdb/_shared.py 的说明。
"""
from __future__ import annotations

import os
from typing import Any, Optional

SQLITE = "sqlite"
POSTGRES = "postgres"
_ALIASES = {
    "sqlite": SQLITE, "sqlite3": SQLITE, "": SQLITE,
    "postgres": POSTGRES, "postgresql": POSTGRES, "pg": POSTGRES,
}


def get_backend() -> str:
    """当前后端名（每次读环境变量，测试可以随时改）。"""
    raw = (os.environ.get("DB_BACKEND") or SQLITE).strip().lower()
    try:
        return _ALIASES[raw]
    except KeyError:
        raise ValueError(
            f"DB_BACKEND={raw!r} 不认识，只支持 {sorted(set(_ALIASES.values()))}")


def is_postgres() -> bool:
    return get_backend() == POSTGRES


def get_database_class() -> type:
    """返回当前后端的 Database **类**。

    tests/golden/harness.py 需要它：那里是按**类属性**给 start_maintenance /
    run_startup_optimize 打 no-op 补丁的，打错类等于没打。
    """
    if get_backend() == POSTGRES:
        from common.pgdb import Database as PgDatabase
        return PgDatabase
    from common.database import Database as SqliteDatabase
    return SqliteDatabase


def create_database(db_path: Optional[str] = None) -> Any:
    """按 DB_BACKEND 构造 Database（未连接）。

    db_path 只对 SQLite 有意义；PG 后端接受它只是为了签名兼容，DSN 来自
    ``PG_DSN`` 环境变量 / config.PG_DSN。
    """
    cls = get_database_class()
    return cls(db_path) if db_path is not None else cls()
