"""tests/pgdb/helpers.py —— 给六个实现 agent 共用的"临时 PG 库"夹具。

用法 A（pytest，推荐）：

    # tests/pgdb/test_batches.py
    import pytest

    pytestmark = pytest.mark.asyncio

    async def test_create_batch_duplicate_is_noop(pgdb):
        first = await pgdb.create_batch("b1")
        again = await pgdb.create_batch("b1")
        assert again == first          # 同名重传返回已有 id，不是 0、不是新 id

用法 B（脚本 / 手工探针）：

    import asyncio
    from tests.pgdb.helpers import scratch_database

    async def main():
        async with scratch_database("myprobe") as db:
            ...
    asyncio.run(main())

每次进入都会 DROP + CREATE 一个全新的库，所以 identity 序列从 1 开始 ——
自增 id 的**烧号**是被黄金基线钉死的行为，复用脏库测不出来。
库名形如 ``scraper_try_<label>_<pid>``，用完即删。

建库时强制 ``LC_COLLATE='C'``：SQLite 的 TEXT 比较是字节序，PG 在 locale
collation 下会重排（影响 iter_results 的 keyset 游标和导出行序）。
schema.py 里每个 text 列还额外带 COLLATE "C"，这里是第二道保险。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from common import config


def _dsn_for(dbname: str) -> str:
    base = os.environ.get("PG_DSN") or config.PG_DSN
    parts = urlsplit(base)
    return urlunsplit(parts._replace(path="/" + dbname))


def scratch_name(label: str = "t") -> str:
    safe = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in label).lower()
    return f"scraper_try_{safe}_{os.getpid()}"


async def create_scratch(name: str) -> str:
    """建库（先 DROP 再 CREATE），返回它的 DSN。"""
    admin = await asyncpg.connect(_dsn_for("postgres"))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(
            f'CREATE DATABASE "{name}" TEMPLATE template0 '
            f"LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8'")
    finally:
        await admin.close()
    return _dsn_for(name)


async def drop_scratch(name: str) -> None:
    admin = await asyncpg.connect(_dsn_for("postgres"))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await admin.close()


@asynccontextmanager
async def scratch_database(label: str = "t", *, verify_schema: bool = True):
    """产出一个**已连接、已建表**的 common.pgdb.Database，退出时删库。"""
    from common.pgdb import Database

    name = scratch_name(label)
    dsn = await create_scratch(name)
    prev_env = os.environ.get("PG_DSN")
    os.environ["PG_DSN"] = dsn
    db = Database()
    try:
        await db.connect()
        if verify_schema:
            # 列集/列序与 SQLite 新库不一致就直接炸 —— SELECT d.* 没有
            # response_model，多一列少一列都会改 erpAPI 看到的响应
            await db.verify_schema(strict=True)
        yield db
    finally:
        try:
            await db.close()
        finally:
            if prev_env is None:
                os.environ.pop("PG_DSN", None)
            else:
                os.environ["PG_DSN"] = prev_env
            await drop_scratch(name)


@asynccontextmanager
async def scratch_conn(label: str = "raw"):
    """只要一条裸 asyncpg 连接（不建表）时用，比如验证 DDL 本身。"""
    name = scratch_name(label)
    dsn = await create_scratch(name)
    conn = await asyncpg.connect(dsn)
    try:
        yield conn
    finally:
        await conn.close()
        await drop_scratch(name)
