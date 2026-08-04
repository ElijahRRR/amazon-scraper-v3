"""common/pgdb/pool.py —— asyncpg 连接层 + aiosqlite 形状兼容垫片。

OWNS（Database 公开面）:
    __init__(db_path=None)
    connect()
    close()
    read()                 @asynccontextmanager
    _open_read_pool()      —— PG 下为兼容保留的 no-op
    _db                    @property -> 写连接代理
    _write_lock            —— TimedLock（从 common.database 共享）

OWNS（非公开基础设施，其余 mixin 只准通过这些入口碰数据库）:
    _tx()                  @asynccontextmanager -> 写连接代理（已在事务里）
    _pool                  asyncpg.Pool（读侧）
    translate_sql()        ? -> $n（+ LIKE 的 ASCII 折叠改写）
    rowcount_from_tag()    'UPDATE 3' -> 3
    text_affinity()        复刻 SQLite TEXT affinity 的绑定强转
    as_int()               HTTP 边界的整数强转

--------------------------------------------------------------------------
为什么要有"垫片"而不是把 SQL 搬进方法里
--------------------------------------------------------------------------
server/app.py 里有 7 个事务、11 条裸 SQL、6 处 ``db.read()``，全部直接用
aiosqlite 协议（``?`` 占位符、``async with conn.execute(...) as cur``、
``cursor.rowcount``）。把它们抽成 Database 方法要么改 common/database.py（禁止），
要么按后端分叉 app.py（破坏"SQLite 路径逐字节不变"）。所以这里提供一层
**形状兼容**的代理：app.py 一个字都不用改。

--------------------------------------------------------------------------
关键决策（详见 OWNERSHIP.md 的决策台账，实现者不要自行推翻）
--------------------------------------------------------------------------
D-2  _write_lock 保持**真锁**，并且 ``_db`` 是**一条专用写连接**。
     这样 17 个 BEGIN/COMMIT 块的串行语义与 SQLite 完全一致：不会出现
     pull_tasks 双发、mark_callback_attempt 丢更新、accept_results_batch
     与 reclaim 互相死锁这些 PG 独有的新故障模式。读侧走 asyncpg 池，
     "重读阻塞写"这个真正的痛点已经解决。真正的写并发是 Phase 1.5 的事
     （前提是先把 app.py 里的裸 SQL 抽干净）。
D-6  pgdb 内部的 SQL 方言是 ``?`` 占位符，由 translate_sql 统一改写成 $n。
     同一条语句里 **不准** 混用 ``?`` 和 ``$n``（会 raise）。
     好处：get_results 那个 "join_params + where_params 与文本顺序不一致"
     的编号陷阱直接消失——``?`` 天然按文本出现顺序绑定。
D-7  statement_cache_size=0。动态拼接的 UPDATE/INSERT 文本组合爆炸，且
     asyncpg 会把首次推断的参数类型 OID 冻结在缓存里；Phase 1 要的是确定性。
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

from common import config
from common.pgdb._shared import TimedLock

logger = logging.getLogger(__name__)


# ============================================================
# 1) 状态标签 / 类型强转 —— 纯函数，可单测
# ============================================================

def rowcount_from_tag(tag: Any) -> int:
    """把 asyncpg 的命令完成标签解析成 int。

    'UPDATE 3' -> 3 / 'DELETE 0' -> 0 / 'INSERT 0 1' -> 1 / 'SELECT 2' -> 2

    aiosqlite 的 ``cursor.rowcount`` 在 22 处被读取，其中 database.py:1673 与
    1745 是 **lease 门**（``if rowcount == 0: stale``）。若这里返回字符串，
    ``'UPDATE 0' == 0`` 为 False，lease 门会静默放行所有过期结果。
    所以：绝不把原始标签泄露到 pgdb 边界之外。
    """
    if tag is None:
        return -1
    if isinstance(tag, int):
        return tag
    try:
        return int(str(tag).rsplit(" ", 1)[-1])
    except (ValueError, IndexError):
        return -1


# SQLite TEXT affinity 的复刻。见规格里的实测表：
#   True -> '1'（不是 'True'）；0.1+0.2 -> '0.3'（不是 '0.30000000000000004'）；
#   1e21 -> '1.0e+21'（不是 '1e+21'）
# 已知不等价的边角：-0.0（SQLite '0.0'）、inf（SQLite 'Inf'）——JSON 到不了，记录备查。
_ASCII_UP = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_ASCII_LO = "abcdefghijklmnopqrstuvwxyz"
_ASCII_FOLD = str.maketrans(_ASCII_UP, _ASCII_LO)

# NUL 字节：SQLite 的 TEXT 收，PG 的 text 不收（CharacterNotInRepertoireError），
# 而且它会让**整个事务**回滚（一条脏标题毁掉一整批上传）。
# 默认 False = 严格等价（保留 NUL、让它报错），置 1 则在绑定层剔除。
# 这是一个需要人来拍板的取舍（剔除会改变落库数据，进而改变 content_hash /
# title_bullets_hash / asin_changes），所以做成开关而不是偷偷剔。
PG_STRIP_NUL = os.environ.get("PG_STRIP_NUL", "0") == "1"


def ascii_fold(s: str) -> str:
    """只折叠 ASCII 大小写——与 SQLite LIKE 的行为一致，不碰 Unicode。

    注意：**不要**用 Python 的 ``str.lower()``，它是 Unicode-aware 的
    （'É'.lower() -> 'é'），会把 SQLite 的大小写敏感行为改掉。
    """
    return s.translate(_ASCII_FOLD)


def text_affinity(v: Any) -> Optional[str]:
    """把任意 JSON 标量转成 SQLite TEXT 列会存进去的那个字符串。

    asyncpg 对参数类型是严格的：往 text 列绑 int/float/bool 会 raise DataError，
    而 SQLite 会静默按 TEXT affinity 转换。``POST /api/tasks/result`` 收的是
    任意 JSON，所以任何一个客户端发 ``"review_count": 128`` 就会把 PG 版打成 500。
    仓库自带的 worker 全部 str 化、黄金夹具也全是字符串，**夹具抓不到这一类**。

    → results_write.py 必须让每一个绑到 TEXT 列的值都过这个函数。
    """
    if v is None:
        return None
    if isinstance(v, str):
        return _maybe_strip_nul(v)
    if v is True:
        return "1"
    if v is False:
        return "0"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        s = "%.15g" % v
        if "e" in s:
            mant, _, exp = s.partition("e")
            if "." not in mant:
                mant += ".0"
            s = mant + "e" + exp
        elif "." not in s and "inf" not in s and "nan" not in s:
            s += ".0"
        return s
    return _maybe_strip_nul(str(v))


def _maybe_strip_nul(s: str) -> str:
    if PG_STRIP_NUL and "\x00" in s:
        return s.replace("\x00", "")
    return s


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


# ============================================================
# 2) SQL 方言翻译
# ============================================================

# 事务控制语句：垫片自己拦下来，转成 asyncpg 的 transaction 对象。
# 'BEGIN IMMEDIATE' 在 PG 里是语法错误，而且它的语义（提前抢 SQLite 写锁）
# 在 PG 里没有对应物，直接当普通 BEGIN。
_TX_BEGIN = frozenset({
    "begin", "begin immediate", "begin deferred", "begin exclusive",
    "begin transaction", "begin immediate transaction", "start transaction",
})
_TX_COMMIT = frozenset({"commit", "commit transaction", "end", "end transaction"})
_TX_ROLLBACK = frozenset({"rollback", "rollback transaction"})

# 返回行的语句：走 fetch()；其余走 execute() 拿命令标签。
_ROW_RETURNING_HEAD = ("select", "values", "table", "show", "explain")
_RETURNING_RE = re.compile(r"\breturning\b", re.IGNORECASE)

# `col LIKE ?` -> `ascii_lower(col) LIKE ascii_lower(?)`
#
# 为什么不用 ILIKE：实测（39 个探针 × 5 种候选写法）ILIKE 有 9 处与 SQLite 不一致，
# ascii_lower + LIKE 是 0 处。差异来自非 ASCII——SQLite 的 LIKE 只折叠 ASCII，
# 'CAFÉ CREME' 不匹配 '%café%'，而 ILIKE 会匹配。ILIKE 还依赖 collation，
# 换台机器行为就变。ascii_lower 是 IMMUTABLE 且与 collation 无关。
#
# 这条改写覆盖 server/app.py:2274（DELETE /api/results 的模糊选中）——那条 SQL
# 是 f-string 拼的、又选中行去**删除**，大小写敏感会静默少删。改写掉它就不必动 app.py。
# 已经显式写成 ``ascii_lower(x) LIKE ascii_lower(?)`` 的语句不会被二次匹配。
_LIKE_QMARK_RE = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s+LIKE\s+\?",
    re.IGNORECASE,
)

# SQLite 独有、无法机械翻译的整条语句 -> PG 等价语句序列。
# key 是"压平空白后转小写"的语句文本。
_STATEMENT_OVERRIDES: Dict[str, List[str]] = {
    # server/app.py:2654，DELETE /api/database 的收尾。
    # SQLite 里删掉 sqlite_sequence 行 = 下一个 id 回到 1；PG 要显式重启 identity。
    # 只重启 SQLite 那 5 张有 AUTOINCREMENT 的表（batch_asins / seller_discoveries
    # 是复合主键，没有序列）。响应体仍然只是 {"ok": true}。
    "delete from sqlite_sequence": [
        "ALTER TABLE batches       ALTER COLUMN id RESTART WITH 1",
        "ALTER TABLE asin_data     ALTER COLUMN id RESTART WITH 1",
        "ALTER TABLE asin_changes  ALTER COLUMN id RESTART WITH 1",
        "ALTER TABLE tasks         ALTER COLUMN id RESTART WITH 1",
        "ALTER TABLE screenshots   ALTER COLUMN id RESTART WITH 1",
    ],
}

_WS_RE = re.compile(r"\s+")


def normalize_stmt(sql: str) -> str:
    return _WS_RE.sub(" ", sql.strip().rstrip(";").strip()).lower()


def qmark_to_numeric(sql: str) -> str:
    """把 ``?`` 换成 ``$1..$n``，跳过字符串字面量、标识符引号和注释。"""
    out: List[str] = []
    i, n, idx = 0, len(sql), 0
    while i < n:
        ch = sql[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            out.append(sql[i:j])
            i = j
            continue
        if ch == '"':
            j = sql.find('"', i + 1)
            j = n if j < 0 else j + 1
            out.append(sql[i:j])
            i = j
            continue
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(sql[i:j])
            i = j
            continue
        if sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(sql[i:j])
            i = j
            continue
        if ch == "?":
            idx += 1
            out.append("$%d" % idx)
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def translate_sql(sql: str) -> str:
    """pgdb 的唯一 SQL 入口翻译：LIKE 折叠改写 + ``?`` -> ``$n``。

    不含 ``?`` 的语句原样透传，所以想直接写 ``= ANY($1::bigint[])`` 也可以——
    但**同一条语句里不准混用两种占位符**。
    """
    if "?" not in sql:
        return sql
    if "$" in sql:
        raise ValueError(
            "同一条语句里混用了 ? 和 $n 占位符，翻译结果一定是错的：\n" + sql)
    sql = _LIKE_QMARK_RE.sub(r"ascii_lower(\1) LIKE ascii_lower(?)", sql)
    return qmark_to_numeric(sql)


# ============================================================
# 3) aiosqlite 形状的游标 / 连接代理
# ============================================================

class Cursor:
    """aiosqlite ``Cursor`` 的形状子集。

    支持：``await cur.fetchone()`` / ``fetchall()`` / ``async for row in cur``
    / ``cur.rowcount``，并且自身也是 async context manager（对应
    ``async with conn.execute(...) as cur:``）。

    行对象是 ``asyncpg.Record``：``row[0]`` / ``row["col"]`` / ``dict(row)``
    三种访问方式都支持，与 ``aiosqlite.Row`` 一致。
    """

    __slots__ = ("_rows", "_pos", "rowcount", "sql")

    def __init__(self, rows: Sequence[Any], rowcount: int, sql: str = ""):
        self._rows = rows
        self._pos = 0
        self.rowcount = rowcount
        self.sql = sql

    async def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchall(self):
        rows = list(self._rows[self._pos:])
        self._pos = len(self._rows)
        return rows

    async def fetchmany(self, size: int = 1):
        rows = list(self._rows[self._pos:self._pos + size])
        self._pos += len(rows)
        return rows

    async def close(self):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._pos >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _ExecOp:
    """``conn.execute(...)`` 的返回值：既可 ``await``，也可 ``async with``。

    aiosqlite 两种用法在本仓库里都有：
        cursor = await self._db.execute(sql, params)      # 读 rowcount
        async with self._db.execute(sql, params) as cur:  # 迭代
    """

    __slots__ = ("_proxy", "_sql", "_params", "_cursor")

    def __init__(self, proxy: "ConnProxy", sql: str, params: Sequence[Any]):
        self._proxy = proxy
        self._sql = sql
        self._params = params
        self._cursor: Optional[Cursor] = None

    def __await__(self):
        return self._proxy._run(self._sql, self._params).__await__()

    async def __aenter__(self) -> Cursor:
        self._cursor = await self._proxy._run(self._sql, self._params)
        return self._cursor

    async def __aexit__(self, *exc):
        return False


class ConnProxy:
    """一条 asyncpg 连接的 aiosqlite 形状包装。

    只暴露仓库真正用到的面：execute / executemany / executescript /
    fetch 系列 + BEGIN/COMMIT/ROLLBACK 拦截。
    """

    def __init__(self, conn: asyncpg.Connection, *, allow_tx: bool = True,
                 label: str = ""):
        self._conn = conn
        self._allow_tx = allow_tx
        self._label = label
        self._tx: Optional[asyncpg.transaction.Transaction] = None
        # 单条连接上的**语句级**串行化，复刻 aiosqlite 的内部排队。
        #
        # 为什么必须有：D-2 让 ``_db`` 是**一条**专用写连接。aiosqlite 把每个
        # 操作丢到该连接自己的工作线程上排队，所以两个协程同时用同一条连接是
        # 合法的；asyncpg 不排队，直接
        #   InterfaceError: cannot perform operation: another operation is in progress
        #
        # 仓库里确实存在"不持 _write_lock 就碰 _db"的路径，它们在 SQLite 下
        # 完全合法，因此不能改（equivalence-first）：
        #   common/pgdb/batches.py  list_callback_due —— _callback_dispatcher 定时调用
        #   server/app.py:1298 / 2230 / 2281 / 2289 / 2294 / 2309 —— 裸 _db 读
        # 这些与任意持锁写路径并发就会 raise。黄金夹具看不见（它把 4 个后台
        # 协程 no-op 掉了，且 TestClient 是顺序的），但**真实服务**必然撞上。
        #
        # 语义与 aiosqlite 逐条对齐：锁只包住**一条**语句，不包住事务。
        # 于是"另一个协程的 SELECT 插进某个开着的事务中间"这件事，两个后端
        # 的行为一致（同一条连接 = 同一个事务，都能读到未提交的改动）。
        # 不会与 _write_lock 形成环：持有 _op_lock 期间绝不去拿 _write_lock。
        self._op_lock = asyncio.Lock()

    # ---- 原生连接（需要 asyncpg 专有能力时用，例如 copy / listen）----
    @property
    def raw(self) -> asyncpg.Connection:
        return self._conn

    @property
    def in_transaction(self) -> bool:
        return self._tx is not None

    # ---- aiosqlite 面 ----
    def execute(self, sql: str, parameters: Sequence[Any] = ()) -> _ExecOp:
        return _ExecOp(self, sql, parameters or ())

    async def executemany(self, sql: str, seq_of_parameters) -> Cursor:
        stmt = translate_sql(sql)
        async with self._op_lock:
            await self._conn.executemany(stmt, [tuple(p) for p in seq_of_parameters])
        # asyncpg 的 executemany 不返回任何计数。谁要"实际插入了几行"，
        # 必须改用单条 set-based INSERT ... ON CONFLICT DO NOTHING 读命令标签
        # （见 OWNERSHIP.md 的 total_changes 条目），不要指望这里的 rowcount。
        return Cursor((), -1, stmt)

    async def executescript(self, script: str) -> Cursor:
        """多语句 DDL。asyncpg 的简单查询协议支持，但**不能带参数**。"""
        if "?" in script:
            raise ValueError("executescript 不接受占位符参数")
        async with self._op_lock:
            await self._conn.execute(script)
        return Cursor((), -1, script)

    async def begin(self):
        async with self._op_lock:
            await self._exec_tx_control("begin")

    async def commit(self):
        async with self._op_lock:
            await self._exec_tx_control("commit")

    async def rollback(self):
        async with self._op_lock:
            await self._exec_tx_control("rollback")

    async def close(self):
        return None

    # ---- 内部 ----
    async def _run(self, sql: str, params: Sequence[Any]) -> Cursor:
        # 语句级串行化（见 __init__ 里 _op_lock 的说明）。锁只包一条语句，
        # 行是即时取完的，所以 ``async with conn.execute(...) as cur`` 迭代
        # 期间并不持锁。
        async with self._op_lock:
            return await self._run_unlocked(sql, params)

    async def _run_unlocked(self, sql: str, params: Sequence[Any]) -> Cursor:
        norm = normalize_stmt(sql)

        # (a) 事务控制
        if norm in _TX_BEGIN or norm in _TX_COMMIT or norm in _TX_ROLLBACK:
            await self._exec_tx_control(norm)
            return Cursor((), -1, sql)

        # (b) SQLite 专有语句的整句替换
        override = _STATEMENT_OVERRIDES.get(norm)
        if override is not None:
            total = 0
            for stmt in override:
                total += max(rowcount_from_tag(await self._conn.execute(stmt)), 0)
            return Cursor((), total, sql)

        stmt = translate_sql(sql)
        args = tuple(params or ())

        head = norm.split(" ", 1)[0] if norm else ""
        returns_rows = (
            head in _ROW_RETURNING_HEAD
            or (head == "with" and bool(_RETURNING_RE.search(norm)))
            or (head in ("insert", "update", "delete")
                and bool(_RETURNING_RE.search(norm)))
        )
        if returns_rows:
            rows = await self._conn.fetch(stmt, *args)
            # aiosqlite 对 SELECT 也给 -1，保持一致，避免有人误把它当影响行数
            return Cursor(rows, -1, stmt)

        tag = await self._conn.execute(stmt, *args)
        return Cursor((), rowcount_from_tag(tag), stmt)

    async def _exec_tx_control(self, norm: str):
        if not self._allow_tx:
            raise RuntimeError(
                "只读连接上不允许事务控制语句（SQLite 侧读池同样不接受写事务）")
        if norm in _TX_BEGIN:
            if self._tx is not None:
                # SQLite 会报 "cannot start a transaction within a transaction"，
                # 但本仓库不存在嵌套 BEGIN。这里显式炸掉，防止悄悄吞掉。
                raise RuntimeError("嵌套 BEGIN：上一个事务还没结束")
            self._tx = self._conn.transaction()
            await self._tx.start()
        elif norm in _TX_COMMIT:
            if self._tx is None:
                return
            tx, self._tx = self._tx, None
            await tx.commit()
        else:  # rollback
            if self._tx is None:
                return
            tx, self._tx = self._tx, None
            await tx.rollback()

    async def _abort_dangling(self):
        """连接归还前的兜底：还挂着事务就回滚掉。"""
        if self._tx is not None:
            tx, self._tx = self._tx, None
            try:
                await tx.rollback()
            except Exception:  # noqa: BLE001
                pass

    # ---- 明确不支持的东西：早失败，别静默给错数 ----
    @property
    def total_changes(self) -> int:
        raise NotImplementedError(
            "asyncpg 没有连接级的 total_changes 计数器。"
            "要'实际插入了几行'，改用单条 "
            "INSERT ... SELECT unnest(...) ON CONFLICT DO NOTHING 并读命令标签"
            "（rowcount_from_tag）。见 OWNERSHIP.md。")


# ============================================================
# 4) PoolMixin
# ============================================================

class PoolMixin:
    """Database 的连接层。MRO 里必须排第一，它的 __init__/_db/read 要赢。

    其余 mixin **不得**定义 __init__，也不得自己建连接；一律通过
    ``self._db`` / ``self.read()`` / ``self._tx()`` / ``self._write_lock``。
    """

    # 子类/实例属性声明（便于阅读，不是运行时约束）
    _pool: Optional[asyncpg.Pool]
    _write_conn: Optional[asyncpg.Connection]
    _write_proxy: Optional[ConnProxy]

    def __init__(self, db_path: str = None):
        # db_path 保留纯粹是为了签名兼容：server/app.py 与测试都可能传。
        # PG 后端忽略它，DSN 来自 config.PG_DSN / 环境变量 PG_DSN。
        self.db_path = db_path or config.DB_PATH
        self.dsn = os.environ.get("PG_DSN") or config.PG_DSN
        self._pool = None
        self._write_conn = None
        self._write_proxy = None
        # 与 SQLite 版共用同一个 TimedLock 类和同一个 LOCK_STATS 全局容器，
        # /api/_debug/lock-stats 的 JSON 形状才不会变（黄金基线 step 56 钉死了
        # waits/holds 的三个 caller key 与 stage_timings 的四个 stage key）。
        self._write_lock = TimedLock()
        # 兼容 SQLite 版的属性名（server 端有代码读过这些；保留但无意义）
        self._read_pool = None
        self._read_conns: List[Any] = []
        self._read_pool_size = config.PG_POOL_MAX
        self._maintenance_task: Optional[asyncio.Task] = None

    # ---------------- 生命周期 ----------------
    async def connect(self):
        """建池 + 建写连接 + 建表。对应 SQLite 版的 connect()。"""
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=config.PG_POOL_MIN,
            max_size=config.PG_POOL_MAX,
            # D-7：Phase 1 关预备语句缓存。动态拼接的 UPDATE/INSERT 文本组合爆炸，
            # 且 asyncpg 会冻结首次推断的参数类型 OID。
            statement_cache_size=0,
            command_timeout=config.PG_COMMAND_TIMEOUT,
            server_settings={"search_path": "public"},
        )
        self._write_conn = await asyncpg.connect(
            dsn=self.dsn,
            statement_cache_size=0,
            command_timeout=config.PG_COMMAND_TIMEOUT,
            server_settings={"search_path": "public"},
        )
        self._write_proxy = ConnProxy(self._write_conn, allow_tx=True, label="write")

        await self.init_tables()          # SchemaMixin
        await self._warm_pool()
        logger.info("PostgreSQL 连接就绪：pool=%d~%d + 1 条写连接",
                    config.PG_POOL_MIN, config.PG_POOL_MAX)

    async def _warm_pool(self):
        """预热每条池连接。

        冷连接的第一次往返有握手/规划开销；黄金基线里 ``slow_holds_recent`` 是
        **空列表**，而 _record_hold 在持锁 >200ms 时会往里塞元素。第一笔
        accept_results_batch 撞上冷池就会让那个列表非空 → 列表长度 diff。
        """
        if not self._pool:
            return
        conns = []
        try:
            for _ in range(config.PG_POOL_MIN):
                c = await self._pool.acquire()
                await c.execute("SELECT 1")
                conns.append(c)
        finally:
            for c in conns:
                await self._pool.release(c)
        if self._write_conn:
            await self._write_conn.execute("SELECT 1")

    async def _open_read_pool(self):
        """SQLite 专有的只读连接池。PG 下由 asyncpg.Pool 承担，这里是 no-op。

        公开面保留是因为它在 API 清单里（#3），而且 connect() 之外可能有人调。
        """
        return None

    async def close(self):
        if self._maintenance_task and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except (asyncio.CancelledError, Exception):  # noqa: B014
                pass
        self._maintenance_task = None
        if self._write_proxy is not None:
            await self._write_proxy._abort_dangling()
        if self._write_conn is not None:
            try:
                await self._write_conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._write_conn = None
            self._write_proxy = None
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception:  # noqa: BLE001
                pass
            self._pool = None

    # ---------------- 连接入口 ----------------
    @property
    def _db(self) -> Optional[ConnProxy]:
        """写连接代理。

        SQLite 版的 ``self._db`` 是**一条**连接，17 个 BEGIN/COMMIT 块靠
        ``_write_lock`` 串行化。这里刻意维持同一形状：一条专用写连接 + 真锁。
        server/app.py 的 ``async with db._write_lock: ... db._db.execute('BEGIN')
        ... db._db.execute('COMMIT')`` 因此逐字可用，并且 app.py:1510 那种
        "退出锁块之后才读 cursor.rowcount" 也仍然正确。

        ⚠ 任何写路径都必须在 ``self._write_lock`` 里使用 ``_db``。
        """
        return self._write_proxy

    @asynccontextmanager
    async def read(self):
        """从 asyncpg 池借一条连接（池满则排队，形成读侧背压）。

        与 SQLite 版形状一致：``async with db.read() as rc, rc.execute(sql, p) as c:``
        回退分支也保留（池未就绪时退化到写连接），与 database.py:363-364 一致。
        """
        if self._pool is None:
            yield self._write_proxy
            return
        conn = await self._pool.acquire()
        proxy = ConnProxy(conn, allow_tx=False, label="read")
        try:
            yield proxy
        finally:
            await self._pool.release(conn)

    @asynccontextmanager
    async def _tx(self):
        """写事务：``async with self._tx() as conn:`` —— 取代裸 BEGIN/COMMIT。

        调用方**必须**已经持有 ``self._write_lock``（与 SQLite 版一致）。
        asyncpg 的 transaction 上下文对 CancelledError 是安全的，顺带修掉了
        catalog_sync_audit.md:130 记的"取消让共享连接永久卡在事务里"。
        """
        proxy = self._write_proxy
        if proxy is None:
            raise RuntimeError("数据库未连接")
        # 走代理自己的事务控制（而不是 proxy.raw.transaction()），有两个原因：
        # 1) BEGIN/COMMIT 也必须过 _op_lock。raw.transaction() 直接在裸连接上
        #    发语句，会绕开语句级串行化，于是"另一个协程正在 fetch"时开事务
        #    就抛 InterfaceError —— 这正是 _callback_dispatcher 撞上的那条路径。
        # 2) 与 execute("BEGIN") 共用同一个 proxy._tx 状态位，两套事务写法
        #    因此可以互相看见（嵌套 BEGIN 仍会被显式挡下）。
        await proxy.begin()
        try:
            yield proxy
        except BaseException:
            # BaseException 而非 Exception：CancelledError 也必须回滚，
            # 否则那条唯一的写连接会永远卡在事务里
            # （catalog_sync_audit.md:130 记的就是这个故障）。
            await proxy.rollback()
            raise
        else:
            await proxy.commit()

    # ---------------- 供 mixin 使用的小工具 ----------------
    @staticmethod
    def rowcount_from_tag(tag: Any) -> int:
        return rowcount_from_tag(tag)

    @staticmethod
    def text_affinity(v: Any) -> Optional[str]:
        return text_affinity(v)

    @staticmethod
    def as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
        return as_int(v, default)
