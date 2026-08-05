"""common/pgdb/outbox.py —— 事件流**写钩子侧**的胶水（Phase 2）。

发射器本身不在这里：``EventStreamMixin._emit_outbox``（`common/pgdb/relay.py`）
是唯一的 body 构造与 INSERT 入口，gen / instance_id / 归一化 / NUL 剔除 / 计数
全归它。本文件只装写钩子**自己**要解决的四件事，每一件都是 relay 那边够不着的：

1. **payload 定形**（`safe_payload`）。relay 的 `dumps_body` 只做 `default=str`，
   挡不住 `NaN` / `±Infinity` / 超长整数——而这三种值**都到得了**，见下面 §一。
   它们会把一次今天成功的提交变成 500 + 整批回滚，这是本阶段绝不能引入的回归。
2. **调用上下文**（`EventContext` / `scoped_context`）。§1.1 的钩子在
   `_save_result_inner_unlocked` 里，而那个方法的签名被
   `tests/pgdb/test_skeleton.py::test_signatures_match_sqlite` 逐字钉死
   （形参名 + 种类 + 默认值全比），加不了参数。只能走实例属性。
3. **服务端事实**（`task_facts`）。`attempt` / `zip_requested` / `asin` 必须来自
   `tasks` 行，不能来自 worker 交上来的 dict，也不能靠给租约 UPDATE 加 RETURNING
   （那会毁掉租约门，见 `task_facts` 的注释）。
4. **stale 的独立事务**（`emit_stale_event_own_tx`）。S1/F1 判定 stale 之后立刻
   `ROLLBACK`，插在它前面的 outbox 行会跟着没。

为什么是模块级函数、一个 mixin 方法都不定义：`_assert_single_owner` 只遍历
`PUBLIC_API` 里的名字，**私有方法重名不会报错，只会被 MRO 静默遮蔽**。写钩子与
relay 由两个人分别实现，本文件不占任何方法名，结构上不可能和 `EventStreamMixin`
撞车。

--------------------------------------------------------------------------
一、payload 定形：三种会杀事务的值（实测，scratchpad/bind_probe.py）
--------------------------------------------------------------------------
    json.dumps({"x": float('nan')})  -> '{"x":NaN}'
        -> InvalidTextRepresentationError: invalid input syntax for type json
    json.dumps({"x": float('inf')})  -> '{"x":Infinity}'          -> 同上
    json.dumps({"x": 10**5000})
        -> ValueError: Exceeds the limit (4300 digits) for integer string
           conversion                     ← Python 3.11 自己先炸，根本到不了 PG

三种都**到得了**（决策 D-20 逐条实测过）：`request.json()` 用的是 `json.loads`，
它接受 `NaN` / `Infinity` / `-Infinity` 字面量，`1e400` 解析出来就是 `inf`，
超长整数就是普通 JSON 数字。而 D-20 之后 `text_affinity` 对这些值的处理是
**落库**（`nan -> NULL`、`inf -> 'Inf'`）或**抛 OverflowError**，也就是说：
今天有一类载荷是能成功写进 `asin_data` 的，如果写钩子在它们上面炸，就等于
"事件流的存在让一次本来成功的采集失败了"。

对策：`json_safe` 按 `pool.text_affinity` 的口径把它们定形（`nan -> null`、
`inf -> "Inf"`、`-inf -> "-Inf"`），超 int64 的整数换成 `"<int:Nbit>"` 标记
（`str()` 它自己就会抛，连"存成字符串"都做不到）。**本函数不抛异常**，并且
在无需改动时**返回原对象**（身份不变），常见路径上零拷贝。

NUL 不在这里管：relay 的 `scrub_nul` 已经无条件剔除（与 `PG_STRIP_NUL` 无关）。

--------------------------------------------------------------------------
二、SQLite 后端：零字节
--------------------------------------------------------------------------
本文件只被 `common/pgdb/*` 导入。`common/dbfactory.py` 在 `DB_BACKEND != postgres`
时返回未经改动的 `common.database.Database`，那条路径上没有本文件的任何代码——
比运行期 `if is_postgres(): ...` 守卫严格更强。`common/database.py` 没被打开过。
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Optional

# 只依赖 pool 的一个纯函数：文本字段的强转口径必须与 tasks / asin_data 的写入
# 完全一致，否则同一条记录在事件流与 HTTP API 里的键会长得不一样。
from common.pgdb.pool import text_affinity

logger = logging.getLogger(__name__)

__all__ = [
    "PAYLOAD_STATS", "json_safe", "safe_payload", "text_or_none", "stream_ready",
    "EventContext", "scoped_context", "task_facts", "result_context",
    "emit", "emit_result_event", "emit_stale_event", "emit_stale_event_own_tx",
]

#: `_save_result_inner_unlocked` 的调用上下文挂在实例的这个属性上。
#: 全程在 `_write_lock` 内设置/清除，中间的 await 只可能被同样要拿写锁的协程
#: 打断（而它拿不到锁），所以不存在交错。
_CTX_ATTR = "_outbox_ctx"

_SQLITE_INT_MIN = -(2 ** 63)
_SQLITE_INT_MAX = 2 ** 63 - 1
_INF = float("inf")

#: 递归深度上限。payload 实际是一层扁平 dict；这里只挡"深到能把 Python 栈打穿"
#: 的输入——超过就换成标记字符串，绝不抛。
_MAX_DEPTH = 100

#: 定形动作的计数器。**刻意不接进任何 HTTP 端点**：黄金基线钉死了
#: `/api/_debug/lock-stats` 与 `/api/diagnostic` 的键集，多一个键就是 64/64 失败。
#: relay 侧的 `event_relay_metrics()` 是给运维看的那一份，这里只给测试和排查用。
PAYLOAD_STATS: Dict[str, int] = {
    "nonfinite": 0,        # NaN / ±Inf 被定形的次数
    "int_overflow": 0,     # 超出 int64 的整数被换成标记的次数
    "coerced": 0,          # 非 JSON 原生对象被 str() 的次数
    "truncated_depth": 0,  # 触到 _MAX_DEPTH 的次数
    "stale_emit_failed": 0,  # 独立事务里的 stale 事件写失败（已吞，见 §2.3）
}


# ============================================================
# 1) payload 定形（纯函数，可单测）
# ============================================================

def json_safe(obj: Any, _depth: int = 0) -> Any:
    """把任意对象变成 **json.dumps + jsonb 一定收得下**的等价物。绝不抛异常。

    无需改动时返回**原对象**（`x is json_safe(x)`），所以正常 payload 上不产生
    任何拷贝。见模块头注释 §一。
    """
    if obj is None or isinstance(obj, bool) or isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        if _SQLITE_INT_MIN <= obj <= _SQLITE_INT_MAX:
            return obj
        PAYLOAD_STATS["int_overflow"] += 1
        return f"<int:{obj.bit_length()}bit>"
    if isinstance(obj, float):
        # pool.text_affinity 的口径：nan -> NULL、inf -> 'Inf'、-inf -> '-Inf'。
        # 让 payload 与同一次提交落进 asin_data 的字符串保持一致。
        if obj != obj:
            PAYLOAD_STATS["nonfinite"] += 1
            return None
        if obj == _INF:
            PAYLOAD_STATS["nonfinite"] += 1
            return "Inf"
        if obj == -_INF:
            PAYLOAD_STATS["nonfinite"] += 1
            return "-Inf"
        return obj
    if _depth >= _MAX_DEPTH:
        PAYLOAD_STATS["truncated_depth"] += 1
        return "<depth-limit>"
    if isinstance(obj, dict):
        out: Dict[Any, Any] = {}
        changed = False
        for k, v in obj.items():
            k2 = k if isinstance(k, str) else str(k)
            v2 = json_safe(v, _depth + 1)
            if k2 is not k or v2 is not v:
                changed = True
            out[k2] = v2
        return out if changed else obj
    if isinstance(obj, (list, tuple, set, frozenset)):
        vals = [json_safe(v, _depth + 1) for v in obj]
        if isinstance(obj, list) and all(a is b for a, b in zip(vals, obj)):
            return obj
        return vals
    # bytes / datetime / Decimal / 自定义对象……`request.json()` 到不了，
    # 但 `save_result()` 是公开方法，谁都能直接喂。relay 的 dumps_body 有
    # `default=str` 兜底，这里提前做掉并计数，免得它无声无息。
    PAYLOAD_STATS["coerced"] += 1
    try:
        return str(obj)
    except Exception:  # noqa: BLE001 —— 全函数承诺：__str__ 炸了也得有输出
        return "<unserializable>"


def safe_payload(data: Optional[dict]) -> dict:
    """交给 `_emit_outbox` 的 `data=`。**不修改入参**（返回的可能就是入参本身）。"""
    if not isinstance(data, dict):
        return {}
    safe = json_safe(data)
    return safe if isinstance(safe, dict) else {}


def text_or_none(v: Any) -> Optional[str]:
    """body 里那些**将来要落 text 列**的字段（asin / worker_id / zip_requested /
    error_type / error_detail / batch_name）的定形。

    这些值同样直接来自 `request.json()`。三条理由让它必须在这里做：

    * relay 把 body 的这几个键**直接绑到 text 列**。一个 jsonb 布尔/数组绑过去
      就是 `DataError`，而 relay 对"是行的错"的处置是隔离进死信表——一条本可以
      正常入流的记录被降级成人工捞。
    * `outcome_for_error_type(True)` 会 `AttributeError`
      （`(True or "").strip()`），那是一次**新的 500**。
    * 口径必须与写进 `tasks` / `asin_data` 的字符串**逐字相同**（`True -> '1'`），
      否则同一条记录在事件流与 HTTP API 里长得不一样。

    所以走 `pool.text_affinity`，但**绝不抛**：它对 list/dict 抛
    `ProgrammingError`（D-20），而调用点里有一条（B7 的 stale 失败项）今天根本
    不碰 text_affinity，在那里抛异常会把一次成功的批量提交变成 500。
    """
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        return text_affinity(v)
    except Exception:  # noqa: BLE001
        PAYLOAD_STATS["coerced"] += 1
        safe = json_safe(v)
        if isinstance(safe, str):
            return safe
        try:
            return str(safe)
        except Exception:  # noqa: BLE001
            return "<unserializable>"


# ============================================================
# 2) 事件流就绪与否（同步，不碰库）
# ============================================================

def stream_ready(db) -> bool:
    """`init_event_stream()` 跑过没有。

    用途只有一个：**没就绪时连那条 `task_facts` 的 SELECT 都别发**。
    真正的"发不发事件"由 `_emit_outbox` 自己判定（它有计数器和 ERROR 日志，
    还有 `PG_EVENTSTREAM_STRICT`），这里不重复那套策略。
    """
    ev = getattr(db, "_ev", None)
    if ev is None:
        # EventStreamMixin 没被拼进来（只装了部分 mixin 的测试壳）。
        return False
    try:
        return bool(ev().get("ready"))
    except Exception:  # noqa: BLE001
        return False


# ============================================================
# 3) 调用上下文 + 服务端事实
# ============================================================

class EventContext:
    """一次终态尝试的服务端事实。

    `_save_result_inner_unlocked` 只收 `(data, batch_id)`，而它的签名被
    `test_signatures_match_sqlite` 逐字钉死（形参名/种类/默认值全比），
    加不了参数——所以由调用方放进 `db._outbox_ctx`，钩子从那里取。
    """

    __slots__ = ("task_id", "worker_id", "lease_epoch", "attempt",
                 "auto_retry_count", "zip_requested", "zip_source", "asin",
                 "batch_id")

    def __init__(self, *, task_id=None, worker_id=None, lease_epoch=None,
                 attempt=0, auto_retry_count=0, zip_requested=None,
                 zip_source="task", asin=None, batch_id=None):
        self.task_id = task_id
        self.worker_id = worker_id
        self.lease_epoch = lease_epoch
        self.attempt = attempt
        self.auto_retry_count = auto_retry_count
        self.zip_requested = zip_requested
        self.zip_source = zip_source
        self.asin = asin
        self.batch_id = batch_id

    def as_kwargs(self) -> dict:
        return {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "lease_epoch": self.lease_epoch,
            "attempt": self.attempt,
            "auto_retry_count": self.auto_retry_count,
            "zip_requested": self.zip_requested,
            "zip_requested_source": self.zip_source,
        }


_TASK_FACTS_SQL = (
    "SELECT asin, zip_code, batch_id, retry_count, "
    "COALESCE(auto_retry_count, 0) AS auto_retry_count "
    "FROM tasks WHERE id = ?")


async def task_facts(db, task_id) -> Optional[dict]:
    """读 tasks 行上的服务端事实（asin / zip_code / batch_id / retry_count /
    auto_retry_count）。调用方必须已经在事务里。

    ⚠ 为什么是**单独一条 SELECT**，而不是给租约 UPDATE 加 `RETURNING`
    （设计规格 §1.3 TRAP 1，实测）：

        普通 UPDATE，租约不匹配 : rowcount=0   -> 门 `rowcount == 0` 触发？ True
        同一条 + RETURNING      : rowcount=-1  -> 门 `rowcount == 0` 触发？ False

    `ConnProxy._run_unlocked` 见到 RETURNING 就走 `returns_rows` 分支、返回
    `Cursor(rows, -1)`，而 `-1 == 0` 是 False —— **租约门会放行每一条过期结果**。
    那是全系统安全性最高的一条谓词，一个字都不许动。

    本语句是事务持有者自己发的普通读：D-15 的"改道读池"只对**非持有者**的
    普通读生效，所以它留在同一个事务里（走 PK 索引）。实测
    `_is_plain_read(...) = True`，也就是说它**具备**被改道的资格——但调用点全部
    是持有者，所以永远不会。退一步说即使被改道也无害：它读的四列没有一列会被
    在途事务改动（租约 UPDATE 动的是 status/updated_at，失败分支动的是
    retry_count 但那条路径自己带 `FOR UPDATE` 不改道）。
    """
    if task_id is None:
        return None
    async with db._db.execute(_TASK_FACTS_SQL, (task_id,)) as c:
        row = await c.fetchone()
    return dict(row) if row is not None else None


async def result_context(db, *, task_id=None, worker_id=None, lease_epoch=None,
                         data: Optional[dict] = None,
                         batch_id=None) -> Optional[EventContext]:
    """构造写钩子的上下文。事件流没就绪时返回 None 且**一条语句都不发**。"""
    if not stream_ready(db):
        return None
    facts = await task_facts(db, task_id) if task_id else None
    if facts is not None and facts.get("zip_code") is not None:
        zip_requested, zip_source = facts["zip_code"], "task"
    else:
        # 无 task_id 的直写路径（B1 / save_result），或 tasks.zip_code 为 NULL。
        # payload 里的 zip_code 是**观测值**不是请求值（worker/parser.py:111/:847
        # 会用页面 glow-ingress-line1 抽到的邮编覆盖它），所以必须标出来源，
        # 否则消费侧的分组键会把同一个商品的价格序列悄悄劈成两组。
        zip_requested = (data or {}).get("zip_code")
        zip_source = "payload"
    return EventContext(
        task_id=task_id,
        worker_id=worker_id,
        lease_epoch=lease_epoch,
        attempt=(facts or {}).get("retry_count") or 0,
        auto_retry_count=(facts or {}).get("auto_retry_count") or 0,
        zip_requested=zip_requested,
        zip_source=zip_source,
        asin=(facts or {}).get("asin"),
        batch_id=batch_id if batch_id is not None else (facts or {}).get("batch_id"),
    )


@contextmanager
def scoped_context(db, ctx: Optional[EventContext]):
    """把 ctx 挂到 db 上，退出时**一定**还原（异常路径也还原）。"""
    prev = getattr(db, _CTX_ATTR, None)
    setattr(db, _CTX_ATTR, ctx)
    try:
        yield
    finally:
        setattr(db, _CTX_ATTR, prev)


# ============================================================
# 4) 发射（全部转给 relay 的 _emit_outbox）
# ============================================================

#: body 里会被 relay 直接绑到 text 列的键（见 text_or_none）。
_TEXT_KEYS = ("asin", "worker_id", "zip_requested", "error_type",
              "error_detail", "batch_name")


async def emit(db, *, data: Optional[dict] = None, **kw):
    """`db._emit_outbox` 的唯一入口：把 payload 与文本字段定形之后转过去。

    所有写钩子都必须走这里，不要直接调 `_emit_outbox`——"交出去的东西必须是
    jsonb 收得下、且 relay 绑得进列的"这条不变式，只有收在一个地方才守得住。
    六个调用点各自记得做一遍是守不住的。
    """
    emit_fn = getattr(db, "_emit_outbox", None)
    if emit_fn is None:
        # EventStreamMixin 没拼进来。写钩子不该因此让采集失败。
        return None
    for k in _TEXT_KEYS:
        if k in kw:
            kw[k] = text_or_none(kw[k])
    return await emit_fn(data=safe_payload(data), **kw)


async def emit_result_event(db, data: dict, batch_id=None):
    """§1.1 的 H3 钩子：`_save_result_inner_unlocked` 里的那一发。

    outcome 由 payload 自己定（`ok` / `not_found` / 防御性的 `parse_failed`），
    其余事实来自 `db._outbox_ctx`。**body 用提交上来的 `data`，绝不回读
    `asin_data`**——那一行是跨时间合并出来的，事件必须是"一次完整采集"。
    """
    from common.pgdb.relay import classify_success_outcome

    ctx: Optional[EventContext] = getattr(db, _CTX_ATTR, None)
    asin = data.get("asin", "")
    if isinstance(asin, str):
        asin = asin.strip()
    kw = ctx.as_kwargs() if ctx is not None else {}
    if ctx is None or not kw.get("worker_id"):
        kw["worker_id"] = data.get("worker_id")
    return await emit(
        db,
        outcome=classify_success_outcome(data),
        # `.strip()` 之后的原值，**不做 .upper()**：create_tasks（tasks.py:137）
        # 也只 strip，大小写折叠会让事件流与 /api/results 对同一条记录的键不一致。
        asin=asin,
        data=data,
        batch_id=batch_id if batch_id is not None
        else (ctx.batch_id if ctx is not None else None),
        **kw,
    )


async def emit_stale_event(db, *, task_id=None, worker_id=None, lease_epoch=None,
                           data: Optional[dict] = None, batch_id=None,
                           error_type=None, error_detail=None):
    """stale 事件，发在**调用方的事务里**（B3 / B7 —— 它们只是 `continue`，
    批事务照常提交）。

    asin / zip_requested / attempt 按 **task_id** 去读 tasks 行：租约谓词没匹配上
    不代表那一行不存在，绝大多数情况是 `reclaim_dead_worker_tasks` 把 epoch
    bump 掉了（它对"只是慢"的 worker 一视同仁），而失败提交的 payload 里根本
    没有 asin（worker/engine.py:1647 只发 error_type / error_detail）。
    """
    if not stream_ready(db):
        return None
    facts = await task_facts(db, task_id) if task_id else None
    asin = (facts or {}).get("asin")
    if asin is None:
        asin = (data or {}).get("asin", "")
        if isinstance(asin, str):
            asin = asin.strip()
    has_task_zip = (facts or {}).get("zip_code") is not None
    return await emit(
        db,
        outcome="stale",
        asin=asin,
        data=data,
        task_id=task_id,
        # 调用方给的 batch_id 优先（这次提交声明的那个），没有就用 tasks 行上的：
        # fail_task 根本收不到 batch_id，而事件里缺它会让消费侧无从归批。
        batch_id=batch_id if batch_id is not None else (facts or {}).get("batch_id"),
        worker_id=worker_id,
        lease_epoch=lease_epoch,
        attempt=(facts or {}).get("retry_count") or 0,
        auto_retry_count=(facts or {}).get("auto_retry_count") or 0,
        zip_requested=(facts["zip_code"] if has_task_zip
                       else (data or {}).get("zip_code")),
        zip_requested_source="task" if has_task_zip else "payload",
        error_type=error_type,
        error_detail=error_detail,
    )


async def emit_stale_event_own_tx(db, *, task_id=None, worker_id=None,
                                  lease_epoch=None, data: Optional[dict] = None,
                                  batch_id=None, error_type=None,
                                  error_detail=None):
    """stale 事件，**自带事务**（设计规格 §2.3）。

    S1（accept_success_result）与 F1（fail_task）判定 stale 之后立刻 `ROLLBACK`
    ——插在它前面的 outbox 行会跟着没。所以这里在那条 `ROLLBACK` 之后、仍持有
    `_write_lock` 的情况下另开一个事务。原来那两条 `ROLLBACK` **一个字没动**：
    把它改成 `COMMIT` 今天是等价的（失败的 UPDATE 匹配了 0 行），但那是在黄金
    校验过的路径上改语句，没有收益。

    唯一代价：`ROLLBACK` 与 `COMMIT` 之间硬崩会丢掉这条 stale 事件。接受，并写进
    契约——那个窗口里租约本来就已经没了，这次采集无论如何都丢了。

    **`Exception` 一律吞掉**：主事务已经回滚，没有任何需要保持一致的东西；
    而让它冒出去会把一次本该 200 `{"ok": false, "stale": true}` 的提交变成 500,
    那是黄金基线钉死的响应（step `submit_result_stale_lease`）。
    `CancelledError` 这类 `BaseException` 照常上抛——停服必须能停下来。
    """
    if not stream_ready(db):
        return None
    await db._db.execute("BEGIN")
    try:
        rid = await emit_stale_event(
            db, task_id=task_id, worker_id=worker_id, lease_epoch=lease_epoch,
            data=data, batch_id=batch_id,
            error_type=error_type, error_detail=error_detail)
        await db._db.execute("COMMIT")
        return rid
    except BaseException as e:
        try:
            await db._db.execute("ROLLBACK")
        except BaseException:  # noqa: BLE001
            pass
        if isinstance(e, Exception):
            PAYLOAD_STATS["stale_emit_failed"] += 1
            logger.error("stale 事件写入失败（已忽略，采集路径不受影响）: %s: %s",
                         type(e).__name__, e)
            return None
        raise
