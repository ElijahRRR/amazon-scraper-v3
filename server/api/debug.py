"""诊断 / 侦查端点（5 个）—— Phase 3.2 从 `server/app.py` 拆出。

    GET    /api/diagnostic
    GET    /api/_debug/lock-stats
    GET    /api/_debug/event-stream        （include_in_schema=False）
    POST   /api/_debug/lock-stats/reset
    DELETE /api/database

------------------------------------------------------------------------
承重约束
------------------------------------------------------------------------

1. **router 光秃**：`APIRouter()`，不带 `tags=` / `prefix=`，也不在 router 上
   设 `include_in_schema`。当前 `/openapi.json` 里没有任何 `tags` 键，
   而它是黄金基线的一步、逐字节钉死。
   `api_debug_event_stream` 的 `include_in_schema=False` 是**装饰器级**参数，
   原样带过来 —— 它本来就不在 schema 里，挪到 router 上会连累另外 4 个。

2. **函数名 / docstring / 路径一个字不改**（含 `_pct` / `_summary` 这类私有助手
   的行为）：前三者被编码进 `operationId` / `summary` / `description`。

3. **`api_clear_database` 的裸事务原文照搬，一个字符都不许动。**
   其中 `DELETE FROM sqlite_sequence` 是 `common/pgdb/pool.py` 的
   `_STATEMENT_OVERRIDES` **字典键**（按"压平空白后转小写"匹配）：改一个字符
   就静默失配 —— PG 上那五张表的 identity 不再重启，而响应仍然是
   `{"ok": true}`，两侧都不报错。

4. **模块级可变全局一个都不搬**：`db` / `_worker_registry` / `_settings_version`
   留在 `server/app.py`，这里一律 `_srv().xxx`。
   `_settings_version` 是 **int**，`from server.app import _settings_version`
   会把当时的值快照下来，之后永远不变（黄金夹具还会把它重置成 0）。
   `_rollback_quietly` 同理留在 `app.py`（另外 6 个调用点在那边）。
"""

import os

from fastapi import APIRouter

from common import config


def _srv():
    from server import app as _s
    return _s


router = APIRouter()


# ==================== API: 诊断 ====================

@router.get("/api/diagnostic")
async def api_diagnostic():
    total_asins = await _srv().db.get_total_asins()
    progress = await _srv().db.get_progress()
    return {
        "total_asins": total_asins,
        "task_progress": progress,
        "active_workers": len(_srv()._worker_registry),
        "settings_version": _srv()._settings_version,
    }


# ==================== Recon 侦查端点（锁竞争 / 阶段耗时）====================
# 临时仪表，用于诊断 _write_lock 竞争和 accept_results_batch 内部瓶颈
# 完成诊断后可删除（含 common/database.py 中的 TimedLock / LOCK_STATS）

def _pct(samples: list, p: float) -> float:
    if not samples:
        return 0.0
    s = sorted(samples)
    i = max(0, min(len(s) - 1, int(len(s) * p)))
    return round(s[i], 2)


def _summary(samples: list) -> dict:
    if not samples:
        return {"count": 0}
    return {
        "count": len(samples),
        "p50": _pct(samples, 0.50),
        "p95": _pct(samples, 0.95),
        "p99": _pct(samples, 0.99),
        "max": round(max(samples), 2),
        "mean": round(sum(samples) / len(samples), 2),
    }


@router.get("/api/_debug/lock-stats")
async def api_debug_lock_stats():
    """返回锁等待 / 持锁 / 内部分阶段耗时分布。单位：毫秒。"""
    from common.database import LOCK_STATS
    return {
        "waits": {k: _summary(list(v)) for k, v in LOCK_STATS["waits"].items()},
        "holds": {k: _summary(list(v)) for k, v in LOCK_STATS["holds"].items()},
        "stage_timings": {k: _summary(list(v)) for k, v in LOCK_STATS["stage_timings"].items()},
        "slow_holds_recent": LOCK_STATS["slow_holds"][-50:],
        "slow_holds_count": len(LOCK_STATS["slow_holds"]),
    }


@router.get("/api/_debug/event-stream", include_in_schema=False)
async def api_debug_event_stream():
    """Phase 2 事件流的可观测面：outbox 深度、relay 滞后、每分钟事件数、
    seq 窗口、分区、死信、计数器。

    三条约束决定了它长这样：

    * **``include_in_schema=False``**。``/openapi.json`` 是黄金基线的第 5 步，
      逐字节钉死；新端点只要进了 schema，64 步当场挂。同一个理由写在计划
      §Phase 3 里（sync router 也是这么挂的）。
    * **不动任何既有响应**。指标本来最自然的去处是 ``/api/diagnostic``，
      但那个端点是基线 step 55，多一个 key 就是"字段出现"差异。所以另开一个
      端点，而不是往老的里塞。
    * **两个后端都必须能回**。SQLite 上事件流在结构上就不存在，这里如实回
      ``enabled: false`` 而不是 404 或 500 —— 运维拿同一个 URL 探两种部署。
    """
    from common.dbfactory import get_backend, is_postgres

    db = _srv().db
    out = {"backend": get_backend(), "enabled": False}
    if not is_postgres() or db is None or not hasattr(db, "event_stream_stats"):
        # SQLite：事件流是 PG 专属，这不是错误状态。
        out["reason"] = "event stream is postgres-only"
        return out
    out["enabled"] = True
    try:
        out.update(await db.event_stream_stats())
    except Exception as e:  # noqa: BLE001
        # 观测端点自己不许把服务搞挂：库还没引导好 / 表还没建时如实报错。
        out["error"] = f"{type(e).__name__}: {e}"
        out.update(db.event_relay_metrics())
    return out


@router.post("/api/_debug/lock-stats/reset")
async def api_debug_lock_stats_reset():
    """清空所有计时统计（开始新一轮观察前调用）。"""
    from common.database import LOCK_STATS
    LOCK_STATS["waits"].clear()
    LOCK_STATS["holds"].clear()
    LOCK_STATS["stage_timings"].clear()
    LOCK_STATS["slow_holds"].clear()
    return {"ok": True}


@router.delete("/api/database")
async def api_clear_database():
    """清空所有数据 + 截图文件"""
    import shutil
    db = _srv().db
    async with db._write_lock:
        await db._db.execute("BEGIN")
        try:
            for table in ["asin_changes", "asin_data", "batch_asins", "tasks", "screenshots", "batches"]:
                await db._db.execute(f"DELETE FROM {table}")
            await db._db.execute("DELETE FROM sqlite_sequence")
            await db._db.execute("COMMIT")
        except BaseException:
            await _srv()._rollback_quietly(db._db)
            raise

    # 清理截图文件
    ss_dir = config.SCREENSHOT_DIR
    if os.path.isdir(ss_dir):
        shutil.rmtree(ss_dir)
        os.makedirs(ss_dir, exist_ok=True)

    return {"ok": True}
