"""Worker 机群：注册表 / 心跳 / 配额 / 协调器（6 个端点）—— Phase 3.3 从
`server/app.py` 拆出。

    GET    /api/coordinator
    DELETE /api/workers                    （清离线）
    POST   /api/workers/{worker_id}/restart
    POST   /api/worker/sync                （心跳）
    GET    /api/workers
    DELETE /api/workers/{worker_id}

------------------------------------------------------------------------
承重约束
------------------------------------------------------------------------

1. **`_worker_registry` / `_global_coordinator` / `_worker_restart_flags` /
   `_runtime_settings` / `_settings_version` 一个都不搬**，全部留在
   `server/app.py`，这里一律 `_srv().xxx`。
   `tests/golden/harness.py` 在每个样本前按名字 `srv._worker_registry.clear()` /
   `srv._global_coordinator.clear()` / `srv._worker_restart_flags.clear()` /
   `srv._settings_version = 0` —— 名字搬走 = 补丁打空 = 样本随执行顺序漂移。
   `_settings_version` 还是个 **int**：from-import 会把值快照下来，
   之后 `/api/settings` 每改一次它都不再跟着涨，而 worker 靠它判断要不要拉设置。

2. **`_register_worker` 与 `_allocate_quotas` 留在 `app.py`**（它们直接读写上面
   那几个全局，且 `_allocate_quotas` 还被别处调用），这里走
   `_srv()._register_worker(...)` / `_srv()._allocate_quotas()`。

3. **router 光秃**：`APIRouter()`，不带 `tags=` / `prefix=`。
   `/openapi.json` 是黄金基线的一步、逐字节钉死，而整份 schema 里没有 `tags` 键。

4. **函数名 / docstring / 路径一个字不改** —— 它们进 `operationId` / `summary`。

5. **路由遮蔽无风险**（已复核）：`/api/workers` 是静态路径，
   与 `/api/workers/{worker_id}` 不会互相吃；后者的 DELETE 与
   `/api/workers/{worker_id}/restart` 的 POST 方法也不同。
"""

import time

from fastapi import APIRouter, Request

from common import config


def _srv():
    from server import app as _s
    return _s


router = APIRouter()


@router.get("/api/coordinator")
async def api_coordinator():
    """全局并发协调器状态"""
    _s = _srv()
    max_conc = _s._runtime_settings.get("global_max_concurrency", config.GLOBAL_MAX_CONCURRENCY)
    max_qps = _s._runtime_settings.get("global_max_qps", config.GLOBAL_MAX_QPS)
    allocated_conc = sum(info.get("quota", {}).get("max_concurrency", 0)
                         for info in _s._global_coordinator.values())
    allocated_qps = sum(info.get("quota", {}).get("max_qps", 0)
                        for info in _s._global_coordinator.values())
    return {
        "max_concurrency": max_conc,
        "allocated_concurrency": allocated_conc,
        "max_qps": max_qps,
        "allocated_qps": allocated_qps,
        "active_workers": len(_s._global_coordinator),
        "global_block_until": 0,
        "global_block_count": 0,
    }


@router.delete("/api/workers")
async def api_delete_all_offline():
    """清除所有离线 worker"""
    _s = _srv()
    cutoff = time.time() - 60
    offline = [wid for wid, w in _s._worker_registry.items() if w["last_seen"] < cutoff]
    for wid in offline:
        del _s._worker_registry[wid]
        _s._global_coordinator.pop(wid, None)
    return {"ok": True, "removed": len(offline)}


@router.post("/api/workers/{worker_id}/restart")
async def api_restart_worker(worker_id: str):
    """标记 worker 软重启（下次 sync 时生效）"""
    _srv()._worker_restart_flags[worker_id] = True
    return {"ok": True, "message": f"重启指令已下发，{worker_id} 将在 30 秒内重启"}


# ==================== API: Worker 同步 ====================

@router.post("/api/worker/sync")
async def api_worker_sync(request: Request):
    """Worker 心跳 + 指标上报 + 设置/配额下发"""
    _s = _srv()
    body = await request.json()
    worker_id = body.get("worker_id", "")
    ip = request.client.host if request.client else None

    _s._register_worker(worker_id, body.get("enable_screenshot"), ip)

    # 更新指标
    metrics = body.get("metrics", {})
    if worker_id not in _s._global_coordinator:
        _s._global_coordinator[worker_id] = {"metrics": {}, "quota": {}}
    _s._global_coordinator[worker_id]["metrics"] = metrics
    _s._allocate_quotas()

    quota = _s._global_coordinator.get(worker_id, {}).get("quota", {})

    # 检查重启标记
    restart = _s._worker_restart_flags.pop(worker_id, False)

    return {
        "settings": _s._runtime_settings,
        "settings_version": _s._settings_version,
        "quota": quota,
        "restart": restart,
    }


@router.get("/api/workers")
async def api_workers():
    _s = _srv()
    now = time.time()
    workers = []
    for wid, w in _s._worker_registry.items():
        coord = _s._global_coordinator.get(wid, {})
        metrics = coord.get("metrics", {})
        quota = coord.get("quota", {})
        uptime = now - w.get("first_seen", now)
        workers.append({
            **w,
            "online": (now - w["last_seen"]) < 60,
            "last_seen_ago": int(now - w["last_seen"]),
            "uptime": int(uptime),
            "success_rate": metrics.get("success_rate"),
            "block_rate": metrics.get("block_rate"),
            "latency_p50": metrics.get("latency_p50"),
            "inflight": metrics.get("inflight"),
            "task_queue_size": metrics.get("task_queue_size"),
            "result_queue_size": metrics.get("result_queue_size"),
            "accepted": metrics.get("accepted", 0),
            "stale": metrics.get("stale", 0),
            "quota_concurrency": quota.get("max_concurrency"),
            "quota_qps": quota.get("max_qps"),
        })
    return {"workers": workers}


@router.delete("/api/workers/{worker_id}")
async def api_delete_worker(worker_id: str):
    _s = _srv()
    _s._worker_registry.pop(worker_id, None)
    _s._global_coordinator.pop(worker_id, None)
    return {"ok": True}
