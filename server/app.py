"""
Amazon ASIN 采集系统 v3 - Server
轻量级 FastAPI 服务器，适合 1C/2GB 低配部署
"""
import os
import io
import csv
import json
import re
import asyncio
import logging
import time
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, Request, UploadFile, File, Form, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import openpyxl

from common import config
from common.database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==================== 全局状态 ====================

db: Optional[Database] = None
_worker_registry: Dict[str, Dict] = {}
_WORKER_ID_RE = re.compile(r'^[\w\-]{1,64}$')

# 运行时设置
_runtime_settings: dict = {}
_settings_version: int = 0
_SETTINGS_FILE = os.path.join(config.PROJECT_DIR, "runtime_settings.json")

# 全局并发协调
_global_coordinator: Dict[str, Dict] = {}

# Worker 重启标记
_worker_restart_flags: Dict[str, bool] = {}


def _default_settings() -> dict:
    return {
        "zip_code": config.DEFAULT_ZIP_CODE,
        "max_retries": config.MAX_RETRIES,
        "request_timeout": config.REQUEST_TIMEOUT,
        "session_rotate_every": config.SESSION_ROTATE_EVERY,
        "proxy_url": config.PROXY_URL,
        "token_bucket_rate": config.TOKEN_BUCKET_RATE,
        "initial_concurrency": config.INITIAL_CONCURRENCY,
        "max_concurrency": config.MAX_CONCURRENCY,
        "min_concurrency": config.MIN_CONCURRENCY,
        "global_max_concurrency": config.GLOBAL_MAX_CONCURRENCY,
        "global_max_qps": config.GLOBAL_MAX_QPS,
        "adjust_interval": config.ADJUST_INTERVAL_S,
        "target_latency": config.TARGET_LATENCY_S,
        "max_latency": config.MAX_LATENCY_S,
        "target_success_rate": config.TARGET_SUCCESS_RATE,
        "min_success_rate": config.MIN_SUCCESS_RATE,
        "block_rate_threshold": config.BLOCK_RATE_THRESHOLD,
        "cooldown_after_block": 15,
        "proxy_bandwidth_mbps": config.PROXY_BANDWIDTH_MBPS,
        "screenshot_browsers": 1,
        "screenshot_pages_per_browser": 4,
        "auto_scrape_schedules": [],
    }


def _load_settings():
    global _runtime_settings, _settings_version
    _runtime_settings = _default_settings()
    if os.path.isfile(_SETTINGS_FILE):
        try:
            with open(_SETTINGS_FILE) as f:
                saved = json.load(f)
            _runtime_settings.update(saved)
        except Exception as e:
            logger.warning(f"加载设置失败: {e}")
    _settings_version = 0


def _save_settings():
    try:
        with open(_SETTINGS_FILE, "w") as f:
            json.dump(_runtime_settings, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"保存设置失败: {e}")


def _register_worker(worker_id: str, enable_screenshot: bool = None, ip: str = None):
    if not _WORKER_ID_RE.match(worker_id):
        return
    now = time.time()
    if worker_id not in _worker_registry:
        _worker_registry[worker_id] = {
            "worker_id": worker_id,
            "first_seen": now,
            "last_seen": now,
            "tasks_pulled": 0,
            "results_submitted": 0,
            "enable_screenshot": True,
            "ip": ip,
        }
    _worker_registry[worker_id]["last_seen"] = now
    if enable_screenshot is not None:
        _worker_registry[worker_id]["enable_screenshot"] = enable_screenshot
    if ip is not None:
        _worker_registry[worker_id]["ip"] = ip


# ==================== 生命周期 ====================

@asynccontextmanager
async def lifespan(app):
    global db
    db = Database()
    await db.connect()
    _load_settings()
    os.makedirs(config.EXPORT_DIR, exist_ok=True)
    os.makedirs(config.SCREENSHOT_DIR, exist_ok=True)
    logger.info("数据库初始化完成")
    asyncio.create_task(_timeout_task_loop())
    asyncio.create_task(_auto_scrape_scheduler())
    yield
    if db:
        await db.close()
    logger.info("服务器关闭")


app = FastAPI(title="Amazon Scraper v3", version="3.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=config.STATIC_DIR), name="static")
templates = Jinja2Templates(directory=config.TEMPLATE_DIR)


# ==================== 后台任务 ====================

async def _timeout_task_loop():
    """每 30s 回退超时任务、清理离线 worker"""
    while True:
        await asyncio.sleep(30)
        try:
            await db.reset_timeout_tasks()
            # 清理 5 分钟无心跳的 worker
            cutoff = time.time() - 300
            dead = [wid for wid, w in _worker_registry.items() if w["last_seen"] < cutoff]
            for wid in dead:
                del _worker_registry[wid]
                _global_coordinator.pop(wid, None)
        except Exception as e:
            logger.error(f"超时任务循环异常: {e}")


async def _auto_scrape_scheduler():
    """定时自动采集调度"""
    while True:
        await asyncio.sleep(60)
        try:
            schedules = _runtime_settings.get("auto_scrape_schedules", [])
            now = datetime.now()
            for sched in schedules:
                if not sched.get("enabled"):
                    continue
                sched_time = sched.get("time", "")
                if not sched_time:
                    continue
                try:
                    hour, minute = map(int, sched_time.split(":"))
                except ValueError:
                    continue

                today = now.strftime("%Y-%m-%d")
                if sched.get("last_run_date") == today:
                    continue
                if now.hour < hour or (now.hour == hour and now.minute < minute):
                    continue

                # 创建自动采集批次
                asins = await db.get_all_asins()
                if not asins:
                    continue

                batch_name = f"auto_{now.strftime('%Y%m%d_%H%M')}"
                batch_id = await db.create_batch(batch_name)
                await db.create_tasks(batch_id, asins, _runtime_settings.get("zip_code", "10001"))
                sched["last_run_date"] = today
                _save_settings()
                logger.info(f"自动采集已调度: {batch_name}, {len(asins)} ASINs")
        except Exception as e:
            logger.error(f"自动采集调度异常: {e}")


# ==================== 全局并发协调 ====================

def _allocate_quotas():
    """根据 worker 健康度分配并发配额"""
    active = {wid: info for wid, info in _global_coordinator.items()
              if wid in _worker_registry}
    if not active:
        return

    max_conc = _runtime_settings.get("global_max_concurrency", config.GLOBAL_MAX_CONCURRENCY)
    max_qps = _runtime_settings.get("global_max_qps", config.GLOBAL_MAX_QPS)

    # 健康度加权分配
    scores = {}
    for wid, info in active.items():
        metrics = info.get("metrics", {})
        sr = metrics.get("success_rate", 1.0)
        br = metrics.get("block_rate", 0.0)
        scores[wid] = max(0.1, sr * (1 - br * 5))

    total_score = sum(scores.values())
    per_worker_max_conc = _runtime_settings.get("max_concurrency", config.MAX_CONCURRENCY)
    per_worker_max_qps = _runtime_settings.get("token_bucket_rate", config.TOKEN_BUCKET_RATE)

    for wid in active:
        weight = scores[wid] / total_score if total_score > 0 else 1.0 / len(active)
        allocated_conc = max(config.MIN_CONCURRENCY, int(max_conc * weight))
        allocated_qps = max(1.0, max_qps * weight)
        active[wid]["quota"] = {
            # 取 global 分配值和单 worker 上限的较小值
            "max_concurrency": min(allocated_conc, per_worker_max_conc),
            "max_qps": min(allocated_qps, per_worker_max_qps),
        }


# ==================== HTML 页面 ====================

@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    progress = await db.get_progress()
    total = progress["done"] + progress["failed"]
    progress["completion_rate"] = round(progress["done"] / progress["total"] * 100, 1) if progress["total"] else 0
    progress["success_rate"] = round(progress["done"] / total * 100, 1) if total else 0
    batches = await db.get_batches()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "progress": progress,
        "batches": batches,
        "active_workers": len([w for w in _worker_registry.values() if time.time() - w["last_seen"] < 60]),
    })


@app.get("/tasks", response_class=HTMLResponse)
async def page_tasks(request: Request):
    batches = await db.get_batches()
    return templates.TemplateResponse("tasks.html", {
        "request": request,
        "batches": batches,
        "default_zip_code": _runtime_settings.get("zip_code", config.DEFAULT_ZIP_CODE),
    })


@app.get("/results", response_class=HTMLResponse)
async def page_results(request: Request):
    return templates.TemplateResponse("results.html", {"request": request})


@app.get("/workers", response_class=HTMLResponse)
async def page_workers(request: Request):
    return templates.TemplateResponse("workers.html", {"request": request})


@app.get("/settings", response_class=HTMLResponse)
async def page_settings(request: Request):
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": _runtime_settings,
        "config": {
            "port": config.SERVER_PORT,
            "timeout": config.REQUEST_TIMEOUT,
            "db_path": config.DB_PATH,
        },
    })


# ==================== API: 批次和任务 ====================

@app.post("/api/upload")
async def api_upload(request: Request,
                     file: UploadFile = File(...),
                     batch_name: str = Form(None),
                     zip_code: str = Form(None),
                     needs_screenshot: bool = Form(False)):
    """上传 ASIN 文件创建批次"""
    content = await file.read()
    filename = file.filename or ""

    asins = []
    if filename.endswith(".xlsx"):
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=1, values_only=True):
            for cell in row:
                if cell:
                    val = str(cell).strip().upper()
                    if re.match(r'^B[0-9A-Z]{9}$', val):
                        asins.append(val)
        wb.close()
    elif filename.endswith(".csv"):
        text = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            for cell in row:
                val = cell.strip().upper()
                if re.match(r'^B[0-9A-Z]{9}$', val):
                    asins.append(val)
    else:
        text = content.decode("utf-8", errors="ignore")
        for line in text.splitlines():
            val = line.strip().upper()
            if re.match(r'^B[0-9A-Z]{9}$', val):
                asins.append(val)

    if not asins:
        raise HTTPException(400, "未找到有效 ASIN")

    # 去重
    seen = set()
    unique_asins = []
    for a in asins:
        if a not in seen:
            unique_asins.append(a)
            seen.add(a)

    if not batch_name:
        batch_name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    zc = zip_code or _runtime_settings.get("zip_code", config.DEFAULT_ZIP_CODE)
    batch_id = await db.create_batch(batch_name, needs_screenshot)
    inserted = await db.create_tasks(batch_id, unique_asins, zc, needs_screenshot)

    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "total_asins": len(unique_asins),
        "inserted": inserted,
    }


@app.get("/api/batches")
async def api_batches():
    batches = await db.get_batches()
    return {"batches": batches}


@app.get("/api/progress")
async def api_progress(batch_id: int = None):
    return await db.get_progress(batch_id)


@app.get("/api/batches/{batch_name}/screenshots/progress")
async def api_screenshot_progress(batch_name: str):
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")
    return await db.get_screenshot_progress(batch["id"])


@app.post("/api/batches/{batch_id}/prioritize")
async def api_prioritize(batch_id: int):
    await db.prioritize_batch(batch_id)
    return {"ok": True}


@app.post("/api/batches/{batch_name}/retry")
async def api_retry_batch(batch_name: str):
    """重试失败任务"""
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")
    batch_id = batch["id"]
    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute(
            "UPDATE tasks SET status='pending', retry_count=0, worker_id=NULL WHERE batch_id=? AND status='failed'",
            (batch_id,)
        )
        await db._db.execute("COMMIT")
    return {"ok": True}


@app.delete("/api/batches/{batch_name}")
async def api_delete_batch(batch_name: str):
    """删除批次及其任务"""
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")
    batch_id = batch["id"]
    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute("DELETE FROM tasks WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM batch_asins WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM screenshots WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM asin_changes WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM batches WHERE id=?", (batch_id,))
        await db._db.execute("COMMIT")
    return {"ok": True}


@app.get("/api/batches/{batch_name}/errors")
async def api_batch_errors(batch_name: str):
    """获取批次错误详情"""
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")
    batch_id = batch["id"]
    async with db._db.execute(
        "SELECT error_type, COUNT(*) as cnt FROM tasks WHERE batch_id=? AND status='failed' GROUP BY error_type",
        (batch_id,)
    ) as c:
        error_summary = [dict(r) for r in await c.fetchall()]
    async with db._db.execute(
        "SELECT asin, error_type, error_detail, retry_count, updated_at FROM tasks WHERE batch_id=? AND status='failed' ORDER BY updated_at DESC LIMIT 100",
        (batch_id,)
    ) as c:
        failed_tasks = [dict(r) for r in await c.fetchall()]
    return {"error_summary": error_summary, "failed_tasks": failed_tasks}


@app.get("/api/coordinator")
async def api_coordinator():
    """全局并发协调器状态"""
    max_conc = _runtime_settings.get("global_max_concurrency", config.GLOBAL_MAX_CONCURRENCY)
    max_qps = _runtime_settings.get("global_max_qps", config.GLOBAL_MAX_QPS)
    allocated_conc = sum(info.get("quota", {}).get("max_concurrency", 0)
                        for info in _global_coordinator.values())
    allocated_qps = sum(info.get("quota", {}).get("max_qps", 0)
                        for info in _global_coordinator.values())
    return {
        "max_concurrency": max_conc,
        "allocated_concurrency": allocated_conc,
        "max_qps": max_qps,
        "allocated_qps": allocated_qps,
        "active_workers": len(_global_coordinator),
        "global_block_until": 0,
        "global_block_count": 0,
    }


@app.delete("/api/workers")
async def api_delete_all_offline():
    """清除所有离线 worker"""
    cutoff = time.time() - 60
    offline = [wid for wid, w in _worker_registry.items() if w["last_seen"] < cutoff]
    for wid in offline:
        del _worker_registry[wid]
        _global_coordinator.pop(wid, None)
    return {"ok": True, "removed": len(offline)}


@app.post("/api/workers/{worker_id}/restart")
async def api_restart_worker(worker_id: str):
    """标记 worker 软重启（下次 sync 时生效）"""
    _worker_restart_flags[worker_id] = True
    return {"ok": True, "message": f"重启指令已下发，{worker_id} 将在 30 秒内重启"}


@app.post("/api/settings/reset")
async def api_reset_settings():
    """恢复默认设置"""
    global _runtime_settings, _settings_version
    _runtime_settings = _default_settings()
    _settings_version += 1
    _save_settings()
    return {"ok": True, "settings": _runtime_settings}


# ==================== API: Worker 任务拉取和提交 ====================

@app.get("/api/tasks/pull")
async def api_pull_tasks(request: Request,
                         worker_id: str = Query(...),
                         count: int = Query(10),
                         needs_screenshot: Optional[bool] = Query(None),
                         enable_screenshot: Optional[bool] = Query(None)):
    ip = request.client.host if request.client else None
    # enable_screenshot 来自 worker 的 query param，更新 registry
    _register_worker(worker_id, enable_screenshot=enable_screenshot, ip=ip)

    ns = None
    if needs_screenshot is not None:
        ns = needs_screenshot
    elif enable_screenshot is not None:
        ns = enable_screenshot
    elif worker_id in _worker_registry:
        if not _worker_registry[worker_id].get("enable_screenshot", True):
            ns = False

    tasks = await db.pull_tasks(worker_id, count, ns)
    if worker_id in _worker_registry:
        _worker_registry[worker_id]["tasks_pulled"] += len(tasks)
    return {"tasks": tasks}


@app.post("/api/tasks/release")
async def api_release_tasks(request: Request):
    body = await request.json()
    task_ids = body.get("task_ids", [])
    await db.release_tasks(task_ids)
    return {"ok": True}


@app.post("/api/tasks/result")
async def api_submit_result(request: Request):
    """提交单个结果"""
    data = await request.json()
    task_id = data.pop("task_id", None)
    batch_id = data.pop("batch_id", None)

    saved = await db.save_result(data, batch_id)
    if task_id and saved:
        await db.complete_task(task_id)
    elif task_id and not saved:
        await db.fail_task(task_id, "parse_error", "save_result returned False")

    worker_id = data.get("worker_id", "")
    if worker_id in _worker_registry:
        _worker_registry[worker_id]["results_submitted"] += 1

    return {"ok": saved}


@app.post("/api/tasks/result/batch")
async def api_submit_batch(request: Request):
    """批量提交结果"""
    body = await request.json()
    results = body.get("results", [])
    saved = 0
    for item in results:
        task_id = item.pop("task_id", None)
        batch_id = item.pop("batch_id", None)
        ok = await db.save_result(item, batch_id)
        if task_id:
            if ok:
                await db.complete_task(task_id)
            else:
                await db.fail_task(task_id, "parse_error", "save_result returned False")
        if ok:
            saved += 1

        worker_id = item.get("worker_id", "")
        if worker_id in _worker_registry:
            _worker_registry[worker_id]["results_submitted"] += 1

    return {"saved": saved, "total": len(results)}


@app.post("/api/tasks/screenshot")
async def api_upload_screenshot(request: Request,
                                asin: str = Form(...),
                                batch_name: str = Form(...),
                                file: UploadFile = File(...)):
    """接收截图上传"""
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(400, f"批次不存在: {batch_name}")

    batch_id = batch["id"]
    save_dir = os.path.join(config.SCREENSHOT_DIR, batch_name)
    os.makedirs(save_dir, exist_ok=True)

    filename = f"{asin}.png"
    filepath = os.path.join(save_dir, filename)
    content = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)

    rel_path = f"/static/screenshots/{batch_name}/{filename}"
    updated = await db.update_screenshot_status(asin, batch_id, "done", file_path=rel_path)
    if not updated:
        try:
            os.remove(filepath)
        except OSError:
            pass
        raise HTTPException(409, f"截图状态不存在: {asin}@{batch_name}")
    return {"ok": True, "path": rel_path}


@app.post("/api/tasks/screenshot/fail")
async def api_screenshot_fail(request: Request):
    """截图渲染失败上报（触发重试或标记永久失败）"""
    body = await request.json()
    asin = body.get("asin", "")
    batch_name = body.get("batch_name", "")
    error = body.get("error", "unknown")
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(400, f"批次不存在: {batch_name}")
    updated = await db.update_screenshot_status(asin, batch["id"], "failed", error=error)
    if not updated:
        raise HTTPException(409, f"截图状态不存在: {asin}@{batch_name}")
    return {"ok": True}


# ==================== API: Worker 同步 ====================

@app.post("/api/worker/sync")
async def api_worker_sync(request: Request):
    """Worker 心跳 + 指标上报 + 设置/配额下发"""
    body = await request.json()
    worker_id = body.get("worker_id", "")
    ip = request.client.host if request.client else None

    _register_worker(worker_id, body.get("enable_screenshot"), ip)

    # 更新指标
    metrics = body.get("metrics", {})
    if worker_id not in _global_coordinator:
        _global_coordinator[worker_id] = {"metrics": {}, "quota": {}}
    _global_coordinator[worker_id]["metrics"] = metrics
    _allocate_quotas()

    quota = _global_coordinator.get(worker_id, {}).get("quota", {})

    # 检查重启标记
    restart = _worker_restart_flags.pop(worker_id, False)

    return {
        "settings": _runtime_settings,
        "settings_version": _settings_version,
        "quota": quota,
        "restart": restart,
    }


@app.get("/api/workers")
async def api_workers():
    now = time.time()
    workers = []
    for wid, w in _worker_registry.items():
        coord = _global_coordinator.get(wid, {})
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
            "quota_concurrency": quota.get("max_concurrency"),
            "quota_qps": quota.get("max_qps"),
        })
    return {"workers": workers}


@app.delete("/api/workers/{worker_id}")
async def api_delete_worker(worker_id: str):
    _worker_registry.pop(worker_id, None)
    _global_coordinator.pop(worker_id, None)
    return {"ok": True}


# ==================== API: 结果查询 ====================

@app.get("/api/results")
async def api_results(batch_id: int = None,
                      cursor: int = None,
                      limit: int = Query(50, le=200),
                      search: str = None,
                      change_filter: str = "all",
                      direction: str = "next"):
    result = await db.get_results(
        batch_id=batch_id,
        cursor_id=cursor,
        limit=limit,
        search=search,
        change_filter=change_filter,
        direction=direction,
    )
    return result


@app.get("/api/results/{asin}")
async def api_result_detail(asin: str):
    data = await db.get_result_by_asin(asin)
    if not data:
        raise HTTPException(404, f"ASIN {asin} 不存在")
    changes = await db.get_asin_changes(asin)
    return {"data": data, "changes": changes}


@app.get("/api/changes/stats")
async def api_change_stats(batch_id: int = None):
    return await db.get_change_stats(batch_id)


# ==================== API: 设置 ====================

@app.get("/api/settings")
async def api_get_settings():
    return {"settings": _runtime_settings, "version": _settings_version}


@app.put("/api/settings")
async def api_update_settings(request: Request):
    global _settings_version
    body = await request.json()

    for key, value in body.items():
        if key in _runtime_settings:
            _runtime_settings[key] = value

    _settings_version += 1
    _save_settings()
    return {"ok": True, "version": _settings_version, "settings": _runtime_settings}


# ==================== API: 导出 ====================

from common.models import EXPORTABLE_FIELDS
from common.database import _parse_price_float


def _parse_selected_fields(fields_param: str = None):
    """解析并校验字段选择，返回 None 表示全选"""
    if not fields_param:
        return None
    selected = [f for f in fields_param.split(",") if f in EXPORTABLE_FIELDS]
    return selected if selected else None


def _get_export_headers(selected_fields=None):
    """构建导出表头和字段键"""
    if selected_fields is None:
        selected_fields = list(EXPORTABLE_FIELDS)

    include_total = "total_price" in selected_fields
    field_keys = [f for f in selected_fields if f != "total_price"]
    headers = [config.HEADER_MAP.get(f, f) for f in field_keys]

    if include_total:
        shipping_h = config.HEADER_MAP.get("buybox_shipping", "buybox_shipping")
        idx = headers.index(shipping_h) + 1 if shipping_h in headers else len(headers)
        headers.insert(idx, config.HEADER_MAP.get("total_price", "总价"))

    return headers, field_keys, include_total


def _prepare_row(item: dict, field_keys: list, headers: list, include_total: bool):
    """构建单行导出数据"""
    row = [str(item.get(f, "") or "") for f in field_keys]
    if include_total:
        bp = _parse_price_float(item.get("buybox_price", ""))
        bs_str = str(item.get("buybox_shipping", ""))
        if bp is not None:
            bs = 0.0 if bs_str.upper() == "FREE" else (_parse_price_float(bs_str) or 0.0)
            total = f"${bp + bs:.2f}"
        else:
            total = ""
        shipping_h = config.HEADER_MAP.get("buybox_shipping", "buybox_shipping")
        idx = headers.index(shipping_h) + 1 if shipping_h in headers else len(row)
        row.insert(idx, total)
    return row


@app.get("/api/export/fields")
async def api_export_fields():
    """返回可导出字段列表"""
    return {
        "fields": EXPORTABLE_FIELDS,
        "headers": {f: config.HEADER_MAP.get(f, f) for f in EXPORTABLE_FIELDS},
    }


@app.get("/api/export/all")
async def api_export_all(format: str = "xlsx", change_filter: str = "all", fields: str = None):
    selected = _parse_selected_fields(fields)
    name = f"all_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if format == "csv":
        return await _export_csv_streaming(name, batch_id=None, change_filter=change_filter, selected_fields=selected)
    else:
        return await _export_xlsx_streaming(name, batch_id=None, change_filter=change_filter, selected_fields=selected)


@app.get("/api/export/{batch_name}")
async def api_export_batch(batch_name: str, format: str = "xlsx", change_filter: str = "all", fields: str = None):
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")
    selected = _parse_selected_fields(fields)
    if format == "csv":
        return await _export_csv_streaming(batch_name, batch_id=batch["id"], change_filter=change_filter, selected_fields=selected)
    else:
        return await _export_xlsx_streaming(batch_name, batch_id=batch["id"], change_filter=change_filter, selected_fields=selected)


async def _export_xlsx_streaming(filename: str, batch_id: int = None,
                                  change_filter: str = "all", selected_fields=None):
    """write_only 模式 + 临时文件 + 流式响应（百万级不 OOM）"""
    import tempfile
    headers, field_keys, include_total = _get_export_headers(selected_fields)
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(title="采集结果")
    ws.append(headers)

    count = 0
    async for item in db.iter_results(batch_id, change_filter=change_filter):
        ws.append(_prepare_row(item, field_keys, headers, include_total))
        count += 1

    if count == 0:
        wb.close()
        raise HTTPException(404, "无数据")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    try:
        wb.save(tmp_path)
    except Exception:
        wb.close()
        os.unlink(tmp_path)
        raise
    wb.close()

    async def stream_and_cleanup():
        try:
            with open(tmp_path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    yield chunk
        finally:
            os.unlink(tmp_path)

    safe = re.sub(r'[^a-zA-Z0-9_\-]', '_', filename)
    return StreamingResponse(
        stream_and_cleanup(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={safe}.xlsx"},
    )


async def _export_csv_streaming(filename: str, batch_id: int = None,
                                 change_filter: str = "all", selected_fields=None):
    """逐行 yield 的真流式 CSV（百万级不 OOM）"""
    headers, field_keys, include_total = _get_export_headers(selected_fields)

    async def generate():
        out = io.StringIO()
        csv.writer(out).writerow(headers)
        yield out.getvalue().encode("utf-8-sig")

        async for item in db.iter_results(batch_id, change_filter=change_filter):
            out = io.StringIO()
            csv.writer(out).writerow(_prepare_row(item, field_keys, headers, include_total))
            yield out.getvalue().encode("utf-8")

    safe = re.sub(r'[^a-zA-Z0-9_\-]', '_', filename)
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={safe}.csv"},
    )


@app.get("/api/export/{batch_name}/screenshots")
async def api_export_screenshots(batch_name: str):
    ss_dir = os.path.join(config.SCREENSHOT_DIR, batch_name)
    if not os.path.isdir(ss_dir):
        raise HTTPException(404, "无截图文件")

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(ss_dir):
            if fname.endswith(".png"):
                zf.write(os.path.join(ss_dir, fname), fname)

    output.seek(0)
    filename = f"screenshots_{batch_name}.zip"
    return StreamingResponse(
        output,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ==================== API: 诊断 ====================

@app.get("/api/diagnostic")
async def api_diagnostic():
    total_asins = await db.get_total_asins()
    progress = await db.get_progress()
    return {
        "total_asins": total_asins,
        "task_progress": progress,
        "active_workers": len(_worker_registry),
        "settings_version": _settings_version,
    }


@app.delete("/api/database")
async def api_clear_database():
    """清空所有数据 + 截图文件"""
    import shutil
    async with db._write_lock:
        await db._db.execute("BEGIN")
        for table in ["asin_changes", "asin_data", "batch_asins", "tasks", "screenshots", "batches"]:
            await db._db.execute(f"DELETE FROM {table}")
        await db._db.execute("DELETE FROM sqlite_sequence")
        await db._db.execute("COMMIT")

    # 清理截图文件
    ss_dir = config.SCREENSHOT_DIR
    if os.path.isdir(ss_dir):
        shutil.rmtree(ss_dir)
        os.makedirs(ss_dir, exist_ok=True)

    return {"ok": True}


# ==================== 入口 ====================

def main():
    import uvicorn
    uvicorn.run(
        "server.app:app",
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
