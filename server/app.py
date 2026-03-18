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


def _remove_screenshot_files(file_paths: list):
    """删除截图物理文件，忽略不存在的"""
    for fp in file_paths:
        if not fp:
            continue
        # file_path 可能是相对路径 (/static/screenshots/...) 或绝对路径
        if fp.startswith("/static/"):
            full = os.path.join(config.PROJECT_DIR, "server", fp.lstrip("/"))
        else:
            full = fp
        try:
            if os.path.isfile(full):
                os.remove(full)
        except OSError:
            pass


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
    os.makedirs(_SCHEDULES_DIR, exist_ok=True)
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
    """每 30s 心跳感知回收 + 硬超时兜底 + 清理离线 worker"""
    while True:
        await asyncio.sleep(30)
        try:
            # 识别死 Worker：2 分钟无心跳
            now = time.time()
            heartbeat_cutoff = now - 120
            dead_worker_ids = [
                wid for wid, w in _worker_registry.items()
                if w["last_seen"] < heartbeat_cutoff
            ]

            # 回收死 Worker 的 processing 任务 + 硬超时兜底（合成一条 SQL 防双重 epoch bump）
            await db.reclaim_dead_worker_tasks(dead_worker_ids)

            # 清理 5 分钟无心跳的 worker 注册信息
            offline_cutoff = now - 300
            truly_dead = [wid for wid, w in _worker_registry.items() if w["last_seen"] < offline_cutoff]
            for wid in truly_dead:
                del _worker_registry[wid]
                _global_coordinator.pop(wid, None)

            # 清理导出临时文件（超过 1 小时的 export_*.xlsx）
            try:
                export_dir = config.EXPORT_DIR
                if os.path.isdir(export_dir):
                    for fname in os.listdir(export_dir):
                        if fname.startswith("export_") and fname.endswith(".xlsx"):
                            fpath = os.path.join(export_dir, fname)
                            if now - os.path.getmtime(fpath) > 3600:
                                os.remove(fpath)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"超时任务循环异常: {e}")


async def _auto_scrape_scheduler():
    """定时自动采集调度（支持 interval_days 和文件指定 ASIN）"""
    while True:
        await asyncio.sleep(60)
        try:
            schedules = _runtime_settings.get("auto_scrape_schedules", [])
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            changed = False

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

                # 检查时间点是否到达
                if now.hour < hour or (now.hour == hour and now.minute < minute):
                    continue

                # 检查间隔天数
                interval = sched.get("interval_days", 1)
                last_run = sched.get("last_run_date", "")
                if last_run:
                    try:
                        last_date = datetime.strptime(last_run, "%Y-%m-%d")
                        if (now - last_date).days < interval:
                            continue
                    except ValueError:
                        pass

                # 获取 ASIN 列表：优先从文件，否则全库
                source_file = sched.get("source_file", "")
                if source_file and os.path.isfile(source_file):
                    asins = _extract_asins_from_file(source_file)
                else:
                    asins = await db.get_all_asins()

                if not asins:
                    continue

                sched_name = sched.get("name", "task")
                batch_name = f"auto_{sched_name}_{now:%Y%m%d_%H%M}"
                zc = _runtime_settings.get("zip_code", config.DEFAULT_ZIP_CODE)
                ns = sched.get("needs_screenshot", False)
                batch_id = await db.create_batch(batch_name, ns)
                await db.create_tasks(batch_id, asins, zc, ns)
                sched["last_run_date"] = today
                changed = True
                logger.info(f"自动采集已调度: {batch_name}, {len(asins)} ASINs (间隔{interval}天)")

            if changed:
                _save_settings()
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
    # 先收集截图物理文件路径
    screenshot_files = []
    async with db._db.execute(
        "SELECT file_path FROM screenshots WHERE batch_id=? AND file_path IS NOT NULL", (batch_id,)
    ) as c:
        screenshot_files = [row["file_path"] for row in await c.fetchall()]

    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute("DELETE FROM tasks WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM batch_asins WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM screenshots WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM asin_changes WHERE batch_id=?", (batch_id,))
        await db._db.execute("DELETE FROM batches WHERE id=?", (batch_id,))
        await db._db.execute("COMMIT")

    # 删除物理截图文件
    _remove_screenshot_files(screenshot_files)
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

    # ns=None: 不限制; ns=False: 只拉不需要截图的任务
    # enable_screenshot 表示 worker 是否有截图能力：
    #   True  → 拉所有任务 (ns=None)
    #   False → 只拉不需要截图的 (ns=False)
    ns = None
    if needs_screenshot is not None:
        ns = needs_screenshot
    elif enable_screenshot is not None:
        if not enable_screenshot:
            ns = False
        # enable_screenshot=True → ns=None (拉所有任务)
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
    worker_id = body.get("worker_id", "")
    tasks = body.get("tasks", [])
    # 兼容旧格式 {"task_ids": [1,2,3]}（无 lease 校验，直接释放）
    if not tasks and "task_ids" in body:
        task_ids = body["task_ids"]
        if task_ids:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            placeholders = ",".join("?" * len(task_ids))
            async with db._write_lock:
                await db._db.execute("BEGIN")
                cursor = await db._db.execute(
                    f"UPDATE tasks SET status='pending', worker_id=NULL, "
                    f"lease_epoch=lease_epoch+1, updated_at=? "
                    f"WHERE id IN ({placeholders}) AND status='processing'",
                    [now] + task_ids
                )
                await db._db.execute("COMMIT")
            return {"ok": True, "released": cursor.rowcount}
        return {"ok": True, "released": 0}
    released = await db.release_tasks(worker_id, tasks)
    return {"ok": True, "released": released}


@app.post("/api/tasks/result")
async def api_submit_result(request: Request):
    """提交单个结果（lease 校验 → 原子写入）"""
    data = await request.json()
    task_id = data.pop("task_id", None)
    batch_id = data.pop("batch_id", None)
    worker_id = data.get("worker_id", "")
    lease_epoch = data.pop("lease_epoch", 0)

    if task_id:
        if data.get("success", True):
            result = await db.accept_success_result(task_id, worker_id, lease_epoch, data, batch_id)
        else:
            result = await db.accept_failed_result(
                task_id, worker_id, lease_epoch,
                data.get("error_type", ""), data.get("error_detail", ""))
        if worker_id in _worker_registry and result.get("accepted"):
            _worker_registry[worker_id]["results_submitted"] += 1
        return {"ok": result.get("accepted", False), "stale": result.get("stale", False)}
    else:
        # 无 task_id 的直接写入（兼容）
        saved = await db.save_result(data, batch_id)
        return {"ok": saved, "stale": False}


@app.post("/api/tasks/result/batch")
async def api_submit_batch(request: Request):
    """批量提交结果（单事务，减少锁争用）"""
    body = await request.json()
    results = body.get("results", [])

    # 构建 batch items
    batch_items = []
    worker_id_set = set()
    for item in results:
        task_id = item.pop("task_id", None)
        batch_id = item.pop("batch_id", None)
        worker_id = item.get("worker_id", "")
        lease_epoch = item.pop("lease_epoch", 0)
        is_success = item.pop("success", True)
        worker_id_set.add(worker_id)
        batch_items.append({
            "task_id": task_id,
            "worker_id": worker_id,
            "lease_epoch": lease_epoch,
            "batch_id": batch_id,
            "data": item,
            "success": is_success,
        })

    result = await db.accept_results_batch(batch_items)

    # results_submitted 只计 accepted
    for wid in worker_id_set:
        if wid in _worker_registry and result["accepted"] > 0:
            _worker_registry[wid]["results_submitted"] += result["accepted"]

    return {**result, "total": len(results)}


@app.post("/api/tasks/screenshot")
async def api_upload_screenshot(request: Request,
                                asin: str = Form(...),
                                batch_name: str = Form(...),
                                worker_id: str = Form(""),
                                file: UploadFile = File(...)):
    """接收截图上传（检查 worker 存活状态）"""
    # 拒绝已死 worker 的截图上传
    if worker_id and worker_id not in _worker_registry:
        raise HTTPException(409, f"Worker {worker_id} 已离线，截图被丢弃")
    if worker_id and worker_id in _worker_registry:
        if time.time() - _worker_registry[worker_id]["last_seen"] > 120:
            raise HTTPException(409, f"Worker {worker_id} 心跳超时，截图被丢弃")

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
            "task_queue_size": metrics.get("task_queue_size"),
            "result_queue_size": metrics.get("result_queue_size"),
            "accepted": metrics.get("accepted", 0),
            "stale": metrics.get("stale", 0),
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

    os.makedirs(config.EXPORT_DIR, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(
        delete=False, suffix=".xlsx", prefix="export_",
        dir=config.EXPORT_DIR)
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


@app.delete("/api/results")
async def api_delete_results(request: Request):
    """按条件删除采集结果"""
    body = await request.json()
    batch_id = body.get("batch_id")
    asins = body.get("asins")  # list of ASIN strings
    search = body.get("search")  # fuzzy search string

    # 构建 ASIN 列表
    target_asins = set()
    has_explicit_filter = bool(asins or search)

    if asins:
        target_asins.update(asins)

    if search:
        terms = [t.strip() for t in search.split(",") if t.strip()]
        if terms:
            or_clauses = []
            params = []
            for term in terms:
                or_clauses.append("(d.asin LIKE ? OR d.title LIKE ? OR d.brand LIKE ?)")
                params.extend([f"%{term}%", f"%{term}%", f"%{term}%"])
            where = " OR ".join(or_clauses)
            sql = f"SELECT d.asin FROM asin_data d WHERE {where}"
            async with db._db.execute(sql, params) as c:
                rows = await c.fetchall()
                for row in rows:
                    target_asins.add(row["asin"])

    # 纯 batch_id（无 asins/search）→ 删除该批次所有 ASIN
    if batch_id and not has_explicit_filter:
        sql = "SELECT asin FROM batch_asins WHERE batch_id = ?"
        async with db._db.execute(sql, (batch_id,)) as c:
            target_asins = {row["asin"] for row in await c.fetchall()}
    # batch_id + 其他条件 → 取交集
    elif batch_id and target_asins:
        sql = "SELECT asin FROM batch_asins WHERE batch_id = ?"
        async with db._db.execute(sql, (batch_id,)) as c:
            batch_asins = {row["asin"] for row in await c.fetchall()}
        target_asins &= batch_asins

    if not target_asins:
        return {"ok": True, "deleted": 0}

    asin_list = list(target_asins)
    placeholders = ",".join("?" * len(asin_list))

    # 先收集截图物理文件路径
    screenshot_files = []
    async with db._db.execute(
        f"SELECT file_path FROM screenshots WHERE asin IN ({placeholders}) AND file_path IS NOT NULL", asin_list
    ) as c:
        screenshot_files = [row["file_path"] for row in await c.fetchall()]

    async with db._write_lock:
        await db._db.execute("BEGIN")
        await db._db.execute(f"DELETE FROM asin_changes WHERE asin IN ({placeholders})", asin_list)
        await db._db.execute(f"DELETE FROM screenshots WHERE asin IN ({placeholders})", asin_list)
        await db._db.execute(f"DELETE FROM batch_asins WHERE asin IN ({placeholders})", asin_list)
        await db._db.execute(f"DELETE FROM asin_data WHERE asin IN ({placeholders})", asin_list)
        await db._db.execute("COMMIT")

    # 删除物理截图文件
    _remove_screenshot_files(screenshot_files)
    return {"ok": True, "deleted": len(asin_list)}


# ==================== 定时采集管理 ====================

_SCHEDULES_DIR = os.path.join(config.PROJECT_DIR, "data", "schedules")


def _get_schedules() -> list:
    return _runtime_settings.get("auto_scrape_schedules", [])


def _save_schedules(schedules: list):
    _runtime_settings["auto_scrape_schedules"] = schedules
    _save_settings()


def _extract_asins_from_file(filepath: str) -> list:
    """从文件提取 ASIN 列表"""
    asins = []
    seen = set()
    if not os.path.isfile(filepath):
        return asins
    with open(filepath, "rb") as f:
        content = f.read()
    filename = filepath.lower()
    if filename.endswith(".xlsx"):
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=1, values_only=True):
            for cell in row:
                if cell:
                    val = str(cell).strip().upper()
                    if re.match(r'^B[0-9A-Z]{9}$', val) and val not in seen:
                        asins.append(val)
                        seen.add(val)
        wb.close()
    elif filename.endswith(".csv"):
        text = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            for cell in row:
                val = cell.strip().upper()
                if re.match(r'^B[0-9A-Z]{9}$', val) and val not in seen:
                    asins.append(val)
                    seen.add(val)
    else:
        text = content.decode("utf-8", errors="ignore")
        for line in text.splitlines():
            val = line.strip().upper()
            if re.match(r'^B[0-9A-Z]{9}$', val) and val not in seen:
                asins.append(val)
                seen.add(val)
    return asins


@app.get("/api/schedules")
async def api_list_schedules():
    return {"schedules": _get_schedules()}


@app.post("/api/schedules")
async def api_create_schedule(request: Request,
                              file: UploadFile = File(...),
                              name: str = Form(""),
                              time_str: str = Form(..., alias="time"),
                              interval_days: int = Form(1),
                              needs_screenshot: bool = Form(False)):
    """创建定时采集任务"""
    # 验证时间格式
    try:
        h, m = map(int, time_str.split(":"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
    except ValueError:
        raise HTTPException(400, "时间格式错误，应为 HH:MM")

    if interval_days < 1:
        raise HTTPException(400, "间隔天数至少为 1")

    # 保存 ASIN 文件
    os.makedirs(_SCHEDULES_DIR, exist_ok=True)
    import uuid
    sched_id = f"sched_{uuid.uuid4().hex[:8]}"
    ext = os.path.splitext(file.filename or "")[1] or ".txt"
    source_file = os.path.join(_SCHEDULES_DIR, f"{sched_id}{ext}")
    content = await file.read()
    with open(source_file, "wb") as f:
        f.write(content)

    # 验证文件中有 ASIN
    asin_list = _extract_asins_from_file(source_file)
    if not asin_list:
        os.remove(source_file)
        raise HTTPException(400, "文件中未找到有效 ASIN")

    # 首次创建：last_run_date 设为昨天，确保首次检查时立即触发
    from datetime import timedelta
    yesterday = (datetime.now() - timedelta(days=interval_days)).strftime("%Y-%m-%d")

    sched = {
        "id": sched_id,
        "name": name or f"定时任务-{time_str}",
        "time": time_str,
        "interval_days": interval_days,
        "source_file": source_file,
        "asin_count": len(asin_list),
        "needs_screenshot": needs_screenshot,
        "enabled": True,
        "last_run_date": yesterday,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    schedules = _get_schedules()
    schedules.append(sched)
    _save_schedules(schedules)

    return {"ok": True, "schedule": sched, "schedules": schedules}


@app.put("/api/schedules/{sched_id}")
async def api_update_schedule(sched_id: str, request: Request):
    """修改定时任务（enabled/time/interval_days/name）"""
    body = await request.json()
    schedules = _get_schedules()
    target = None
    for s in schedules:
        if s.get("id") == sched_id:
            target = s
            break
    if target is None:
        raise HTTPException(404, "定时任务不存在")

    if "enabled" in body:
        target["enabled"] = bool(body["enabled"])
    if "name" in body:
        target["name"] = body["name"]
    if "time" in body:
        try:
            h, m = map(int, body["time"].split(":"))
            if 0 <= h <= 23 and 0 <= m <= 59:
                target["time"] = body["time"]
        except (ValueError, AttributeError):
            pass
    if "interval_days" in body:
        val = int(body["interval_days"])
        if val >= 1:
            target["interval_days"] = val

    _save_schedules(schedules)
    return {"ok": True, "schedules": schedules}


@app.delete("/api/schedules/{sched_id}")
async def api_delete_schedule(sched_id: str):
    """删除定时任务 + ASIN 文件"""
    schedules = _get_schedules()
    target = None
    new_schedules = []
    for s in schedules:
        if s.get("id") == sched_id:
            target = s
        else:
            new_schedules.append(s)
    if target is None:
        raise HTTPException(404, "定时任务不存在")

    # 删除 ASIN 文件
    source_file = target.get("source_file", "")
    if source_file and os.path.isfile(source_file):
        os.remove(source_file)

    _save_schedules(new_schedules)
    return {"ok": True, "schedules": new_schedules}


@app.post("/api/schedules/{sched_id}/run")
async def api_run_schedule_now(sched_id: str):
    """手动立即执行一次定时任务"""
    schedules = _get_schedules()
    target = None
    for s in schedules:
        if s.get("id") == sched_id:
            target = s
            break
    if target is None:
        raise HTTPException(404, "定时任务不存在")

    source_file = target.get("source_file", "")
    asin_list = _extract_asins_from_file(source_file)
    if not asin_list:
        raise HTTPException(400, "ASIN 文件为空或不存在")

    now = datetime.now()
    batch_name = f"auto_{target.get('name', 'task')}_{now:%Y%m%d_%H%M}"
    zc = _runtime_settings.get("zip_code", config.DEFAULT_ZIP_CODE)
    ns = target.get("needs_screenshot", False)
    batch_id = await db.create_batch(batch_name, ns)
    await db.create_tasks(batch_id, asin_list, zc, ns)

    target["last_run_date"] = now.strftime("%Y-%m-%d")
    _save_schedules(schedules)
    logger.info(f"手动执行定时任务: {batch_name}, {len(asin_list)} ASINs")

    return {"ok": True, "batch_id": batch_id, "batch_name": batch_name, "asin_count": len(asin_list)}


# ==================== 兼容旧 schedule API（settings.html 旧 UI 调用） ====================

@app.get("/api/auto-scrape/schedules")
async def api_legacy_list_schedules():
    return {"schedules": _get_schedules()}


@app.post("/api/auto-scrape/schedules")
async def api_legacy_add_schedule(request: Request):
    """旧式简单定时（无文件，使用全库 ASIN）"""
    body = await request.json()
    time_str = body.get("time", "")
    try:
        h, m = map(int, time_str.split(":"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
    except ValueError:
        raise HTTPException(400, "时间格式错误")

    import uuid
    sched = {
        "id": f"sched_{uuid.uuid4().hex[:8]}",
        "name": f"全库采集-{time_str}",
        "time": time_str,
        "interval_days": 1,
        "source_file": "",  # 空=全库 ASIN
        "asin_count": 0,
        "needs_screenshot": False,
        "enabled": True,
        "last_run_date": "",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    schedules = _get_schedules()
    schedules.append(sched)
    _save_schedules(schedules)
    return {"ok": True, "schedules": schedules}


@app.put("/api/auto-scrape/schedules/{index}")
async def api_legacy_toggle_schedule(index: int, request: Request):
    body = await request.json()
    schedules = _get_schedules()
    if 0 <= index < len(schedules):
        if "enabled" in body:
            schedules[index]["enabled"] = bool(body["enabled"])
        _save_schedules(schedules)
    return {"ok": True, "schedules": schedules}


@app.delete("/api/auto-scrape/schedules/{index}")
async def api_legacy_delete_schedule(index: int):
    schedules = _get_schedules()
    if 0 <= index < len(schedules):
        removed = schedules.pop(index)
        sf = removed.get("source_file", "")
        if sf and os.path.isfile(sf):
            os.remove(sf)
        _save_schedules(schedules)
    return {"ok": True, "schedules": schedules}


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
