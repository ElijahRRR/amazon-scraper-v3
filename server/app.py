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


def _default_settings() -> dict:
    return {
        "zip_code": config.DEFAULT_ZIP_CODE,
        "max_retries": config.MAX_RETRIES,
        "request_timeout": config.REQUEST_TIMEOUT,
        "proxy_api_url": config.PROXY_API_URL_AUTH,
        "token_bucket_rate": config.TOKEN_BUCKET_RATE,
        "per_channel_qps": config.PER_CHANNEL_QPS,
        "max_concurrency": config.MAX_CONCURRENCY,
        "tunnel_max_concurrency": config.TUNNEL_MAX_CONCURRENCY,
        "global_max_concurrency": config.GLOBAL_MAX_CONCURRENCY,
        "global_max_qps": config.GLOBAL_MAX_QPS,
        "proxy_mode": config.PROXY_MODE,
        "tunnel_channels": config.TUNNEL_CHANNELS,
        "tunnel_proxy_url": config.TUNNEL_PROXY_URL,
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
    for wid in active:
        weight = scores[wid] / total_score if total_score > 0 else 1.0 / len(active)
        active[wid]["quota"] = {
            "max_concurrency": max(config.MIN_CONCURRENCY, int(max_conc * weight)),
            "max_qps": max(1.0, max_qps * weight),
        }


# ==================== HTML 页面 ====================

@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/tasks", response_class=HTMLResponse)
async def page_tasks(request: Request):
    return templates.TemplateResponse("tasks.html", {"request": request})


@app.get("/results", response_class=HTMLResponse)
async def page_results(request: Request):
    return templates.TemplateResponse("results.html", {"request": request})


@app.get("/workers", response_class=HTMLResponse)
async def page_workers(request: Request):
    return templates.TemplateResponse("workers.html", {"request": request})


@app.get("/settings", response_class=HTMLResponse)
async def page_settings(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})


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


@app.post("/api/batches/{batch_id}/prioritize")
async def api_prioritize(batch_id: int):
    await db.prioritize_batch(batch_id)
    return {"ok": True}


# ==================== API: Worker 任务拉取和提交 ====================

@app.get("/api/tasks/pull")
async def api_pull_tasks(request: Request,
                         worker_id: str = Query(...),
                         count: int = Query(10),
                         needs_screenshot: Optional[bool] = Query(None)):
    ip = request.client.host if request.client else None
    _register_worker(worker_id, ip=ip)
    if worker_id in _worker_registry:
        _worker_registry[worker_id]["tasks_pulled"] += count

    ns = None
    if needs_screenshot is not None:
        ns = needs_screenshot
    elif worker_id in _worker_registry:
        if not _worker_registry[worker_id].get("enable_screenshot", True):
            ns = False

    tasks = await db.pull_tasks(worker_id, count, ns)
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
    await db.update_screenshot_status(asin, batch_id, "done", file_path=rel_path)
    return {"ok": True, "path": rel_path}


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

    return {
        "settings": _runtime_settings,
        "settings_version": _settings_version,
        "quota": quota,
    }


@app.get("/api/workers")
async def api_workers():
    now = time.time()
    workers = []
    for wid, w in _worker_registry.items():
        workers.append({
            **w,
            "online": (now - w["last_seen"]) < 60,
            "last_seen_ago": int(now - w["last_seen"]),
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
    return {"ok": True, "version": _settings_version}


# ==================== API: 导出 ====================

@app.get("/api/export/{batch_name}")
async def api_export_batch(batch_name: str, format: str = "xlsx"):
    batch = await db.get_batch_by_name(batch_name)
    if not batch:
        raise HTTPException(404, f"批次不存在: {batch_name}")

    batch_id = batch["id"]

    if format == "csv":
        return await _export_csv(batch_id, batch_name)
    else:
        return await _export_xlsx(batch_id, batch_name)


@app.get("/api/export/all")
async def api_export_all(format: str = "xlsx"):
    if format == "csv":
        return await _export_csv(None, "all")
    else:
        return await _export_xlsx(None, "all")


async def _export_xlsx(batch_id: int = None, name: str = "export"):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Results"

    # 写表头
    headers = config.EXPORT_COLUMN_ORDER
    ws.append(headers)

    # 流式写入数据
    reverse_map = {v: k for k, v in config.HEADER_MAP.items()}
    row_count = 0
    async for item in db.iter_results(batch_id):
        row = []
        for h in headers:
            field = reverse_map.get(h, h)
            val = item.get(field, "")
            # 计算总价
            if h == "总价":
                bp = item.get("buybox_price", "")
                bs = item.get("buybox_shipping", "")
                try:
                    from common.database import _parse_price_float
                    bp_f = _parse_price_float(bp) or 0
                    bs_f = _parse_price_float(bs) or 0
                    val = f"${bp_f + bs_f:.2f}" if bp_f > 0 else ""
                except Exception:
                    val = ""
            row.append(str(val) if val else "")
        ws.append(row)
        row_count += 1

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


async def _export_csv(batch_id: int = None, name: str = "export"):
    output = io.StringIO()
    writer = csv.writer(output)

    headers = config.EXPORT_COLUMN_ORDER
    writer.writerow(headers)

    reverse_map = {v: k for k, v in config.HEADER_MAP.items()}
    async for item in db.iter_results(batch_id):
        row = []
        for h in headers:
            field = reverse_map.get(h, h)
            val = item.get(field, "")
            row.append(str(val) if val else "")
        writer.writerow(row)

    output.seek(0)
    filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
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
