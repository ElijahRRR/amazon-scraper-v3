"""黄金样本夹具：把 server 起在隔离环境里，并把响应规范化到可比较的形态。

用途（PG 迁移 Phase 0）：仓库现有测试只覆盖 3 个纯函数，没有任何 HTTP 层测试。
要把 5000 行存储层从 SQLite 移植到 Postgres 而不改变对外行为，必须先有对照物。
本模块负责「可重复地跑起来」与「把不可重复的部分擦掉」，scenario.py 负责跑什么。

设计原则：**scrub 越少越好**。每擦掉一个字段，就少一份发现移植 bug 的机会。
所以只擦真正不可重复的东西（时钟、epoch、临时路径、计时数字），其余逐字节比较。
"""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional

# ---------------------------------------------------------------- 规范化

# "2026-08-04 09:11:07" / "2026-08-04T09:11:07" / 带小数秒 / 带 Z 或 ±08:00
_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)

# 值随时钟或运行时长变化的键。这些字段的**存在与类型**仍会被比较，只是值被替换。
_VOLATILE_KEYS = {
    "first_seen", "last_seen", "last_seen_ago", "uptime",
    "duration_seconds", "elapsed", "elapsed_seconds", "eta", "eta_seconds",
    "speed", "speed_per_min", "server_time", "now",
    "created_at", "updated_at", "completed_at", "crawl_time",
    "discovered_at", "baseline_updated_at", "callback_sent_at",
    "callback_next_retry_at", "started_at", "ended_at", "ts",
}

# 这些端点的响应里遍地是计时数字，逐值比较没有意义；但**结构**必须保持不变，
# 所以把所有数字叶子换成 <NUM>，仍能捕获字段增删与类型变化。
_NUMERIC_BLIND_PATHS = ("/api/diagnostic", "/api/_debug/lock-stats")


def _scrub_scalar(v: Any) -> Any:
    if isinstance(v, str):
        return _TS_RE.sub("<TS>", v)
    return v


def scrub(obj: Any, *, numeric_blind: bool = False, tmp_root: Optional[str] = None) -> Any:
    """递归规范化。numeric_blind=True 时把所有 int/float 换成 <NUM>。"""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in _VOLATILE_KEYS and not isinstance(v, (dict, list)):
                out[k] = "<VOLATILE>" if v is not None else None
            else:
                out[k] = scrub(v, numeric_blind=numeric_blind, tmp_root=tmp_root)
        return out
    if isinstance(obj, list):
        return [scrub(v, numeric_blind=numeric_blind, tmp_root=tmp_root) for v in obj]
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, float)) and numeric_blind:
        return "<NUM>"
    if isinstance(obj, str):
        s = obj
        if tmp_root and tmp_root in s:
            s = s.replace(tmp_root, "<TMP>")
        return _scrub_scalar(s)
    return obj


def decode_body(resp, path: str, tmp_root: Optional[str]) -> Any:
    """把响应体变成可比较的结构。

    - JSON        → 递归 scrub
    - xlsx        → 解析成 {sheet, header, rows}（zip 字节含时间戳，不可逐字节比）
    - zip         → 成员名列表（截图包）
    - text/csv    → 按行的文本
    - HTML        → 只留长度量级与是否含错误标记（模板含大量时间，逐字节无意义）
    """
    ctype = resp.headers.get("content-type", "")
    raw = resp.content
    numeric_blind = any(path.startswith(p) for p in _NUMERIC_BLIND_PATHS)

    if "application/json" in ctype:
        try:
            return scrub(json.loads(raw or b"null"),
                         numeric_blind=numeric_blind, tmp_root=tmp_root)
        except json.JSONDecodeError:
            return {"__undecodable_json__": raw[:200].decode("utf-8", "replace")}

    if "spreadsheetml" in ctype or path.endswith(".xlsx"):
        return _decode_xlsx(raw)

    if "zip" in ctype:
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                return {"__zip_members__": sorted(zf.namelist())}
        except zipfile.BadZipFile:
            return {"__bad_zip__": len(raw)}

    if "text/csv" in ctype or "text/plain" in ctype:
        text = raw.decode("utf-8-sig", "replace")
        if tmp_root:
            text = text.replace(tmp_root, "<TMP>")
        return {"__csv_lines__": [_TS_RE.sub("<TS>", ln) for ln in text.splitlines()]}

    if "text/html" in ctype:
        text = raw.decode("utf-8", "replace")
        return {
            "__html__": {
                # 模板渲染含大量时间与随机顺序，逐字节比较只会制造噪声。
                # 但「页面是否还渲染得出来、是否掉进错误分支」必须被捕获。
                "has_traceback": "Traceback" in text,
                "has_jinja_error": "jinja2" in text.lower() and "error" in text.lower(),
                "size_bucket": len(text) // 1000,
            }
        }

    return {"__bytes_len__": len(raw)}


def _decode_xlsx(raw: bytes) -> Any:
    import openpyxl
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    except Exception as e:  # noqa: BLE001 - 记录失败本身就是有效信号
        return {"__bad_xlsx__": f"{type(e).__name__}: {e}"}
    try:
        ws = wb.active
        rows = [[_scrub_scalar(c) for c in row]
                for row in ws.iter_rows(values_only=True)]
        return {
            "__xlsx__": {
                "sheet": ws.title,
                "header": rows[0] if rows else [],
                "rows": rows[1:],
            }
        }
    finally:
        wb.close()


# ---------------------------------------------------------------- 起服务

# lifespan 会拉起 4 个后台协程 + WAL 维护 + startup optimize。它们都会在录制期间
# 改状态（回收超时任务、自动重试、标记批次完成、发回调），使样本不可重复。
# 这里全部换成 no-op，把状态推进权完全交给 scenario 的显式请求。
_PATCHED_LOOPS = (
    "_timeout_task_loop",
    "_auto_scrape_scheduler",
    "_completion_watcher",
    "_callback_dispatcher",
    # Phase 2 事件流 relay。它会持续把 scraper.scrape_outbox 抽干进
    # scraper.scrape_events，那两张表在基线里不可见，但后台任务本身会持有
    # 一条连接、每秒开一次事务——录制期必须像其余四个循环一样静音。
    # 事件流的**建表**不在这里，在 connect() 期（common/pgdb/schema.py 的
    # init_tables 末尾），所以 no-op 掉这个循环不会让写钩子撞上缺表。
    "_scrape_event_relay",
)


# `_default_settings()` 的取值大多是 config.py 里的字面量常量，但有两项读环境变量
# （经 `.env` 或 shell）。它们会原样出现在 `/api/settings`、`/api/worker/sync`、
# `/api/settings/reset` 的响应里，于是**基线跟着跑测试的机器变**：开发机 `.env` 里配了
# 代理，同一份录制就报 4 处差异；CI 里没配，就绿。这不是移植 bug，是夹具漏了一处隔离。
#
# 值取 config.py 声明的默认值（环境变量不存在时的取值），所以钉住之后重放出来的
# 就是基线录制时的形态。**不是 scrub**——scrub 会连带屏蔽字段本身的变化，钉住则
# 保留逐值比较，只是把机器差异消掉。
#
# 新增环境变量驱动的设置项时这里要同步加：`tests/test_golden_env_isolation.py`
# 会 AST 扫描 config.py 与 `_default_settings()`，漏了直接失败。
_ENV_DERIVED_SETTINGS = {
    "PROXY_URL": "",
    "DEFAULT_ZIP_CODE": "10001",
}


def _pg_scratch_db():
    """DB_BACKEND=postgres 时：建一个本次运行专用的库，返回 (dsn, drop_fn)。

    黄金场景把 **自增 id 的烧号**钉死在基线里（golden_batch_b 是 id 3 不是 2，
    task id 是 1,3,7,8），所以每次重放都必须从全新的序列开始 —— SQLite 侧靠
    临时 DB_PATH 拿到这份隔离，PG 侧只能靠"一次运行一个库"。
    """
    import asyncio
    import uuid
    import asyncpg
    from urllib.parse import urlsplit, urlunsplit

    from common import config

    base = os.environ.get("PG_DSN") or config.PG_DSN
    parts = urlsplit(base)
    # pid 在容器里会被回收，而下面第一句就是 DROP ... WITH (FORCE)：
    # 撞名等于把另一次运行的库当场强拆。加一段随机后缀彻底消掉这个竞争。
    name = f"scraper_golden_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    admin_dsn = urlunsplit(parts._replace(path="/postgres"))
    scratch_dsn = urlunsplit(parts._replace(path="/" + name))

    async def _create():
        conn = await asyncpg.connect(admin_dsn)
        try:
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            # LC_COLLATE=C：TEXT 的字节序 = SQLite 的 BINARY 排序。
            # 建表时每列还额外带 COLLATE "C"，双保险。
            await conn.execute(
                f'CREATE DATABASE "{name}" TEMPLATE template0 '
                f"LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8'")
        finally:
            await conn.close()

    async def _drop():
        conn = await asyncpg.connect(admin_dsn)
        try:
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        finally:
            await conn.close()

    asyncio.run(_create())
    return scratch_dsn, lambda: asyncio.run(_drop())


@contextmanager
def isolated_server():
    """产出 (client, ctx)。ctx.tmp_root 是被 scrub 成 <TMP> 的临时根目录。"""
    from starlette.testclient import TestClient

    import common.config as config
    import common.database as database  # noqa: F401  （restore 表里要用）
    import server.app as srv
    from common.dbfactory import get_database_class, is_postgres

    # 打补丁的目标必须是 DB_BACKEND 实际选中的那个 Database 类：
    # 补到 SQLite 类上、跑的却是 PG 类，那两个后台任务就没被 no-op 掉，
    # 样本会因为回收/重试/回调而不可重复。
    db_cls = get_database_class()

    tmp_root = tempfile.mkdtemp(prefix="golden_")
    saved: Dict[str, Any] = {}
    pg_drop = None
    pg_env_before = os.environ.get("PG_DSN")

    async def _noop():
        return None

    async def _noop_optimize(self):
        return None

    def _noop_maintenance(self, checkpoint_interval: int = 120):
        return None

    try:
        for name, value in (
            ("DB_PATH", os.path.join(tmp_root, "data", "scraper.db")),
            ("EXPORT_DIR", os.path.join(tmp_root, "data", "exports")),
            ("SCREENSHOT_DIR", os.path.join(tmp_root, "screenshots")),
        ):
            saved[f"config.{name}"] = getattr(config, name)
            setattr(config, name, value)

        # 必须在 TestClient 进入 lifespan（其中调用 _load_settings）之前钉住
        for name, value in _ENV_DERIVED_SETTINGS.items():
            saved[f"config.{name}"] = getattr(config, name)
            setattr(config, name, value)

        for name, value in (
            ("_SETTINGS_FILE", os.path.join(tmp_root, "runtime_settings.json")),
            ("_SCHEDULES_DIR", os.path.join(tmp_root, "data", "schedules")),
        ):
            saved[f"srv.{name}"] = getattr(srv, name)
            setattr(srv, name, value)

        for name in _PATCHED_LOOPS:
            saved[f"srv.{name}"] = getattr(srv, name)
            setattr(srv, name, _noop)

        if is_postgres():
            saved["config.PG_DSN"] = config.PG_DSN
            dsn, pg_drop = _pg_scratch_db()
            config.PG_DSN = dsn
            os.environ["PG_DSN"] = dsn

        saved["db.start_maintenance"] = db_cls.start_maintenance
        saved["db.run_startup_optimize"] = db_cls.run_startup_optimize
        db_cls.start_maintenance = _noop_maintenance
        db_cls.run_startup_optimize = _noop_optimize

        # 进程级注册表在同进程内跨用例残留，会让样本依赖执行顺序
        srv._worker_registry.clear()
        srv._global_coordinator.clear()
        srv._worker_restart_flags.clear()
        srv._completion_check_set.clear()
        srv._settings_version = 0

        os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)

        with TestClient(srv.app) as client:
            yield client, _Ctx(tmp_root=tmp_root)
    finally:
        for key, value in saved.items():
            mod, _, attr = key.partition(".")
            target = {"config": config, "srv": srv, "db": db_cls}[mod]
            setattr(target, attr, value)
        if pg_drop is not None:
            if pg_env_before is None:
                os.environ.pop("PG_DSN", None)
            else:
                os.environ["PG_DSN"] = pg_env_before
            pg_drop()
        shutil.rmtree(tmp_root, ignore_errors=True)


class _Ctx:
    def __init__(self, tmp_root: str):
        self.tmp_root = tmp_root


# ---------------------------------------------------------------- 录制

class Recorder:
    """按顺序累积 (name, request, response) 三元组。

    strict=True（录制 / 自检）：`expect` 不符立即抛异常。此时 `expect` 的作用是
        守护场景脚本本身的正确性——录进一个错误的基线比没有基线更糟。
    strict=False（校验）：`expect` 不符只记录，继续跑完。移植期间要的是一次看到
        全部差异，撞到第一处就停会把后面的问题藏起来，逼人一轮只修一个。
    """

    def __init__(self, client, tmp_root: str, strict: bool = True):
        self.client = client
        self.tmp_root = tmp_root
        self.strict = strict
        self.steps: List[Dict[str, Any]] = []

    def call(self, name: str, method: str, path: str, *,
             expect: Optional[int] = None, **kw) -> Any:
        """发一次请求并记录。返回解析后的 JSON（便于 scenario 串联状态）。"""
        resp = self.client.request(method, path, **kw)
        body = decode_body(resp, path, self.tmp_root)

        self.steps.append({
            "name": name,
            "request": {
                "method": method,
                "path": path,
                # 只记会影响响应的入参；文件字节另行摘要
                "params": kw.get("params"),
                "json": kw.get("json"),
                "data": kw.get("data"),
                "files": sorted(kw["files"].keys()) if kw.get("files") else None,
            },
            "response": {
                "status": resp.status_code,
                "content_type": resp.headers.get("content-type", "").split(";")[0],
                "body": body,
            },
        })

        if expect is not None and resp.status_code != expect:
            msg = (f"步骤 {name!r} 期望 HTTP {expect}，实际 {resp.status_code}: "
                   f"{resp.content[:300]!r}")
            if self.strict:
                raise AssertionError(msg)
            # 非 strict：把偏差记进样本，让它以 status 差异的形式出现在 diff 里
            self.steps[-1]["response"]["unexpected_status"] = expect

        try:
            return resp.json()
        except Exception:  # noqa: BLE001
            return None


# ---------------------------------------------------------------- 比较

def diff_steps(golden: List[Dict], actual: List[Dict], limit: int = 40) -> List[str]:
    """返回人类可读的差异行。空列表 = 完全一致。"""
    out: List[str] = []

    gnames = [s["name"] for s in golden]
    anames = [s["name"] for s in actual]
    if gnames != anames:
        out.append(f"步骤序列不一致:\n  golden={gnames}\n  actual={anames}")
        return out

    for g, a in zip(golden, actual):
        name = g["name"]
        if g["response"]["status"] != a["response"]["status"]:
            extra = ""
            if "unexpected_status" in a["response"]:
                # 该步骤同时违反了场景自己的断言——通常意味着行为真的变了，
                # 而不是基线录歪了
                extra = f"（场景断言的是 {a['response']['unexpected_status']}）"
            out.append(
                f"[{name}] status: {g['response']['status']} "
                f"-> {a['response']['status']}{extra}")
        if g["response"]["content_type"] != a["response"]["content_type"]:
            out.append(
                f"[{name}] content-type: {g['response']['content_type']} "
                f"-> {a['response']['content_type']}")
        out.extend(_diff_body(name, g["response"]["body"], a["response"]["body"]))
        if len(out) >= limit:
            out.append(f"... 差异过多，已截断（上限 {limit} 条）")
            break
    return out


def _numeric_but_not_bool(v: Any) -> bool:
    """int/float 之间的宽容**不能**把 bool 也捎带进来。

    bool 是 int 的子类，所以原来的 ``isinstance(g, (int, float))`` 对 True/False
    同样成立；再加上叶子比较用的是 ``g != a``（``1 == True``），基线里 146 个取值
    为 0/1 的 int 叶子和 97 个真 bool 叶子全都可以静默互换。也就是说
    "INTEGER 0/1 变成 BOOLEAN true/false" 这一整类类型漂移（D-1 契约的核心风险，
    asyncpg 会把 BOOLEAN 列直接给成 Python bool）黄金样本一步都拦不住。

    int↔float 的宽容保留：本场景里 ``success_rate`` / ``completion_rate`` 这类
    字段确实会随数据在 int 与 float 之间摆动。
    """
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _diff_body(name: str, g: Any, a: Any, path: str = "") -> List[str]:
    where = f"[{name}]{path}"
    if type(g) is not type(a) and not (_numeric_but_not_bool(g) and _numeric_but_not_bool(a)):
        return [f"{where} 类型: {type(g).__name__} -> {type(a).__name__}"]

    if isinstance(g, dict):
        out = []
        for k in sorted(set(g) | set(a)):
            if k not in a:
                out.append(f"{where}.{k} 字段消失（值曾为 {g[k]!r}）")
            elif k not in g:
                out.append(f"{where}.{k} 新增字段（值 {a[k]!r}）")
            else:
                out.extend(_diff_body(name, g[k], a[k], f"{path}.{k}"))
        return out

    if isinstance(g, list):
        if len(g) != len(a):
            return [f"{where} 列表长度: {len(g)} -> {len(a)}"]
        out = []
        for i, (gi, ai) in enumerate(zip(g, a)):
            out.extend(_diff_body(name, gi, ai, f"{path}[{i}]"))
        return out

    if g != a:
        return [f"{where}: {g!r} -> {a!r}"]
    return []
