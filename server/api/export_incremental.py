"""catalog_sync 增量导出端点 —— 契约 v1 的服务端实现。

    GET /api/export/incremental?cursor=<int>&limit=<≤1000>
    -> {"records": [...], "next_cursor": <int>, "has_more": <bool>}

规格来源：沃尔玛侧 `docs/scraper_migration_brief.md` §5（本仓库存有一份副本：
`docs/incremental_export_contract.md`）。**契约双方各存一份，改动需两侧同步升版本号。**

本文件是一层**适配器**，不是新的存储。数据来自 Phase 2 建的事件流
（`scraper.scrape_events`），与 `server/api/sync.py` 读同一张表、同一套快照纪律。
两者的关系：本端点是**对外契约**；`/api/v1/sync/*` 是运维面（status/counts/ack），
沃尔玛侧不需要实现它们也能正常消费。

------------------------------------------------------------------------
承重约束
------------------------------------------------------------------------

1. **路由注册顺序是承重的，而且这条依赖是隐式的。**
   `server/app.py:2052` 有 `@app.get("/api/export/{batch_name}")`，对不认识的
   名字回 **404**。实测（未挂本端点时）：

       GET /api/export/incremental -> 404 {"detail":"批次不存在: incremental"}

   Starlette 按**注册顺序**匹配，所以本 router 必须在那条 catch-all **之前**
   include。目前 `server/app.py:260` 的 include_router 满足这个条件，
   `/api/export/fields`(2033) 与 `/api/export/all`(2042) 是同前缀下静态路径的
   现成先例。

   **为什么必须有回归守卫**：把 include_router 挪到文件末尾、或者把本端点改成
   用 `@app.get` 直接定义在 catch-all 之后，都会让它静默退化成 404 ——
   而 404 正是消费方最容易读成「暂无数据」的码，游标于是永不推进、同步静默停摆，
   两侧都不会报错。
   `tests/test_incremental_export.py::test_route_order_is_load_bearing` 钉死它。

2. **鉴权：`X-Export-Token` 请求头。** 服务器是公网 IP。
   未配置 `EXPORT_TOKEN` 时**拒绝服务**（503），不是放行 —— fail closed。
   比对用 `hmac.compare_digest`。**只加在本 router 上，不加全局中间件**：
   本服务今天一处鉴权都没有，全局中间件会当场打死所有 worker 与 erpAPI。

3. **永不用 404 表达「没有数据」。** 空就是 `200 + records: [] + has_more: false`。
   这与同前缀下 `/api/export/{batch_name}` 的既有行为**故意相反**，理由见第 1 条。

4. **一个快照。** MIN/MAX/页查询同一个 `REPEATABLE READ READ ONLY` 事务，
   否则保留期可以卡在中间跑完，让消费者拿到一个「跳过了被裁区间」的 200。
"""
from __future__ import annotations

import hmac
import logging
import os
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, Query
from fastapi.responses import JSONResponse

from server.api import sync as _sync

logger = logging.getLogger(__name__)

#: 契约版本。改任何字段语义都必须升它，并同步沃尔玛侧那份。
CONTRACT_VERSION = 1

MAX_LIMIT = 1000
DEFAULT_LIMIT = 200

#: 沃尔玛侧的 marketplace 值域，当前恒 "US"（对方已在 db_schema / schema.sql /
#: 契约三处同步为默认 'US'，复合主键 (marketplace, asin)）。
#:
#: ⚠ 语义提示（已在交付说明里单列）：**这个 marketplace 是「上架目的地」，
#: 不是「采集来源站点」。** 本仓库内部的 `scrape_events.marketplace` 存的是
#: 采集来源（封闭集 {'amazon.com'}）。今天两者一一对应，开了 Walmart CA 之后
#: 就不再对应了——那时很可能仍然从 amazon.com 采、却要上架到 CA。
#: 所以这里**不做**「把内部值改名」的处理，而是显式映射，并把来源站点原样
#: 放进 scrape_params.source_marketplace，让两个概念从第一天就是两个字段。
DEST_MARKETPLACE = "US"

#: 我们不采集币种。amazon.com 恒为美元。
#: ⚠ 这是本适配器**凭空补出来**的字段，不是采集到的事实。
DEFAULT_CURRENCY = "USD"

#: 采集侧的库存文案 -> 契约枚举。
#: ⚠ 枚举取值需与 §5 核对，见交付说明的假设清单第 4 条。
_STOCK_IN = re.compile(r"in\s*stock|available|有货", re.I)
_STOCK_OUT = re.compile(r"out\s*of\s*stock|unavailable|currently\s+unavailable|无货|缺货", re.I)

_NA = {"", "n/a", "none", "null", "-"}

#: ``include_in_schema=False`` 是硬要求，不是风格。
#: ``/openapi.json`` 是**既有端点**，黄金基线第 5 步逐字节钉死它，而沃尔玛侧
#: 明确要求「既有端点全部不动」。少这一行，黄金门当场红——实测过：
#:   [openapi_schema].paths./api/export/incremental 新增字段（…）
#: 契约文档是给人看的那份（docs/incremental_export_contract.md），不靠 schema 分发。
router = APIRouter(tags=["export-incremental"], include_in_schema=False)


# ============================================================ 取值助手

def _clean(v: Any) -> Optional[str]:
    """占位符归一到 None。采集侧用 "N/A" 表示「本次没取到」。"""
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in _NA else s


def _split_list(v: Any, sep: str = ",") -> List[str]:
    s = _clean(v)
    return [] if s is None else [p.strip() for p in s.split(sep) if p.strip()]


def _price(v: Any) -> Optional[float]:
    """'$1,299.00' / '19.99' -> 1299.0 / 19.99；取不到 -> None。

    绝不返回 0.0 当「取不到」—— 0 是个合法价格，用它当哨兵会让消费侧
    把「没采到」误读成「免费」。
    """
    s = _clean(v)
    if s is None:
        return None
    m = re.search(r"\d[\d,]*\.?\d*", s.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _stock_state(payload: Dict[str, Any]) -> str:
    """-> in_stock | out_of_stock | unknown"""
    s = _clean(payload.get("stock_status"))
    if s is None:
        return "unknown"
    if _STOCK_OUT.search(s):
        return "out_of_stock"
    if _STOCK_IN.search(s):
        return "in_stock"
    return "unknown"


def _category_path(payload: Dict[str, Any]) -> List[str]:
    """'Home > Test > Sub' -> ['Home','Test','Sub']。

    采集侧只有面包屑一个数据源，软降级页会把它整块剥掉，那时这里是 []。
    **[] 表示「本次没采到」，不表示「该商品无类目」** —— 配合 record 里的
    `completeness_ok` 判断，别拿 [] 去覆盖你侧已有的类目。
    """
    raw = _clean(payload.get("category_tree"))
    if raw is None:
        return []
    return [p.strip() for p in re.split(r"\s*>\s*", raw) if p.strip()]


# ============================================================ record 映射

def _to_record(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = row.get("payload") or {}
    if isinstance(payload, str):          # asyncpg 在某些配置下回字符串
        import json
        try:
            payload = json.loads(payload)
        except Exception:                 # noqa: BLE001
            payload = {}

    completeness = row.get("completeness") or 0
    measured = bool(completeness & _sync.COMPLETENESS_MEASURED_BIT)
    complete = measured and (completeness & _sync.COMPLETENESS_REQUIRED_MASK
                             ) == _sync.COMPLETENESS_REQUIRED_MASK

    return {
        # ---- 契约必填 ----
        "source_id": row["source_id"],
        "cursor": row["seq"],
        "marketplace": DEST_MARKETPLACE,
        "asin": row["asin"],
        "scraped_at": _sync._iso(row.get("collected_at")),
        "scrape_params": {
            "zip": row.get("zip_requested"),
            "zip_observed": row.get("zip_observed"),
            "zip_verify": row.get("zip_verify"),
            # 采集来源站点。与顶层 marketplace（上架目的地）是两个概念，见文件头。
            "source_marketplace": row.get("marketplace"),
            "parse_engine": row.get("parse_engine"),
        },
        "slow": {
            "title": _clean(payload.get("title")),
            "brand": _clean(payload.get("brand")),
            "category_path": _category_path(payload),
            "images": _split_list(payload.get("image_urls")),
        },
        "fast": {
            "price": _price(payload.get("current_price")),
            "currency": DEFAULT_CURRENCY,
            "stock_state": _stock_state(payload),
        },
        # ---- 契约「建议带」 ----
        "slow_hash": row.get("slow_hash"),

        # ---- 采集侧附加，契约未要求，收着无害 ----
        # outcome != 'ok' 的记录**只进 snapshots，不要 upsert products**。
        # 这类记录的 slow/fast 基本为空，那是「本次没采到」不是「值变了」。
        "outcome": row.get("outcome"),
        "completeness_ok": complete,
        "review_hash": row.get("review_hash"),
        "hash_ver": row.get("hash_ver"),
        "recorded_at": _sync._iso(row.get("recorded_at")),
    }


# ============================================================ 鉴权

def _check_token(token: Optional[str]) -> Optional[JSONResponse]:
    expected = os.environ.get("EXPORT_TOKEN", "").strip()
    if not expected:
        # fail closed。服务器是公网 IP，"没配就放行" 等于把商品库敞在互联网上。
        return _sync._err(503, "export_token_not_configured",
                          "服务端未配置 EXPORT_TOKEN，增量导出关闭。")
    if not token or not hmac.compare_digest(token, expected):
        return _sync._err(401, "invalid_export_token",
                          "X-Export-Token 缺失或不匹配。")
    return None


# ============================================================ 端点

@router.get("/api/export/incremental")
async def export_incremental(
    cursor: int = Query(0, ge=0, description="独占下界，返回 cursor 大于它的记录；从头拉传 0"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    x_export_token: Optional[str] = Header(None, alias="X-Export-Token"),
):
    denied = _check_token(x_export_token)
    if denied is not None:
        return denied

    unavailable = _sync._unavailable()
    if unavailable is not None:
        return unavailable

    if cursor > _sync.MAX_BIGINT:
        return _sync._err(422, "invalid_parameter",
                          f"cursor 超出 bigint 上限：{cursor}")

    try:
        async with _sync._snapshot() as conn:
            meta = await _sync._read_meta(conn)
            min_raw, max_raw = await _sync._bounds(conn)
            rows = await conn.fetch(
                f"SELECT {_sync._RECORD_SELECT} FROM scraper.scrape_events "
                "WHERE seq > $1 ORDER BY seq LIMIT $2",
                cursor, limit + 1)
            # 快照内复核下界（保守方向）：宁可多报一次 409，也不能漏。
            min_after, _ = await _sync._bounds(conn)
    except _sync._PoolUnavailable:
        return _sync._err(503, "event_stream_unavailable", "连接池尚未就绪。")
    except Exception as exc:                                   # noqa: BLE001
        if _sync._schema_missing(exc):
            return _sync._err(503, "event_stream_unavailable",
                              f"事件流表还没建好: {type(exc).__name__}")
        raise

    min_available, max_seq = _sync._window(
        max(_sync._as_int(min_raw) or 0, _sync._as_int(min_after) or 0),
        max_raw, _sync._as_int(meta.get("max_seq_ever")))

    # 游标掉出保留窗口 —— 契约 v1 没写这一种，但静默跳过是不可接受的。
    # 见交付说明假设清单第 6 条：需要写进 v1.1。
    if cursor + 1 < min_available:
        return _sync._err(
            409, "cursor_below_retention",
            "你要的下一条已被保留期裁掉。请告警并做一次全量对账。",
            cursor=cursor, min_available_cursor=min_available, max_cursor=max_seq)

    has_more = len(rows) > limit
    rows = rows[:limit]
    records = [_to_record(dict(r)) for r in rows]
    # 游标只推进到**真正投递过的那一条**。空页不推进 —— 这是唯一不丢数据的方向。
    next_cursor = records[-1]["cursor"] if records else cursor

    return JSONResponse(content={
        "contract_version": CONTRACT_VERSION,
        "records": records,
        "next_cursor": next_cursor,
        "has_more": has_more,
    })
