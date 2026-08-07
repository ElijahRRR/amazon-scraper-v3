"""common/core/timeutil.py —— DB 时间戳的唯一真源。

只依赖标准库 ``datetime``。server / worker / 两个 DB 后端都能 import。

==========================================================================
``'%Y-%m-%d %H:%M:%S'`` 是**契约**，不是风格选择 —— 不许改成 RFC3339
==========================================================================
交叉引用 ``common/pgdb/schema.py``（文件头「类型决策（已定，不要推翻）」第一条
与 ``TS_DEFAULT``）。那边把整套时间戳列定成 ``text``、值形如
``'2026-08-04 20:25:57'``，PG 侧的列默认值是

    TS_DEFAULT = "to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"

—— 那是**同一个格式的 SQL 方言写法**。本模块的 ``TS_FMT`` 与它必须永远一致：
写入路径（Python）和列默认值（SQL）落进的是同一列，格式一分叉，同一张表里
就会混着两种形状的字符串，而所有比较都是**字符串比较**。

改成 RFC3339（``2026-08-04T20:25:57Z``）会同时踩四个坑：

1. **消费侧切片解析**。``server/app.py`` 对 ``created_at`` / ``completed_at``
   做 ``datetime.strptime(x[:19], '%Y-%m-%d %H:%M:%S')``（批次耗时计算、
   ``/api/batches`` 详情）。``T`` 会让 strptime 抛 ValueError，耗时整列变空。
2. **字符串比较即时间比较**。``updated_at < cutoff`` / ``callback_next_retry_at
   <= now`` / ``ba.asin`` keyset 游标之外的那批回收判定，全是 ``text`` 列上的
   字典序比较。当前格式定长 19 且字典序 == 时间序；混进带 ``T``/``Z`` 的值，
   ``'2026-08-04T...' > '2026-08-04 ...'``（``T`` = 0x54 > 空格 = 0x20），
   超时回收会**静默漏掉**一整批行。
3. **PG 列默认值**。schema.py 的 ``TS_DEFAULT`` 是 DDL 里的字符串，改 Python
   这边不会连带改它；已有库的默认值也不会被重建。
4. **已交付的导出内容**。这两列直接进 CSV/XLSX 导出与 erpAPI 响应。

⚠ 与 ``worker/parser.py`` 的 ``_CRAWL_TIME_FMT``（``'%Y-%m-%dT%H:%M:%SZ'``）
**没有关系**，别把两者对齐。那是 P4-7 有用户明确确认的**采集时刻**线格式，
走的是 ``crawl_time`` 字段，不是 DB 的 created_at/updated_at 列。

==========================================================================
⚠ ``now_ts()`` 不是 ``server/app.py:_cn_now()``，两者永远不该互换
==========================================================================
``_cn_now()`` = ``datetime.utcnow() + timedelta(hours=8)``，返回**东八区**的
naive datetime，只用于**批次名 / 导出文件名 / 定时任务调度判定**（与前端展示
口径一致）。它**不落库**。本模块只管落库的那一份，永远是 UTC。
把 ``_cn_now()`` 换成 ``now_ts()`` 会让批次名和定时任务时间点整体偏 8 小时；
反过来把 ``now_ts()`` 换成 ``_cn_now()`` 会往 UTC 列里灌 UTC+8，正是本次
要修掉的那类错位。所以 ``_cn_now()`` **原样留在 app.py**，本次只在它的
docstring 上加了一句指回这里。

==========================================================================
Phase 4.3 有意的行为改动（已声明）
==========================================================================
下面三处原先是 ``datetime.now().strftime(...)``——**无时区的本机墙上时间**：

  * ``server/api/worker_queue.py``  旧格式 release 分支的 ``tasks.updated_at``
  * ``server/app.py``  ``POST /api/schedules`` 的 ``created_at``
  * ``server/app.py``  自动巡检计划的 ``created_at``

它们一并改成 ``now_ts()``（UTC）。
**UTC 主机上输出逐字节相同**（``datetime.now()`` == ``utcnow()``），所以黄金
基线不该红；在 UTC+8 的开发/生产机上，第一处原先会往 UTC 列里写 UTC+8 的值，
和相邻行差 8 小时——这次修掉。

其余 33 处是 ``datetime.utcnow()``，本来就是 UTC，**任何主机上都逐字节不变**
（28 处 → ``now_ts()``；5 处带 ``timedelta`` 的 → ``ts_from(...)``）。
"""
from datetime import datetime, timezone

#: DB 时间戳的线格式。与 ``common/pgdb/schema.py:TS_DEFAULT`` 的
#: ``'YYYY-MM-DD HH24:MI:SS'`` 是同一个格式。**改这里等于改契约**，见上方 docstring。
TS_FMT = '%Y-%m-%d %H:%M:%S'


def utc_now() -> datetime:
    """当前 UTC 时刻，**带时区**（aware）。

    取代散落各处的 ``datetime.utcnow()``（返回 naive，Python 3.12 起 deprecated）。
    ``strftime(TS_FMT)`` 的输出与 ``utcnow().strftime(TS_FMT)`` 逐字节相同 ——
    该格式不含 ``%z``/``%Z``，时区信息不进字符串。
    """
    return datetime.now(timezone.utc)


def now_ts() -> str:
    """当前 UTC 时刻的 DB 时间戳字符串，如 ``'2026-08-04 20:25:57'``。"""
    return datetime.now(timezone.utc).strftime(TS_FMT)


def ts_from(dt: datetime) -> str:
    """把一个 datetime 格式化成 DB 时间戳字符串。

    给带 ``timedelta`` 的那一组用（``ts_from(utc_now() - timedelta(minutes=n))``）。
    naive / aware 都接受：``TS_FMT`` 不含时区字段，格式化时 tzinfo 被忽略 ——
    也就是说**本函数不做任何时区转换**，传进来是什么钟点就写什么钟点。
    调用方有责任传 UTC（用 ``utc_now()``，别用 ``datetime.now()``）。
    """
    return dt.strftime(TS_FMT)



# ==========================================================================
# RFC 3339 —— **另一条线**，别和上面的 TS_FMT 混
# ==========================================================================
# 上面的 now_ts/ts_from 是**落库**格式（text 列，'%Y-%m-%d %H:%M:%S'）。
# 下面的 iso_utc/now_iso_utc 是 **HTTP 响应体**格式（catalog_sync 事件流与
# 保留期接口的 JSON），带 T 和 Z。两条线各有各的消费者，**永远不要互相"统一"**：
# 把库里的 created_at 换成 RFC3339 会踩上面 docstring 列的四个坑；把响应体里的
# recorded_at 换成 '%Y-%m-%d %H:%M:%S' 会丢掉时区标记和小数秒（契约 v1 靠它们）。
#
# Phase 4.4 收的是这两处**逐字节相同**的 `_iso`（含 `.replace("+00:00","Z")`）：
#   * server/api/sync.py:_iso
#   * common/pgdb/retention.py:_iso   ← 已删除，改为从这里 import
# ⚠ 方向很重要：让 common/pgdb/retention.py 去 import server.api.sync 是**分层
#   倒置**（common 是底座，不该依赖 server）。所以真源落在 common/core，
#   两边都往下 import。
#
# ⚠ **不收进来的三处**，都不是重复：
#   1. server/api/export_incremental.py:_iso_seconds —— 建在 _iso **之上**，
#      再用正则截掉小数秒。契约 v1 明写 `scraped_at` 精确到秒，那是额外一层
#      语义，不是抄的一份。它继续调 `_sync._iso`（现在等于 iso_utc）。
#   2. tools/smoke_local.py:152 的 `time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())`
#   3. .agent/monitor/batch_runtime_monitor.py:42 的 `utc_now()`
#      （`.replace(microsecond=0)` 之后才 isoformat —— 与 iso_utc **不等价**）
#      这两个脚本**刻意不引任何仓库依赖**（只用标准库 + httpx），要能在没装好
#      venv、甚至没 checkout 全仓的机器上单文件跑起来。给它们加一条
#      `from common.core.timeutil import ...` 就毁掉了这个属性。不要动。
#      注意 3 里那个函数也叫 `utc_now`，但**返回字符串**，与本模块返回
#      datetime 的 `utc_now()` 不是一回事，别照着名字互相替换。


def iso_utc(dt):
    """RFC 3339 UTC，永远带 ``Z``。小数秒**可能**出现（库里就有）。

    naive datetime 按 UTC 解释（``replace(tzinfo=utc)``，不做换算）；
    ``None`` 原样返回 ``None``。行为与 Phase 4.4 之前 sync.py / retention.py
    那两份 ``_iso`` 逐字节一致。
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def now_iso_utc() -> str:
    """当前时刻的 RFC 3339 UTC 字符串（即原 ``sync.py:_now_iso``）。"""
    return iso_utc(datetime.now(timezone.utc))


__all__ = ["TS_FMT", "utc_now", "now_ts", "ts_from", "iso_utc", "now_iso_utc"]
