"""common/pgdb/results_write.py —— 结果写入路径（系统里最热、也最要命的一段）。

OWNS（6 个方法）:
    _save_result_inner_unlocked  database.py:1816   ← 私有但承重
    accept_success_result        database.py:1655
    accept_results_batch         database.py:1702
    accept_failed_result         database.py:1811   ← 委托 self.fail_task
    save_result                  database.py:1987
    save_results_batch           database.py:2084   ← 当前无外部调用方，仍须存在

依赖别人:
    tasks.py   -> fail_task(...)
    media.py   -> _get_done_screenshot_path(asin, batch_id)

--------------------------------------------------------------------------
一、lease 门：逐字移植，不要"加固"
--------------------------------------------------------------------------
    UPDATE tasks SET status='done', updated_at=?
     WHERE id=? AND worker_id=? AND lease_epoch=? AND status='processing'
    -> rowcount == 0 即 stale

这是全系统安全性最高的一条谓词（README 记录：把它删掉会产生 45 处基线差异，
夹具专门留了一个仍处于 processing 的探针任务做双向断言）。

* rowcount 必须是 **int**。asyncpg 的 execute 返回 ``'UPDATE 0'`` 这种字符串，
  ``'UPDATE 0' == 0`` 为 False，会静默放行所有过期结果。
  ConnProxy 已经帮你把它解析成 int 了；如果你绕开代理直接用 asyncpg，
  必须自己过 ``pool.rowcount_from_tag``。
* **不要**给它加 ``SELECT ... FOR UPDATE``。PG 在 READ COMMITTED 下会阻塞、
  然后用 EvalPlanQual 对新版本重算自己的 WHERE——结果与 SQLite 一致。
* 紧随其后的 server_reject 降级 UPDATE（database.py:1682/1751）**没有** lease 谓词。
  那是对的：上面的 CAS 已经在同一事务里证明了归属。必须留在同一个事务里。

--------------------------------------------------------------------------
二、accept_results_batch：返回值恰好 3 个 int 键
--------------------------------------------------------------------------
    {"accepted": int, "stale": int, "failed": int}
app.py:1580 会 ``return {**result, "total": len(results)}`` —— 多一个键就多泄
一个字段进 HTTP 响应体。

* 五处 ``record_stage(...)`` 必须原地保留，stage 名一字不改：
  ``update_tasks_lease`` / ``save_result`` / ``commit`` / ``total_in_lock``。
  基线 step 56（/api/_debug/lock-stats）钉死了 stage_timings 的这四个 key，
  而 ``_summary`` 对空样本返回的是形状不同的 ``{"count": 0}``。
* 外层必须是 ``async with self._write_lock("accept_results_batch"):``
  —— caller 名同样被基线钉死（waits/holds 的三个 key 之一）。
* 失败分支的 SELECT retry_count 之后的 UPDATE 只有 ``WHERE id=?``，
  既没有 lease 谓词也没有 rowcount 检查——**是 bug，照抄**，只给那条 SELECT
  加 ``FOR UPDATE`` 把读写窗口关上（SQLite 靠全局锁免费拿到的）。
  在源码里留个注释指向 .agent/catalog_sync_audit.md:167。
* item 循环顺序**不要**排序。谁最后写进 asin_data 由循环顺序决定，
  按 task_id 排序会改语义。

--------------------------------------------------------------------------
三、_save_result_inner_unlocked：SELECT → 分支 INSERT / UPDATE
--------------------------------------------------------------------------
两个分支不可互换：INSERT 分支会用当前值播种全部 6 个 baseline_* 列并写一条
change_type='new' 的 asin_changes；UPDATE 分支做变动检测、且只在 is_auto 时
才动 baseline_*。所以**不许**改写成 ``ON CONFLICT DO UPDATE``。

* 保留 ``if not asin: return False`` 的前置早退（database.py:1822）。
* 类型强转（**必做**）：40 多个 TEXT 列的值直接来自 ``await request.json()``。
  SQLite 有 TEXT affinity 会静默转换，asyncpg 直接 DataError。
  每一个绑到 TEXT 列的值都要过 ``common.pgdb.pool.text_affinity``：
  True->'1'、0.1+0.2->'0.3'、1e21->'1.0e+21'。裸 ``str()`` 是**错的**
  （会得到 'True' / '0.30000000000000004' / '1e+21'）。
  作用点：database.py:1909-1912、1921-1932、1948-1951、1963-1967。
  黄金夹具抓不到这一类（自带 worker 和夹具都全 str 化了）。
* 动态列拼接：本文件是唯一会生成"每种非 None 字段组合一份 SQL 文本"的地方。
  pool 已把 statement_cache_size 设成 0，所以不会有预备语句抖动，
  可以放心逐字移植 ``f"{col} = ?"`` 的写法（``?`` 由 translate_sql 统一编号，
  join_params + where_params 的顺序陷阱不存在）。
* 单写连接 + 真写锁的前提下**不需要** ``pg_advisory_xact_lock(hashtext(asin))``。
  多进程部署时需要；已记进 OWNERSHIP.md 的 Phase 1.5 清单。
* 内容 hash 与标题 hash 一律用 _shared 里再导出的函数，禁止本地复制字段表。

--------------------------------------------------------------------------
四、本次移植中与 SQLite 版**逐字一致**的取舍清单（实现者自查用）
--------------------------------------------------------------------------
* ``data["content_hash"] / data["title_bullets_hash"]`` 仍然**原地写回入参 dict**
  （database.py:1827-1828）。accept_results_batch 的调用方会看到被改过的 dict，
  这是现状，照抄。
* ``has_baseline = bl_price is not None`` —— 只看 baseline_price 一列。
  baseline_price 为 NULL 时，即使其余五个 baseline 列都有值也**不做**变动检测。
  这是现状，照抄。
* 变动检测比的是 **baseline_\\* 而非当前值**；``bl_tb_hash and new_tb_hash`` 的
  空串短路也照抄（空 hash 不触发 title_bullets 变动）。
* UPDATE 分支写 asin_changes 时 ``batch_id`` 可能是 NULL；
  INSERT 分支的 'new'/'first_seen' 记录**只在 batch_id 为真时**才写。
* ``val is not None`` 才进动态列 —— 空串 ``""`` 会被写进去，不是跳过。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import List

from common.pgdb._shared import (  # noqa: F401
    ASIN_DATA_FIELDS,
    _ASIN_DATA_COLUMN_SET,
    _compare_price,
    _compare_stock_qty,
    _compare_stock_status,
    _compute_content_hash,
    _compute_title_bullets_hash,
    _fail_cap,
    _is_parse_failure,
    _normalize_screenshot_path,
    record_stage,
)
from common.pgdb.pool import text_affinity  # noqa: F401

logger = logging.getLogger(__name__)


class ResultsWriteMixin:
    """只定义方法，绝不定义 __init__。"""

    # ==================== 结果操作（含变动检测）====================

    async def accept_success_result(self, task_id: int, worker_id: str, lease_epoch: int,
                                    data: dict, batch_id: int = None) -> dict:
        """原子事务：校验 lease → 写数据 → 标 done

        Returns: {"accepted": True, "saved": True} 正常
                 {"accepted": True, "saved": False, "server_reject": True} 解析失败
                 {"accepted": False, "stale": True} lease 不匹配
        """
        # asyncpg 不做隐式参数转换：这些值全部来自未经校验的 request.json()。
        # SQLite 靠列 affinity 把 '1' 当 1 用，PG 会直接 DataError。
        tid = self.as_int(task_id)
        wid = self.text_affinity(worker_id)
        epoch = self.as_int(lease_epoch)

        async with self._write_lock:
            await self._db.execute("BEGIN")
            try:
                # Step 1: 校验 lease（原子 gate）
                cursor = await self._db.execute(
                    "UPDATE tasks SET status='done', updated_at=? "
                    "WHERE id=? AND worker_id=? AND lease_epoch=? AND status='processing'",
                    (datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                     tid, wid, epoch)
                )
                if cursor.rowcount == 0:
                    await self._db.execute("ROLLBACK")
                    return {"accepted": False, "stale": True}

                # Step 2: 解析失败检测
                if _is_parse_failure(data):
                    asin = data.get("asin", "")
                    logger.warning(f"server_reject: {asin} 解析失败数据")
                    # 降级为 failed（不回 pending 重试，Worker 已重试过）
                    await self._db.execute(
                        "UPDATE tasks SET status='failed', error_type='server_reject', "
                        "error_detail='parse_failure_on_server' WHERE id=?",
                        (tid,)
                    )
                    await self._db.execute("COMMIT")
                    return {"accepted": True, "saved": False, "server_reject": True}

                # Step 3: 写入 asin_data（事务内，不拿锁不开新事务）
                saved = await self._save_result_inner_unlocked(data, batch_id)

                await self._db.execute("COMMIT")
                return {"accepted": True, "saved": saved}
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def accept_results_batch(self, items: list) -> dict:
        """单事务批量处理结果（减少锁争用）

        items: [{"task_id", "worker_id", "lease_epoch", "data", "batch_id", "success"}, ...]
        Returns: {"accepted": int, "stale": int, "failed": int}
        """
        accepted = 0
        stale = 0
        failed = 0
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        async with self._write_lock("accept_results_batch"):
            # SQLite 版用 BEGIN IMMEDIATE 显式抢写锁；PG 没有这个概念，
            # 垫片会把它当普通 BEGIN（'BEGIN IMMEDIATE' 在 PG 里是语法错误）。
            _t_total = time.monotonic()
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                # ⚠ 循环顺序不排序：同一 ASIN 多条时"谁最后写进 asin_data"由这里决定。
                for item in items:
                    task_id = item.get("task_id")
                    worker_id = item.get("worker_id", "")
                    lease_epoch = item.get("lease_epoch", 0)
                    batch_id = item.get("batch_id")
                    data = item.get("data", {})
                    is_success = item.get("success", True)

                    # 真值判断用**原始值**（SQLite 版就是这样），只有绑参才强转
                    tid = self.as_int(task_id)
                    wid = self.text_affinity(worker_id)
                    epoch = self.as_int(lease_epoch)

                    if not task_id:
                        # 无 task_id 的直接写入
                        if is_success and data:
                            _t = time.monotonic()
                            saved = await self._save_result_inner_unlocked(data, batch_id)
                            record_stage("save_result", (time.monotonic() - _t) * 1000)
                            if saved:
                                accepted += 1
                        continue

                    if is_success:
                        # 校验 lease
                        _t = time.monotonic()
                        cursor = await self._db.execute(
                            "UPDATE tasks SET status='done', updated_at=? "
                            "WHERE id=? AND worker_id=? AND lease_epoch=? AND status='processing'",
                            (now, tid, wid, epoch)
                        )
                        record_stage("update_tasks_lease", (time.monotonic() - _t) * 1000)
                        if cursor.rowcount == 0:
                            stale += 1
                            continue

                        # 解析失败检测
                        if _is_parse_failure(data):
                            await self._db.execute(
                                "UPDATE tasks SET status='failed', error_type='server_reject', "
                                "error_detail='parse_failure_on_server' WHERE id=?",
                                (tid,)
                            )
                            failed += 1
                            continue

                        # 写入 asin_data
                        _t = time.monotonic()
                        saved = await self._save_result_inner_unlocked(data, batch_id)
                        record_stage("save_result", (time.monotonic() - _t) * 1000)
                        if saved:
                            accepted += 1
                        else:
                            failed += 1
                    else:
                        # 失败结果：校验 lease 后标记失败/重试
                        error_type = data.get("error_type", "")
                        error_detail = data.get("error_detail", "")
                        # FOR UPDATE：SQLite 靠全局写锁免费拿到的"读到写"原子性，
                        # PG 下要显式锁住这一行，否则并发 reclaim 会在 SELECT 与
                        # UPDATE 之间把任务重新入队。
                        # ⚠ 下面两条 UPDATE 只有 WHERE id=?（既无 lease 谓词也无
                        #   rowcount 检查），比 fail_task 更弱——**是已知 bug，照抄**。
                        #   见 .agent/catalog_sync_audit.md:167，修复排在后续阶段。
                        async with self._db.execute(
                            "SELECT retry_count FROM tasks WHERE id=? AND worker_id=? AND lease_epoch=? AND status='processing' FOR UPDATE",
                            (tid, wid, epoch)
                        ) as c:
                            row = await c.fetchone()
                        if not row:
                            stale += 1
                            continue
                        retry_count = row[0] + 1
                        # 按 error_type 决定该任务的失败上限
                        # （variant_offset 不重试，其他用 MAX_RETRIES）
                        cap = _fail_cap(error_type)
                        if retry_count >= cap:
                            await self._db.execute(
                                "UPDATE tasks SET status='failed', retry_count=?, error_type=?, error_detail=?, updated_at=? "
                                "WHERE id=?",
                                (retry_count, self.text_affinity(error_type),
                                 self.text_affinity(error_detail), now, tid)
                            )
                            failed += 1
                        else:
                            await self._db.execute(
                                "UPDATE tasks SET status='pending', retry_count=?, error_type=?, error_detail=?, "
                                "worker_id=NULL, lease_epoch=lease_epoch+1, updated_at=? WHERE id=?",
                                (retry_count, self.text_affinity(error_type),
                                 self.text_affinity(error_detail), now, tid)
                            )
                            # 重新入队不计入 accepted；若后续需要单独统计可在此累加 requeued

                _t = time.monotonic()
                await self._db.execute("COMMIT")
                record_stage("commit", (time.monotonic() - _t) * 1000)
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            record_stage("total_in_lock", (time.monotonic() - _t_total) * 1000)

        return {"accepted": accepted, "stale": stale, "failed": failed}

    async def accept_failed_result(self, task_id: int, worker_id: str, lease_epoch: int,
                                   error_type: str = "", error_detail: str = "") -> dict:
        """原子受理失败结果（校验 lease）"""
        return await self.fail_task(task_id, worker_id, lease_epoch, error_type, error_detail)

    async def _save_result_inner_unlocked(self, data: dict, batch_id: int = None) -> bool:
        """保存采集结果到 asin_data（调用方必须持有 _write_lock 并在事务内）

        变动检测基准：baseline 字段（上次定时采集的数据）
        baseline 更新：仅定时采集（is_auto=True）时更新
        """
        asin = data.get("asin", "").strip()
        if not asin:
            return False

        # 控制流仍用原始 batch_id（``if batch_id:`` 的真值语义与 SQLite 一致），
        # 只有绑到 bigint 列时才强转。
        bid = self.as_int(batch_id)

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        data["content_hash"] = _compute_content_hash(data)
        data["title_bullets_hash"] = _compute_title_bullets_hash(data)

        # 查询批次是否为定时采集
        is_auto = False
        if batch_id:
            async with self._db.execute(
                "SELECT is_auto FROM batches WHERE id = ?", (bid,)
            ) as c:
                row = await c.fetchone()
                if row:
                    is_auto = bool(row[0])

        resolved_screenshot_path = await self._get_done_screenshot_path(asin, batch_id)

        # 只查变动检测和更新路径实际用到的列，减少 I/O 与反序列化开销（原先 SELECT *，40+ 列）
        async with self._db.execute(
            "SELECT screenshot_path, title, "
            "baseline_price, baseline_buybox_price, baseline_stock_count, "
            "baseline_stock_status, baseline_title_bullets_hash "
            "FROM asin_data WHERE asin = ?", (asin,)
        ) as c:
            existing = await c.fetchone()

        if existing:
            existing_dict = dict(existing)
            existing_screenshot_path = _normalize_screenshot_path(
                existing_dict.get("screenshot_path")
            )
            changes = []

            # 变动检测：对比 baseline（上次定时采集），而非当前值
            bl_price = existing_dict.get("baseline_price")
            bl_buybox = existing_dict.get("baseline_buybox_price")
            bl_stock = existing_dict.get("baseline_stock_count")
            bl_status = existing_dict.get("baseline_stock_status")
            bl_tb_hash = existing_dict.get("baseline_title_bullets_hash")
            has_baseline = bl_price is not None  # baseline 存在才做变动检测

            if has_baseline:
                # 1. 价格/库存变动（对比 baseline）
                price_change = _compare_price(bl_price, data.get("current_price"))
                buybox_change = _compare_price(bl_buybox, data.get("buybox_price"))
                stock_qty_change = _compare_stock_qty(bl_stock, data.get("stock_count"))
                stock_status_change = _compare_stock_status(bl_status, data.get("stock_status"))

                if any([price_change, buybox_change, stock_qty_change, stock_status_change]):
                    detail_parts = []
                    if price_change:
                        detail_parts.append(f"price:{price_change}")
                    if buybox_change:
                        detail_parts.append(f"buybox:{buybox_change}")
                    if stock_qty_change:
                        detail_parts.append(f"stock_qty:{stock_qty_change}")
                    if stock_status_change:
                        detail_parts.append(f"stock_status:{stock_status_change}")

                    prev_vals = f"price={bl_price}, buybox={bl_buybox}, stock={bl_stock}, status={bl_status}"
                    new_vals = f"price={data.get('current_price')}, buybox={data.get('buybox_price')}, stock={data.get('stock_count')}, status={data.get('stock_status')}"
                    changes.append(("price_stock", ", ".join(detail_parts), prev_vals, new_vals))

                # 2. 标题/五点描述变动（对比 baseline）
                new_tb_hash = data.get("title_bullets_hash", "")
                if bl_tb_hash and new_tb_hash and bl_tb_hash != new_tb_hash:
                    prev_title = (existing_dict.get("title") or "")[:100]
                    new_title = (data.get("title") or "")[:100]
                    changes.append(("title_bullets", "title_or_bullets_changed",
                                    f"title={prev_title}", f"title={new_title}"))

            # 写入变动记录
            for change_type, detail, prev_val, new_val in changes:
                await self._db.execute(
                    "INSERT INTO asin_changes (asin, batch_id, change_type, change_detail, prev_value, new_value) VALUES (?, ?, ?, ?, ?, ?)",
                    (asin, bid, change_type, detail, prev_val, new_val)
                )

            # 更新主表当前值
            update_fields = []
            update_values = []
            for f in ASIN_DATA_FIELDS:
                if f in ("asin", "screenshot_path"):
                    continue
                val = data.get(f)
                if val is not None:
                    update_fields.append(f"{f} = ?")
                    # asin_data 的 50 个业务列全是 text；SQLite 的 TEXT affinity
                    # 会把 int/float/bool 静默转成字符串，asyncpg 不会。
                    update_values.append(text_affinity(val))

            if resolved_screenshot_path and resolved_screenshot_path != existing_screenshot_path:
                update_fields.append("screenshot_path = ?")
                update_values.append(text_affinity(resolved_screenshot_path))

            # 定时采集：同时更新 baseline
            if is_auto:
                update_fields.append("baseline_price = ?")
                update_values.append(text_affinity(data.get("current_price")))
                update_fields.append("baseline_buybox_price = ?")
                update_values.append(text_affinity(data.get("buybox_price")))
                update_fields.append("baseline_stock_count = ?")
                update_values.append(text_affinity(data.get("stock_count")))
                update_fields.append("baseline_stock_status = ?")
                update_values.append(text_affinity(data.get("stock_status")))
                update_fields.append("baseline_title_bullets_hash = ?")
                update_values.append(text_affinity(data.get("title_bullets_hash")))
                update_fields.append("baseline_updated_at = ?")
                update_values.append(now)

            update_fields.append("updated_at = ?")
            update_values.append(now)
            update_values.append(asin)

            await self._db.execute(
                f"UPDATE asin_data SET {', '.join(update_fields)} WHERE asin = ?",
                update_values
            )
        else:
            # 新 ASIN，插入
            insert_fields = ["asin"]
            insert_values = [asin]
            for f in ASIN_DATA_FIELDS:
                if f in ("asin", "screenshot_path"):
                    continue
                val = data.get(f)
                if val is not None:
                    insert_fields.append(f)
                    insert_values.append(text_affinity(val))

            if resolved_screenshot_path:
                insert_fields.append("screenshot_path")
                insert_values.append(text_affinity(resolved_screenshot_path))

            # 首次入库：baseline = 当前值（无论手动还是定时）
            insert_fields.extend([
                "baseline_price", "baseline_buybox_price",
                "baseline_stock_count", "baseline_stock_status",
                "baseline_title_bullets_hash", "baseline_updated_at",
            ])
            insert_values.extend([
                text_affinity(data.get("current_price")),
                text_affinity(data.get("buybox_price")),
                text_affinity(data.get("stock_count")),
                text_affinity(data.get("stock_status")),
                text_affinity(data.get("title_bullets_hash")), now,
            ])

            insert_fields.extend(["created_at", "updated_at"])
            insert_values.extend([now, now])

            placeholders = ",".join("?" * len(insert_values))
            await self._db.execute(
                f"INSERT INTO asin_data ({', '.join(insert_fields)}) VALUES ({placeholders})",
                insert_values
            )

            # 记录新增变动
            if batch_id:
                await self._db.execute(
                    "INSERT INTO asin_changes (asin, batch_id, change_type, change_detail) VALUES (?, ?, 'new', 'first_seen')",
                    (asin, bid)
                )

        return True

    async def save_result(self, data: dict, batch_id: int = None) -> bool:
        """兼容入口：直接保存结果（不走 lease 校验，用于测试/直接写入）"""
        asin = data.get("asin", "").strip()
        if not asin:
            return False
        if _is_parse_failure(data):
            logger.warning(f"解析失败数据跳过: {asin}")
            return False
        async with self._write_lock:
            await self._db.execute("BEGIN")
            try:
                result = await self._save_result_inner_unlocked(data, batch_id)
                await self._db.execute("COMMIT")
                return result
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def save_results_batch(self, results: List[dict], batch_id: int = None) -> int:
        """批量保存结果"""
        saved = 0
        for data in results:
            if await self.save_result(data, batch_id):
                saved += 1
        return saved
