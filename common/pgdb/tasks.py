"""common/pgdb/tasks.py —— 任务队列（创建 / 拉取 / 回收 / 失败 / 释放 / 进度）。

OWNS（9 个方法）:
    create_tasks               database.py:1095
    pull_tasks                 database.py:1154
    reclaim_dead_worker_tasks  database.py:1246
    auto_retry_failed_tasks    database.py:1283
    fail_task                  database.py:1331
    release_tasks              database.py:1383
    prioritize_batch           database.py:1413
    get_progress               database.py:1422
    get_batch_failures         database.py:2092   ← 名字像 results，SQL 全在 tasks 表

对外欠账（别人依赖本文件）:
    create_tasks -> media.py（expand_batch_variants、seller 详情任务）
    fail_task    -> results_write.py（accept_failed_result 是它的薄封装）

--------------------------------------------------------------------------
移植要点（与 SQLite 原版逐条对照，实测结论见文件末尾的 tests/pgdb/test_tasks.py）
--------------------------------------------------------------------------
create_tasks
  * 返回值是"**实际**插入的任务行数"，SQLite 用 ``self._db.total_changes`` 的
    差值算。asyncpg 既没有这个计数器，executemany 也不返回任何东西
    （ConnProxy.total_changes 会直接 raise）。
    → 改成**单条** set-based 插入并读命令标签（pool.rowcount_from_tag）。
    实测：3 行输入全部冲突 → 标签 ``INSERT 0 0``；4 行输入 2 行冲突 →
    ``INSERT 0 2``，与 SQLite 的 total_changes 差值逐字一致。
  * ⚠ 插入**次数**必须与 SQLite 完全一致，因为 identity 的**烧号**被基线钉死
    （task id 是 1,3,7,8——2,4,5,6 被重复上传烧掉）。实测 unnest 形式的
    INSERT ... ON CONFLICT DO NOTHING 对每一条源行都会取一次 nextval，
    冲突行照样烧号，与 SQLite AUTOINCREMENT + INSERT OR IGNORE 完全一致。
    任何"先查已存在再插"的预过滤都会把下游所有 id 挪位。
  * ⚠ 已知且**无法消除**的偏离：``INSERT OR IGNORE`` 吞掉**所有**约束冲突
    （含 NOT NULL），而 ``ON CONFLICT DO NOTHING`` 只吞唯一/排他冲突。
    实测 ``create_tasks(None, [...])``：SQLite 返回 0（NOT NULL 被静默吞掉），
    PG 抛 NotNullViolationError。现有调用方到不了这条路——create_batch 未命中
    时返回的是 0 而不是 None，而 batch_id=0 两边都正常插入（表间无外键）。
    改成 ``ON CONFLICT (batch_id, asin) DO NOTHING`` 也救不了这一类。
  * batch_asins.is_new 的判定（每个 asin 一次 ``SELECT 1 FROM asin_data``）逐条照抄。
  * asins 只 ``.strip()``、不 upper()（database.py:1106）——保持。
  * screenshots 也是 identity 表，同样按"一行一次插入尝试"来烧号。

pull_tasks
  * 返回 dict 的键**恰好**是：id, batch_id, batch_name, asin, zip_code,
    retry_count, priority, needs_screenshot(**Python bool**), lease_epoch,
    task_type(缺省 "asin"), task_meta, discover_mode。
    needs_screenshot 在这里是 bool，而 /api/batches 里同一列是 int 0/1，
    两种形状都被基线钉死，别统一。
  * 行序被基线钉死 → 保持 SQLite 的两段式：先 ORDER BY 的 SELECT 拿行、
    再按 id 列表 UPDATE、最后用 **SELECT 的顺序** 组装响应。
    绝不改写成 ``UPDATE ... RETURNING`` 直接返回（RETURNING 的行序是执行器的
    修改顺序，实测会乱）。
  * 候选 SELECT 加 ``FOR UPDATE OF t SKIP LOCKED``：只锁 tasks，不锁 join 进来的
    batches（否则会和 create_batch / mark_batch_completed 形成锁序环）。
    在当前"单写连接 + 真写锁"（D-2）下它是 no-op，但多进程部署时它是唯一
    挡住双发的东西。
  * ``ORDER BY t.zip_code`` 是可空列 → 补 ``NULLS FIRST``（PG 的 ASC 默认
    NULLS LAST，与 SQLite 正好相反）。

reclaim_dead_worker_tasks / auto_retry_failed_tasks
  * 变长 worker_id 列表换成 ``= ANY(?::text[])``：一个参数、空数组合法、
    绕开 asyncpg 32767 个参数的上限。但**保留**原有的 ``if dead_worker_ids``
    前置分支，两条 SQL 的形状与 SQLite 版一一对应。
  * auto_retry 的 ``NOT IN (...)`` 保持字面形式（COALESCE 保证无 NULL，
    NOT IN 的三值逻辑坑在这里不成立），参数顺序与文本出现顺序一致。
  * ``updated_at < ?`` 是定宽 '%Y-%m-%d %H:%M:%S' 字符串比较；列上有
    COLLATE "C"，字节序 = 时序，索引可用。

fail_task
  * SELECT retry_count 的 WHERE 比后面 UPDATE 的 WHERE **更严**
    （SELECT 有 status='processing'，UPDATE 没有）。这是有意保留的现状。
    给 SELECT 加 ``FOR UPDATE`` 关掉 TOCTOU 窗口，其余一个字不改——
    包括那个 FOR UPDATE 之后已经走不到的 ``rowcount == 0 -> stale`` 分支。
  * 返回 {"accepted": bool, "stale": bool}。

release_tasks
  * 入参 [{"task_id": int, "lease_epoch": int}, ...]，按 lease_epoch 分组
    各发一条 UPDATE；空输入返回 int 0。

get_progress
  * {pending, processing, done, failed, total, completion_rate, success_rate}。
    比率在 **Python** 里算，不要挪进 SQL（PG 的除法会返回 numeric → Decimal
    → JSON 字符串）。
  * ⚠ 规格书（和本文件早先的注释）说"未知 status 会 KeyError"——**是错的**。
    那行是 ``stats[row["status"]] = row["cnt"]``，赋值不是取值，不会抛。
    实测两个后端一致：未知 status 会往返回的 dict 里**塞一个额外的 key**
    （而这个 dict 就是 HTTP 响应体），且不计入 ``total``。
    照抄这个行为；**不要**为了"保留 bug"去加 raise —— 那是真的引入回归。

get_batch_failures
  * 键：asin, status, error_type, error_detail, retry_count, worker_id, updated_at。
    ``ORDER BY updated_at DESC`` → 补 ``NULLS LAST`` 才等于 SQLite。

类型强转
  * 绑到 text 列的值一律过 ``self.text_affinity()``（worker_id / zip_code /
    error_type / error_detail / asin）。裸 ``str()`` 是错的（True -> 'True'）。
  * 绑到 integer/bigint 列的值一律过 ``self.as_int()``：task_id / batch_id /
    lease_epoch 全部来自未经校验的 ``await request.json()``，SQLite 靠列 affinity
    把 '3' 转成 3 并命中，asyncpg 会直接 DataError。强转后行为与 SQLite 一致
    （无法解析 → None → 谓词不匹配 → stale，与 SQLite 的"类型不同故不相等"同结果）。
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from common import config
from common.pgdb._shared import (  # noqa: F401
    NO_AUTO_RETRY_ERROR_TYPES,
    LIMITED_RETRY_ERROR_TYPES,
    _fail_cap,
)

logger = logging.getLogger(__name__)


class TasksMixin:
    """只定义方法，绝不定义 __init__。"""

    # ==================== 任务操作 ====================

    async def create_tasks(self, batch_id: int, asins: List[str],
                           zip_code: str = "10001",
                           needs_screenshot: bool = False,
                           per_asin_zip: Dict[str, str] = None) -> int:
        """批量创建采集任务，同时维护 batch_asins 关联。

        per_asin_zip: 可选 {asin: zip} 映射；某个 asin 在其中则用该 zip，否则回落到 zip_code。
        """
        clean_asins = []
        seen = set()
        ss_val = 1 if needs_screenshot else 0
        for asin in asins:
            asin = asin.strip()
            if asin and asin not in seen:
                clean_asins.append(asin)
                seen.add(asin)

        if not clean_asins:
            return 0

        per_asin_zip = per_asin_zip or {}

        bid = self.as_int(batch_id)
        zips = [self.text_affinity(per_asin_zip.get(asin) or zip_code)
                for asin in clean_asins]

        async with self._write_lock:
            await self._db.execute("BEGIN")
            try:
                # 插入任务（每个 asin 用各自指定的 zip，未指定则用批次默认）
                #
                # SQLite 版是 executemany(INSERT OR IGNORE) + total_changes 差值。
                # 这里换成单条 set-based 插入并读命令标签：源行数 = 尝试插入次数，
                # 所以 identity 的烧号与 SQLite 逐个 INSERT OR IGNORE 完全一致，
                # 而标签里的计数 = 真正落库的行数 = total_changes 的差值。
                cursor = await self._db.execute(
                    "INSERT INTO tasks (batch_id, asin, zip_code, needs_screenshot) "
                    "SELECT ?::bigint, u.asin, u.zip, ?::int "
                    "  FROM unnest(?::text[], ?::text[]) AS u(asin, zip) "
                    "ON CONFLICT DO NOTHING",
                    (bid, ss_val, clean_asins, zips)
                )
                task_inserted = cursor.rowcount

                # 维护 batch_asins（判断是否新 ASIN）
                for asin in clean_asins:
                    async with self._db.execute(
                        "SELECT 1 FROM asin_data WHERE asin = ?", (asin,)
                    ) as c:
                        exists = await c.fetchone()
                    is_new = 0 if exists else 1
                    await self._db.execute(
                        "INSERT INTO batch_asins (batch_id, asin, is_new) VALUES (?, ?, ?) "
                        "ON CONFLICT DO NOTHING",
                        (bid, asin, is_new)
                    )

                # 如果需要截图，创建截图任务
                # screenshots 也是 identity 表：一行一次插入尝试，烧号方式与
                # SQLite 的 executemany(INSERT OR IGNORE) 一致。
                if needs_screenshot:
                    await self._db.execute(
                        "INSERT INTO screenshots (asin, batch_id) "
                        "SELECT u.asin, ?::bigint FROM unnest(?::text[]) AS u(asin) "
                        "ON CONFLICT DO NOTHING",
                        (bid, clean_asins)
                    )

                inserted = task_inserted
                await self._db.execute("COMMIT")
            except Exception:
                await self._db.execute("ROLLBACK")
                raise
        return inserted

    async def pull_tasks(self, worker_id: str, count: int = 10,
                         needs_screenshot=None,
                         prefer_zip: Optional[str] = None) -> List[Dict]:
        """Worker 拉取待处理任务（原子操作，不再内联超时回收）。

        prefer_zip: worker 当前 session 的邮编。若指定，server 优先返回相同 zip 的任务，
        从而最大化复用同一 session（避免每个任务都切换邮编）。多 worker 不同 prefer_zip
        时各自被分到对应邮编池，自然分流。
        """
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tasks = []

        async with self._write_lock("pull_tasks"):
            # SQLite 的 BEGIN IMMEDIATE 是为了提前抢写锁；PG 没有这个语义，
            # 垫片会把它当普通 BEGIN。
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                ss_filter = ""
                ss_params = []
                if needs_screenshot is not None:
                    ss_filter = " AND t.needs_screenshot = ?"
                    ss_params = [1 if needs_screenshot else 0]

                async with self._db.execute(
                    f"SELECT MAX(priority) FROM tasks t WHERE t.status = 'pending'{ss_filter}",
                    ss_params
                ) as cur:
                    row = await cur.fetchone()
                    top_priority = row[0] if row and row[0] is not None else 0

                # 排序策略：
                #   1. prefer_zip 匹配优先（同 zip 任务先派发，节省 session 切换）
                #   2. 同 zip 内按 id 升序（FIFO，先入先出）
                # zip_code 可空 → 显式 NULLS FIRST，才等于 SQLite 的 ASC 排序
                if prefer_zip:
                    order_clause = ("ORDER BY CASE WHEN t.zip_code = ? THEN 0 ELSE 1 END, "
                                    "t.zip_code ASC NULLS FIRST, t.id ASC")
                    extra_params = [self.text_affinity(prefer_zip)]
                else:
                    # 无偏好时按 zip_code 分组，同 zip 仍尽量连续派发
                    order_clause = "ORDER BY t.zip_code ASC NULLS FIRST, t.id ASC"
                    extra_params = []

                # FOR UPDATE OF t SKIP LOCKED：只锁 tasks，不锁 join 进来的 batches。
                # 单写连接下是 no-op；多进程部署时它是唯一挡住"同一任务双发"的东西。
                async with self._db.execute(
                    f"""SELECT t.id, t.batch_id, t.asin, t.zip_code, t.retry_count,
                               t.priority, t.needs_screenshot, t.lease_epoch,
                               t.task_type, t.task_meta,
                               b.name as batch_name, b.discover_mode
                        FROM tasks t
                        JOIN batches b ON b.id = t.batch_id
                        WHERE t.status = 'pending' AND t.priority = ?{ss_filter}
                        {order_clause}
                        LIMIT ?
                        FOR UPDATE OF t SKIP LOCKED""",
                    (top_priority, *ss_params, *extra_params, int(count))
                ) as cursor:
                    rows = await cursor.fetchall()

                if not rows:
                    await self._db.execute("COMMIT")
                    return tasks

                ids = []
                for row in rows:
                    task = {
                        "id": row["id"],
                        "batch_id": row["batch_id"],
                        "batch_name": row["batch_name"],
                        "asin": row["asin"],
                        "zip_code": row["zip_code"],
                        "retry_count": row["retry_count"],
                        "priority": row["priority"],
                        "needs_screenshot": bool(row["needs_screenshot"]),
                        "lease_epoch": row["lease_epoch"],
                        "task_type": row["task_type"] or "asin",
                        "task_meta": row["task_meta"],
                        "discover_mode": row["discover_mode"],
                    }
                    tasks.append(task)
                    ids.append(row["id"])

                # 变长 IN 列表 → = ANY(array)：一个参数，不受 32767 参数上限约束
                await self._db.execute(
                    "UPDATE tasks SET status='processing', worker_id=?, updated_at=? "
                    "WHERE id = ANY(?::bigint[])",
                    (self.text_affinity(worker_id), now, ids)
                )
                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise

        return tasks

    async def reclaim_dead_worker_tasks(self, dead_worker_ids: List[str]):
        """回收死 Worker 的任务 + 硬超时兜底（liveness safety net）

        所有回收路径都 bump lease_epoch，让旧 Worker 的迟到结果失效。
        合成一条 SQL 防止双重 bump。
        """
        if not dead_worker_ids:
            dead_worker_ids = []
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        hard_cutoff = (datetime.utcnow() - timedelta(minutes=config.TASK_TIMEOUT_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')

        async with self._write_lock:
            await self._db.execute("BEGIN")
            # 原版没有回滚路径（database.py:1258）。SQLite 下一条语句报错只是把连接
            # 留在事务里；PG 下事务 abort + 写锁随异常释放 + 垫片事务槽不清 =
            # 之后**每一次** BEGIN 都撞"嵌套 BEGIN"，整条写路径永久焊死。
            # 捕 BaseException 而不是 Exception：本方法由 _timeout_task_loop 调用，
            # 停服时会被 cancel，CancelledError 同样必须回滚。
            # 返回值、异常类型、成功路径都没变。
            try:
                if dead_worker_ids:
                    cursor = await self._db.execute(
                        """UPDATE tasks SET status='pending', worker_id=NULL,
                               lease_epoch=lease_epoch+1, updated_at=?
                           WHERE status='processing' AND (
                               worker_id = ANY(?::text[]) OR updated_at < ?
                           )""",
                        (now, [self.text_affinity(w) for w in dead_worker_ids], hard_cutoff)
                    )
                else:
                    # 没有死 Worker，只做硬超时兜底
                    cursor = await self._db.execute(
                        "UPDATE tasks SET status='pending', worker_id=NULL, "
                        "lease_epoch=lease_epoch+1, updated_at=? "
                        "WHERE status='processing' AND updated_at < ?",
                        (now, hard_cutoff)
                    )
                reclaimed = cursor.rowcount
                await self._db.execute("COMMIT")
            except BaseException:
                try:
                    await self._db.execute("ROLLBACK")
                except BaseException:
                    pass
                raise
        if reclaimed > 0:
            logger.info(f"回收 {reclaimed} 个任务 (dead_workers={len(dead_worker_ids)}, hard_cutoff={hard_cutoff})")
        return reclaimed

    async def auto_retry_failed_tasks(self, max_auto_cycles: int = 2,
                                      delay_minutes: int = 5) -> int:
        """自动重试终态失败的任务：重置为 pending、重置 retry_count、bump epoch、bump auto_retry_count。

        一个任务达到 MAX_RETRIES (默认 3) 后变为 failed 终态。本方法把这些
        失败任务重新入队，让它们再走一遍最多 3 次重试。最多允许 max_auto_cycles 轮。
        总体尝试次数上限 ≈ MAX_RETRIES * (1 + max_auto_cycles)。
        每次自动重试前，任务的 updated_at 至少要早于 delay_minutes 分钟（避免刚失败就立刻再跑）。

        排除：NO_AUTO_RETRY_ERROR_TYPES 中的失败（如 variant_offset）。
        这些类型是稳定的产品/页面层事实，循环重试会浪费配额。
        """
        if max_auto_cycles <= 0:
            return 0
        now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cutoff = (datetime.utcnow() - timedelta(minutes=delay_minutes)).strftime('%Y-%m-%d %H:%M:%S')

        # 动态构造 NOT IN (...) 子句
        # COALESCE 保证左侧非 NULL，右侧全是字面量，所以 PG 的 NOT IN 三值逻辑
        # 在这里与 SQLite 完全一致，保持字面形式。
        no_retry_list = sorted(NO_AUTO_RETRY_ERROR_TYPES)
        no_retry_placeholders = ",".join("?" * len(no_retry_list))
        excl_clause = f"AND COALESCE(error_type, '') NOT IN ({no_retry_placeholders}) " if no_retry_list else ""

        async with self._write_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._db.execute(
                    "UPDATE tasks SET status='pending', worker_id=NULL, retry_count=0, "
                    "lease_epoch=lease_epoch+1, "
                    "auto_retry_count=COALESCE(auto_retry_count,0)+1, "
                    "updated_at=? "
                    "WHERE status='failed' "
                    "AND COALESCE(auto_retry_count,0) < ? "
                    "AND updated_at < ? "
                    f"{excl_clause}",
                    (now_str, int(max_auto_cycles), cutoff, *no_retry_list)
                )
                retried = cursor.rowcount
                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        if retried > 0:
            logger.info(f"自动重试 {retried} 个失败任务 (delay={delay_minutes}min, max_cycles={max_auto_cycles})")
        return retried

    async def fail_task(self, task_id: int, worker_id: str, lease_epoch: int,
                        error_type: str = "", error_detail: str = "") -> dict:
        """标记任务失败（校验 worker_id + lease_epoch）

        Returns: {"accepted": True/False, "stale": True/False}
        """
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tid = self.as_int(task_id)
        wid = self.text_affinity(worker_id)
        epoch = self.as_int(lease_epoch)
        et = self.text_affinity(error_type)
        ed = self.text_affinity(error_detail)
        async with self._write_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                # 在同一事务内 SELECT + UPDATE，避免 TOCTOU（与 reclaim 循环并发时旧代码会静默失配）
                # FOR UPDATE 把 SQLite 全局写锁提供的那段原子性补回来。
                async with self._db.execute(
                    "SELECT retry_count FROM tasks WHERE id=? AND worker_id=? AND lease_epoch=? "
                    "AND status='processing' FOR UPDATE",
                    (tid, wid, epoch)
                ) as c:
                    row = await c.fetchone()
                if not row:
                    await self._db.execute("ROLLBACK")
                    return {"accepted": False, "stale": True}
                retry_count = row[0] + 1

                # 按 error_type 决定该任务的失败上限：
                # - variant_offset: cap=1（不重试）
                # - 其他: cap=MAX_RETRIES（=3）
                cap = _fail_cap(error_type)
                if retry_count >= cap:
                    cursor = await self._db.execute(
                        "UPDATE tasks SET status='failed', retry_count=?, error_type=?, error_detail=?, updated_at=? "
                        "WHERE id=? AND worker_id=? AND lease_epoch=?",
                        (retry_count, et, ed, now, tid, wid, epoch)
                    )
                else:
                    # 回 pending + bump epoch
                    cursor = await self._db.execute(
                        "UPDATE tasks SET status='pending', retry_count=?, error_type=?, error_detail=?, "
                        "worker_id=NULL, lease_epoch=lease_epoch+1, updated_at=? "
                        "WHERE id=? AND worker_id=? AND lease_epoch=?",
                        (retry_count, et, ed, now, tid, wid, epoch)
                    )
                if cursor.rowcount == 0:
                    # 极罕见：事务内 SELECT 成功但 UPDATE 失配，按 stale 处理。
                    # 加了 FOR UPDATE 之后这条分支已经走不到，但删掉它是行为改变。
                    await self._db.execute("ROLLBACK")
                    return {"accepted": False, "stale": True}
                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        return {"accepted": True, "stale": False}

    async def release_tasks(self, worker_id: str, tasks: List[Dict]):
        """释放任务回 pending（校验 worker_id + lease_epoch，bump epoch）

        tasks: [{"task_id": int, "lease_epoch": int}, ...]
        """
        if not tasks:
            return 0
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        released = 0

        # 按 epoch 分组批量更新
        groups = defaultdict(list)
        for t in tasks:
            groups[self.as_int(t["lease_epoch"])].append(self.as_int(t["task_id"]))

        wid = self.text_affinity(worker_id)
        async with self._write_lock:
            await self._db.execute("BEGIN")
            # 回滚路径同 reclaim_dead_worker_tasks：原版（database.py:1400）没有，
            # PG 下缺它就是"一个失败请求焊死整条写路径"。只在错误路径上跑。
            try:
                for epoch, ids in groups.items():
                    cursor = await self._db.execute(
                        "UPDATE tasks SET status='pending', worker_id=NULL, "
                        "lease_epoch=lease_epoch+1, updated_at=? "
                        "WHERE id = ANY(?::bigint[]) AND worker_id=? AND lease_epoch=? "
                        "AND status='processing'",
                        (now, ids, wid, epoch)
                    )
                    released += cursor.rowcount
                await self._db.execute("COMMIT")
            except BaseException:
                try:
                    await self._db.execute("ROLLBACK")
                except BaseException:
                    pass
                raise
        return released

    async def prioritize_batch(self, batch_id: int, priority: int = 10):
        async with self._write_lock:
            await self._db.execute("BEGIN")
            # 回滚路径同 reclaim_dead_worker_tasks（原版 database.py:1415 没有）。
            try:
                await self._db.execute(
                    "UPDATE tasks SET priority=? WHERE batch_id=? AND status='pending'",
                    (self.as_int(priority), self.as_int(batch_id))
                )
                await self._db.execute("COMMIT")
            except BaseException:
                try:
                    await self._db.execute("ROLLBACK")
                except BaseException:
                    pass
                raise

    async def get_progress(self, batch_id: int = None) -> Dict:
        """获取任务进度"""
        if batch_id:
            sql = "SELECT status, COUNT(*) as cnt FROM tasks WHERE batch_id = ? GROUP BY status"
            params = (self.as_int(batch_id),)
        else:
            sql = "SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"
            params = ()

        stats = {"pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0}
        async with self.read() as rc, rc.execute(sql, params) as c:
            async for row in c:
                # 赋值，不是取值：未知 status 不会 KeyError，而是往返回的 dict
                # 里多塞一个 key（该 dict 直接就是 HTTP 响应体），且不计入
                # total。与 SQLite 实测一致，照抄。
                stats[row["status"]] = row["cnt"]
        stats["total"] = sum(stats[k] for k in ["pending", "processing", "done", "failed"])
        finished = stats["done"] + stats["failed"]
        stats["completion_rate"] = round(stats["done"] / stats["total"] * 100, 1) if stats["total"] else 0
        stats["success_rate"] = round(stats["done"] / finished * 100, 1) if finished else 0
        return stats

    async def get_batch_failures(
        self,
        batch_id: int,
        error_types: Optional[List[str]] = None,
        limit: int = 100000,
    ) -> List[Dict]:
        """返回指定批次失败任务明细，用于调用方按失败原因处理本批最新采集状态。"""
        where_parts = ["batch_id = ?", "status = 'failed'"]
        params: list = [self.as_int(batch_id)]
        clean_types = [str(t).strip() for t in (error_types or []) if str(t).strip()]
        if clean_types:
            placeholders = ",".join("?" for _ in clean_types)
            where_parts.append(f"COALESCE(error_type, '') IN ({placeholders})")
            params.extend(clean_types)
        params.append(max(1, min(int(limit or 100000), 100000)))

        # updated_at 可空 → DESC 补 NULLS LAST，才等于 SQLite 的 DESC 排序
        sql = f"""
            SELECT asin, status, error_type, error_detail, retry_count, worker_id, updated_at
            FROM tasks
            WHERE {' AND '.join(where_parts)}
            ORDER BY updated_at DESC NULLS LAST, id DESC
            LIMIT ?
        """
        async with self.read() as rc, rc.execute(sql, params) as c:
            return [dict(r) for r in await c.fetchall()]
