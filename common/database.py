"""
Amazon ASIN 采集系统 v3 - 数据库模块
重新设计的 Schema，针对百万级 ASIN 优化：
- keyset 分页替代 OFFSET
- batch_asins 直接 JOIN 替代 EXISTS 子查询
- asin_changes 预计算表替代运行时 OR 多列筛选
- 合理的复合索引
"""
import os
import re
import hashlib
import aiosqlite
import asyncio
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta

from common import config

logger = logging.getLogger(__name__)

# ==================== 变动对比辅助函数 ====================

_NA_VALUES = {"", "N/A", "n/a", "None", "none", None}


def _is_parse_failure(data: dict) -> bool:
    """检测采集结果是否为解析失败"""
    key_fields = ["current_price", "buybox_price", "stock_count", "stock_status", "brand"]
    all_empty = all(data.get(f) in _NA_VALUES for f in key_fields)
    price_na = data.get("current_price") in _NA_VALUES and data.get("buybox_price") in _NA_VALUES
    stock_999 = str(data.get("stock_count", "")).strip() == "999"
    return all_empty or (price_na and stock_999)


def _parse_price_float(s) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", "")
    s = re.sub(r'^[^\d.-]+', '', s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _compare_price(old_str, new_str) -> Optional[str]:
    old_val = _parse_price_float(old_str)
    new_val = _parse_price_float(new_str)
    if old_val is None or new_val is None:
        return None
    if new_val > old_val:
        return "up"
    elif new_val < old_val:
        return "down"
    return None


def _compare_stock_qty(old_str, new_str) -> Optional[str]:
    def parse_int(s):
        if not s:
            return None
        s = str(s).strip().replace(",", "")
        m = re.search(r'(\d+)', s)
        return int(m.group(1)) if m else None
    old_val = parse_int(old_str)
    new_val = parse_int(new_str)
    if old_val is None or new_val is None:
        return None
    if new_val > old_val:
        return "up"
    elif new_val < old_val:
        return "down"
    return None


def _compare_stock_status(old_str, new_str) -> Optional[str]:
    def normalize(s):
        v = str(s or "").strip().lower()
        return None if v in ("", "n/a", "none") else v
    old_n = normalize(old_str)
    new_n = normalize(new_str)
    if old_n is None or new_n is None:
        return None
    return "changed" if old_n != new_n else None


# 内容 hash 字段（排除价格/库存等高波动字段）
_HASH_FIELDS = [
    "title", "brand", "product_type", "manufacturer", "model_number",
    "part_number", "country_of_origin", "is_customized", "best_sellers_rank",
    "bullet_points", "long_description", "image_urls",
    "upc_list", "ean_list", "parent_asin", "variation_asins",
    "root_category_id", "category_ids", "category_tree",
    "first_available_date", "package_dimensions", "package_weight",
    "item_dimensions", "item_weight",
]

# 标题/五点描述 hash 字段
_TITLE_BULLETS_FIELDS = ["title", "bullet_points"]


def _compute_content_hash(data: dict) -> str:
    parts = [str(data.get(f, "") or "") for f in _HASH_FIELDS]
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _compute_title_bullets_hash(data: dict) -> str:
    parts = [str(data.get(f, "") or "") for f in _TITLE_BULLETS_FIELDS]
    return hashlib.md5("|".join(parts).encode()).hexdigest()


# ASIN 数据字段列表（对应 asin_data 表列）
ASIN_DATA_FIELDS = [
    "asin", "title", "brand", "product_type", "manufacturer", "model_number",
    "part_number", "country_of_origin", "is_customized", "best_sellers_rank",
    "original_price", "current_price", "buybox_price", "buybox_shipping",
    "is_fba", "stock_count", "stock_status", "delivery_date", "delivery_time",
    "image_urls", "bullet_points", "long_description", "upc_list", "ean_list",
    "parent_asin", "variation_asins", "root_category_id", "category_ids",
    "category_tree", "first_available_date", "package_dimensions",
    "package_weight", "item_dimensions", "item_weight", "product_url",
    "site", "zip_code", "crawl_time", "screenshot_path",
    "content_hash", "title_bullets_hash",
]


class Database:
    """异步 SQLite 数据库管理器 v3"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self._db: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    async def connect(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path, isolation_level=None)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=5000")
        # 低配服务器：限制缓存 16MB（v2 用 32MB，v3 降低以适应 2GB 内存）
        await self._db.execute("PRAGMA cache_size=-16000")
        await self._db.execute("PRAGMA mmap_size=33554432")  # 32MB mmap
        self._db.row_factory = aiosqlite.Row
        await self.init_tables()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def init_tables(self):
        await self._db.executescript("""
            -- 批次表
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                needs_screenshot BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 批次-ASIN 关联表（核心优化：直接 JOIN 替代 EXISTS 子查询）
            CREATE TABLE IF NOT EXISTS batch_asins (
                batch_id INTEGER NOT NULL,
                asin TEXT NOT NULL,
                is_new INTEGER DEFAULT 0,
                PRIMARY KEY (batch_id, asin)
            );
            CREATE INDEX IF NOT EXISTS idx_batch_asins_asin ON batch_asins(asin);

            -- ASIN 数据主表（每 ASIN 一行，存储最新状态）
            CREATE TABLE IF NOT EXISTS asin_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL UNIQUE,
                title TEXT,
                brand TEXT,
                product_type TEXT,
                manufacturer TEXT,
                model_number TEXT,
                part_number TEXT,
                country_of_origin TEXT,
                is_customized TEXT,
                best_sellers_rank TEXT,
                original_price TEXT,
                current_price TEXT,
                buybox_price TEXT,
                buybox_shipping TEXT,
                is_fba TEXT,
                stock_count TEXT,
                stock_status TEXT,
                delivery_date TEXT,
                delivery_time TEXT,
                image_urls TEXT,
                bullet_points TEXT,
                long_description TEXT,
                upc_list TEXT,
                ean_list TEXT,
                parent_asin TEXT,
                variation_asins TEXT,
                root_category_id TEXT,
                category_ids TEXT,
                category_tree TEXT,
                first_available_date TEXT,
                package_dimensions TEXT,
                package_weight TEXT,
                item_dimensions TEXT,
                item_weight TEXT,
                product_url TEXT,
                site TEXT DEFAULT 'amazon.com',
                zip_code TEXT DEFAULT '10001',
                crawl_time TEXT,
                screenshot_path TEXT,
                content_hash TEXT,
                title_bullets_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 变动记录表（预计算，按类型索引，支持高效筛选）
            CREATE TABLE IF NOT EXISTS asin_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                batch_id INTEGER,
                change_type TEXT NOT NULL,
                change_detail TEXT,
                prev_value TEXT,
                new_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_changes_asin ON asin_changes(asin);
            CREATE INDEX IF NOT EXISTS idx_changes_type ON asin_changes(change_type);
            CREATE INDEX IF NOT EXISTS idx_changes_batch_type ON asin_changes(batch_id, change_type);

            -- 采集任务表
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                asin TEXT NOT NULL,
                zip_code TEXT DEFAULT '10001',
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 0,
                needs_screenshot BOOLEAN DEFAULT 0,
                worker_id TEXT,
                retry_count INTEGER DEFAULT 0,
                error_type TEXT,
                error_detail TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(batch_id, asin)
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_batch ON tasks(batch_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_status_priority ON tasks(status, priority DESC);

            -- 截图任务表（独立追踪，可靠重试）
            CREATE TABLE IF NOT EXISTS screenshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                batch_id INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                error_detail TEXT,
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(batch_id, asin)
            );
            CREATE INDEX IF NOT EXISTS idx_screenshots_status ON screenshots(status);
            CREATE INDEX IF NOT EXISTS idx_screenshots_batch ON screenshots(batch_id);
        """)

    # ==================== 批次操作 ====================

    async def create_batch(self, name: str, needs_screenshot: bool = False) -> int:
        """创建批次，返回 batch_id"""
        async with self._write_lock:
            await self._db.execute("BEGIN")
            await self._db.execute(
                "INSERT OR IGNORE INTO batches (name, needs_screenshot) VALUES (?, ?)",
                (name, 1 if needs_screenshot else 0)
            )
            await self._db.execute("COMMIT")
            async with self._db.execute("SELECT id FROM batches WHERE name = ?", (name,)) as c:
                row = await c.fetchone()
                return row[0] if row else 0

    async def get_batches(self) -> List[Dict]:
        """获取所有批次及其统计"""
        sql = """
            SELECT b.id, b.name, b.needs_screenshot, b.created_at,
                   COUNT(t.id) as total_tasks,
                   SUM(CASE WHEN t.status = 'done' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END) as pending,
                   SUM(CASE WHEN t.status = 'processing' THEN 1 ELSE 0 END) as processing
            FROM batches b
            LEFT JOIN tasks t ON t.batch_id = b.id
            GROUP BY b.id
            ORDER BY b.id DESC
        """
        async with self._db.execute(sql) as c:
            rows = await c.fetchall()
            return [dict(r) for r in rows]

    async def get_batch_by_name(self, name: str) -> Optional[Dict]:
        async with self._db.execute("SELECT * FROM batches WHERE name = ?", (name,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

    # ==================== 任务操作 ====================

    async def create_tasks(self, batch_id: int, asins: List[str], zip_code: str = "10001",
                           needs_screenshot: bool = False) -> int:
        """批量创建采集任务，同时维护 batch_asins 关联"""
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

        async with self._write_lock:
            await self._db.execute("BEGIN")
            try:
                before = self._db.total_changes

                # 插入任务
                await self._db.executemany(
                    "INSERT OR IGNORE INTO tasks (batch_id, asin, zip_code, needs_screenshot) VALUES (?, ?, ?, ?)",
                    [(batch_id, asin, zip_code, ss_val) for asin in clean_asins]
                )

                # 维护 batch_asins（判断是否新 ASIN）
                for asin in clean_asins:
                    async with self._db.execute(
                        "SELECT 1 FROM asin_data WHERE asin = ?", (asin,)
                    ) as c:
                        exists = await c.fetchone()
                    is_new = 0 if exists else 1
                    await self._db.execute(
                        "INSERT OR IGNORE INTO batch_asins (batch_id, asin, is_new) VALUES (?, ?, ?)",
                        (batch_id, asin, is_new)
                    )

                # 如果需要截图，创建截图任务
                if needs_screenshot:
                    await self._db.executemany(
                        "INSERT OR IGNORE INTO screenshots (asin, batch_id) VALUES (?, ?)",
                        [(asin, batch_id) for asin in clean_asins]
                    )

                inserted = self._db.total_changes - before
                await self._db.execute("COMMIT")
            except Exception:
                await self._db.execute("ROLLBACK")
                raise
        return inserted

    async def pull_tasks(self, worker_id: str, count: int = 10,
                         needs_screenshot=None) -> List[Dict]:
        """Worker 拉取待处理任务（原子操作）"""
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tasks = []

        async with self._write_lock:
            await self._reset_timeout_tasks_unlocked()
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

                async with self._db.execute(
                    f"""SELECT t.id, t.batch_id, t.asin, t.zip_code, t.retry_count,
                               t.priority, t.needs_screenshot, b.name as batch_name
                        FROM tasks t
                        JOIN batches b ON b.id = t.batch_id
                        WHERE t.status = 'pending' AND t.priority = ?{ss_filter}
                        ORDER BY t.id ASC
                        LIMIT ?""",
                    (top_priority, *ss_params, count)
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
                    }
                    tasks.append(task)
                    ids.append(row["id"])

                placeholders = ",".join("?" * len(ids))
                await self._db.execute(
                    f"UPDATE tasks SET status='processing', worker_id=?, updated_at=? WHERE id IN ({placeholders})",
                    [worker_id, now] + ids
                )
                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise

        return tasks

    async def _reset_timeout_tasks_unlocked(self):
        """回退超时任务（必须在 _write_lock 内调用）"""
        cutoff = (datetime.utcnow() - timedelta(minutes=config.TASK_TIMEOUT_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')
        await self._db.execute(
            "UPDATE tasks SET status='pending', worker_id=NULL WHERE status='processing' AND updated_at < ?",
            (cutoff,)
        )

    async def reset_timeout_tasks(self):
        async with self._write_lock:
            await self._db.execute("BEGIN")
            await self._reset_timeout_tasks_unlocked()
            await self._db.execute("COMMIT")

    async def fail_task(self, task_id: int, error_type: str = "", error_detail: str = ""):
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        async with self._write_lock:
            async with self._db.execute(
                "SELECT retry_count FROM tasks WHERE id = ?", (task_id,)
            ) as c:
                row = await c.fetchone()
                if not row:
                    return
                retry_count = row[0] + 1

            await self._db.execute("BEGIN")
            if retry_count >= config.MAX_RETRIES:
                await self._db.execute(
                    "UPDATE tasks SET status='failed', retry_count=?, error_type=?, error_detail=?, updated_at=? WHERE id=?",
                    (retry_count, error_type, error_detail, now, task_id)
                )
            else:
                await self._db.execute(
                    "UPDATE tasks SET status='pending', retry_count=?, error_type=?, error_detail=?, worker_id=NULL, updated_at=? WHERE id=?",
                    (retry_count, error_type, error_detail, now, task_id)
                )
            await self._db.execute("COMMIT")

    async def complete_task(self, task_id: int):
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        async with self._write_lock:
            await self._db.execute("BEGIN")
            await self._db.execute(
                "UPDATE tasks SET status='done', updated_at=? WHERE id=?",
                (now, task_id)
            )
            await self._db.execute("COMMIT")

    async def release_tasks(self, task_ids: List[int]):
        """释放任务回 pending 状态"""
        if not task_ids:
            return
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        async with self._write_lock:
            await self._db.execute("BEGIN")
            placeholders = ",".join("?" * len(task_ids))
            await self._db.execute(
                f"UPDATE tasks SET status='pending', worker_id=NULL, updated_at=? WHERE id IN ({placeholders})",
                [now] + task_ids
            )
            await self._db.execute("COMMIT")

    async def prioritize_batch(self, batch_id: int, priority: int = 10):
        async with self._write_lock:
            await self._db.execute("BEGIN")
            await self._db.execute(
                "UPDATE tasks SET priority=? WHERE batch_id=? AND status='pending'",
                (priority, batch_id)
            )
            await self._db.execute("COMMIT")

    async def get_progress(self, batch_id: int = None) -> Dict:
        """获取任务进度"""
        if batch_id:
            sql = "SELECT status, COUNT(*) as cnt FROM tasks WHERE batch_id = ? GROUP BY status"
            params = (batch_id,)
        else:
            sql = "SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"
            params = ()

        stats = {"pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0}
        async with self._db.execute(sql, params) as c:
            async for row in c:
                stats[row["status"]] = row["cnt"]
        stats["total"] = sum(stats[k] for k in ["pending", "processing", "done", "failed"])
        finished = stats["done"] + stats["failed"]
        stats["completion_rate"] = round(stats["done"] / stats["total"] * 100, 1) if stats["total"] else 0
        stats["success_rate"] = round(stats["done"] / finished * 100, 1) if finished else 0
        return stats

    # ==================== 结果操作（含变动检测）====================

    async def save_result(self, data: dict, batch_id: int = None) -> bool:
        """保存采集结果，自动检测变动并写入 asin_changes"""
        asin = data.get("asin", "").strip()
        if not asin:
            return False

        if _is_parse_failure(data):
            logger.warning(f"解析失败数据跳过: {asin}")
            return False

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        data["content_hash"] = _compute_content_hash(data)
        data["title_bullets_hash"] = _compute_title_bullets_hash(data)

        async with self._write_lock:
            # 查询已有记录
            async with self._db.execute(
                "SELECT * FROM asin_data WHERE asin = ?", (asin,)
            ) as c:
                existing = await c.fetchone()

            await self._db.execute("BEGIN")
            try:
                if existing:
                    existing_dict = dict(existing)
                    changes = []

                    # 1. 价格/库存变动检测
                    price_change = _compare_price(existing_dict.get("current_price"), data.get("current_price"))
                    buybox_change = _compare_price(existing_dict.get("buybox_price"), data.get("buybox_price"))
                    stock_qty_change = _compare_stock_qty(existing_dict.get("stock_count"), data.get("stock_count"))
                    stock_status_change = _compare_stock_status(existing_dict.get("stock_status"), data.get("stock_status"))

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

                        prev_vals = f"price={existing_dict.get('current_price')}, buybox={existing_dict.get('buybox_price')}, stock={existing_dict.get('stock_count')}, status={existing_dict.get('stock_status')}"
                        new_vals = f"price={data.get('current_price')}, buybox={data.get('buybox_price')}, stock={data.get('stock_count')}, status={data.get('stock_status')}"

                        changes.append(("price_stock", ", ".join(detail_parts), prev_vals, new_vals))

                    # 2. 标题/五点描述变动检测
                    old_tb_hash = existing_dict.get("title_bullets_hash", "")
                    new_tb_hash = data.get("title_bullets_hash", "")
                    if old_tb_hash and new_tb_hash and old_tb_hash != new_tb_hash:
                        prev_title = (existing_dict.get("title") or "")[:100]
                        new_title = (data.get("title") or "")[:100]
                        changes.append(("title_bullets", "title_or_bullets_changed",
                                        f"title={prev_title}", f"title={new_title}"))

                    # 写入变动记录
                    for change_type, detail, prev_val, new_val in changes:
                        await self._db.execute(
                            "INSERT INTO asin_changes (asin, batch_id, change_type, change_detail, prev_value, new_value) VALUES (?, ?, ?, ?, ?, ?)",
                            (asin, batch_id, change_type, detail, prev_val, new_val)
                        )

                    # 更新主表
                    update_fields = []
                    update_values = []
                    for f in ASIN_DATA_FIELDS:
                        if f == "asin":
                            continue
                        val = data.get(f)
                        if val is not None:
                            update_fields.append(f"{f} = ?")
                            update_values.append(val)

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
                        if f == "asin":
                            continue
                        val = data.get(f)
                        if val is not None:
                            insert_fields.append(f)
                            insert_values.append(val)

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
                            (asin, batch_id)
                        )

                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        return True

    async def save_results_batch(self, results: List[dict], batch_id: int = None) -> int:
        """批量保存结果"""
        saved = 0
        for data in results:
            if await self.save_result(data, batch_id):
                saved += 1
        return saved

    # ==================== 查询操作（keyset 分页）====================

    async def get_results(self, batch_id: int = None, cursor_id: int = None,
                          limit: int = 50, search: str = None,
                          change_filter: str = "all",
                          direction: str = "next") -> Dict:
        """
        获取结果列表（keyset 分页）
        change_filter: all / price_stock / title_bullets / new
        direction: next (向后翻页) / prev (向前翻页)
        返回: {"items": [...], "has_more": bool, "next_cursor": int, "prev_cursor": int, "total": int}
        """
        where_parts = []
        params = []
        join_parts = []
        count_join_parts = []

        # 批次筛选 - 通过 batch_asins JOIN（核心优化）
        if batch_id:
            join_parts.append("JOIN batch_asins ba ON ba.asin = d.asin AND ba.batch_id = ?")
            count_join_parts.append("JOIN batch_asins ba ON ba.asin = d.asin AND ba.batch_id = ?")
            params.append(batch_id)

        # 搜索
        if search:
            where_parts.append("(d.asin LIKE ? OR d.title LIKE ? OR d.brand LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        # 变动筛选 - 通过 asin_changes JOIN（核心优化）
        if change_filter == "price_stock":
            join_parts.append(
                "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'price_stock'"
                + (" AND batch_id = ?" if batch_id else "")
                + ") ac ON ac.asin = d.asin"
            )
            count_join_parts.append(
                "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'price_stock'"
                + (" AND batch_id = ?" if batch_id else "")
                + ") ac ON ac.asin = d.asin"
            )
            if batch_id:
                params.append(batch_id)
        elif change_filter == "title_bullets":
            join_parts.append(
                "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'title_bullets'"
                + (" AND batch_id = ?" if batch_id else "")
                + ") ac ON ac.asin = d.asin"
            )
            count_join_parts.append(
                "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'title_bullets'"
                + (" AND batch_id = ?" if batch_id else "")
                + ") ac ON ac.asin = d.asin"
            )
            if batch_id:
                params.append(batch_id)
        elif change_filter == "new":
            if batch_id:
                join_parts.append(
                    "JOIN batch_asins ba2 ON ba2.asin = d.asin AND ba2.batch_id = ? AND ba2.is_new = 1"
                )
                count_join_parts.append(
                    "JOIN batch_asins ba2 ON ba2.asin = d.asin AND ba2.batch_id = ? AND ba2.is_new = 1"
                )
                params.append(batch_id)
            else:
                join_parts.append(
                    "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'new') ac ON ac.asin = d.asin"
                )
                count_join_parts.append(
                    "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'new') ac ON ac.asin = d.asin"
                )

        # 构建 count 查询参数（不含 cursor）
        count_params = list(params)

        # keyset 分页
        if cursor_id is not None:
            if direction == "next":
                where_parts.append("d.id < ?")
            else:
                where_parts.append("d.id > ?")
            params.append(cursor_id)

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        join_clause = " ".join(join_parts) if join_parts else ""
        count_join_clause = " ".join(count_join_parts) if count_join_parts else ""

        order = "DESC" if direction == "next" else "ASC"

        # 查询数据
        sql = f"""
            SELECT d.* FROM asin_data d
            {join_clause}
            WHERE {where_clause}
            ORDER BY d.id {order}
            LIMIT ?
        """
        params.append(limit + 1)  # 多取一条判断 has_more

        async with self._db.execute(sql, params) as c:
            rows = await c.fetchall()

        items = [dict(r) for r in rows]
        has_more = len(items) > limit
        if has_more:
            items = items[:limit]

        if direction == "prev":
            items.reverse()

        # 查询总数
        count_where = " AND ".join(
            [p for p in (where_parts[:-1] if cursor_id is not None else where_parts)]
        ) if where_parts else "1=1"
        # 移除 cursor 条件
        if cursor_id is not None and where_parts:
            count_where_parts = [p for p in where_parts if "d.id" not in p]
            count_where = " AND ".join(count_where_parts) if count_where_parts else "1=1"

        count_sql = f"SELECT COUNT(*) FROM asin_data d {count_join_clause} WHERE {count_where}"
        async with self._db.execute(count_sql, count_params) as c:
            total = (await c.fetchone())[0]

        next_cursor = items[-1]["id"] if items else None
        prev_cursor = items[0]["id"] if items else None

        return {
            "items": items,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "prev_cursor": prev_cursor,
            "total": total,
        }

    async def get_result_by_asin(self, asin: str) -> Optional[Dict]:
        async with self._db.execute("SELECT * FROM asin_data WHERE asin = ?", (asin,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

    async def get_asin_changes(self, asin: str) -> List[Dict]:
        """获取 ASIN 的变动历史"""
        async with self._db.execute(
            "SELECT * FROM asin_changes WHERE asin = ? ORDER BY id DESC", (asin,)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    # ==================== 截图操作 ====================

    async def get_pending_screenshots(self, batch_id: int, limit: int = 50) -> List[Dict]:
        async with self._db.execute(
            "SELECT * FROM screenshots WHERE batch_id = ? AND status = 'pending' LIMIT ?",
            (batch_id, limit)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    async def update_screenshot_status(self, asin: str, batch_id: int, status: str,
                                       file_path: str = None, error: str = None):
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        async with self._write_lock:
            await self._db.execute("BEGIN")
            if status == "done" and file_path:
                await self._db.execute(
                    "UPDATE screenshots SET status=?, file_path=?, updated_at=? WHERE asin=? AND batch_id=?",
                    (status, file_path, now, asin, batch_id)
                )
                await self._db.execute(
                    "UPDATE asin_data SET screenshot_path=? WHERE asin=?",
                    (file_path, asin)
                )
            elif status == "failed":
                await self._db.execute(
                    """UPDATE screenshots SET status='pending', retry_count=retry_count+1,
                       error_detail=?, updated_at=? WHERE asin=? AND batch_id=? AND retry_count < 3""",
                    (error, now, asin, batch_id)
                )
                await self._db.execute(
                    "UPDATE screenshots SET status='failed', updated_at=? WHERE asin=? AND batch_id=? AND retry_count >= 3",
                    (now, asin, batch_id)
                )
            else:
                await self._db.execute(
                    "UPDATE screenshots SET status=?, updated_at=? WHERE asin=? AND batch_id=?",
                    (status, now, asin, batch_id)
                )
            await self._db.execute("COMMIT")

    async def get_screenshot_progress(self, batch_id: int) -> Dict:
        sql = "SELECT status, COUNT(*) as cnt FROM screenshots WHERE batch_id = ? GROUP BY status"
        stats = {"pending": 0, "processing": 0, "done": 0, "failed": 0}
        async with self._db.execute(sql, (batch_id,)) as c:
            async for row in c:
                stats[row["status"]] = row["cnt"]
        stats["total"] = sum(stats.values())
        return stats

    # ==================== 导出操作 ====================

    async def iter_results(self, batch_id: int = None, batch_size: int = 500):
        """流式迭代结果（使用 keyset 分页避免大 OFFSET）"""
        last_id = 0
        while True:
            if batch_id:
                sql = """
                    SELECT d.* FROM asin_data d
                    JOIN batch_asins ba ON ba.asin = d.asin AND ba.batch_id = ?
                    WHERE d.id > ?
                    ORDER BY d.id ASC
                    LIMIT ?
                """
                params = (batch_id, last_id, batch_size)
            else:
                sql = "SELECT * FROM asin_data WHERE id > ? ORDER BY id ASC LIMIT ?"
                params = (last_id, batch_size)

            async with self._db.execute(sql, params) as c:
                rows = await c.fetchall()
                if not rows:
                    break
                for row in rows:
                    d = dict(row)
                    last_id = d["id"]
                    yield d

    # ==================== 统计 ====================

    async def get_total_asins(self) -> int:
        async with self._db.execute("SELECT COUNT(*) FROM asin_data") as c:
            return (await c.fetchone())[0]

    async def get_all_asins(self) -> List[str]:
        """获取所有已知 ASIN（用于自动采集）"""
        result = []
        async with self._db.execute("SELECT asin FROM asin_data ORDER BY id") as c:
            async for row in c:
                result.append(row["asin"])
        return result

    async def get_change_stats(self, batch_id: int = None) -> Dict:
        """获取变动统计"""
        batch_filter = "WHERE batch_id = ?" if batch_id else ""
        params = (batch_id,) if batch_id else ()

        stats = {}
        sql = f"SELECT change_type, COUNT(DISTINCT asin) as cnt FROM asin_changes {batch_filter} GROUP BY change_type"
        async with self._db.execute(sql, params) as c:
            async for row in c:
                stats[row["change_type"]] = row["cnt"]
        return stats
