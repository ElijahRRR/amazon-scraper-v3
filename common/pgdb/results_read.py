"""common/pgdb/results_read.py —— 结果读取 / 搜索 / 分页 / 流式导出。

OWNS（7 个方法）:
    get_results        database.py:2120   ← 返回值**就是** HTTP 响应体（app.py:1758）
    get_result_by_asin database.py:2272
    get_asin_changes   database.py:2282
    iter_results       database.py:2344   ← **async generator**，不是 coroutine
    get_total_asins    database.py:2464
    get_all_asins      database.py:2468
    get_change_stats   database.py:2476

依赖别人:
    media.py -> _hydrate_screenshot_paths(items, batch_id)  （原地改 items）

--------------------------------------------------------------------------
一、搜索：FTS5 没了，换 ascii_lower + LIKE（**不是** ILIKE）
--------------------------------------------------------------------------
词条解析逐字照抄 database.py:2174-2179：
    search = str(search)[:500]
    terms  = [t.strip()[:100] for t in search.split(",") if t.strip()][:10]
截断顺序（先 strip 后截）、500/100/10 三个常数、逗号是唯一分隔符、多词是 OR、
terms 为空则**完全不加 where 子句**（静默返回全量）——全部保持。

谓词形状：
    (ascii_lower(d.asin) LIKE ascii_lower(?) OR
     ascii_lower(d.title) LIKE ascii_lower(?) OR
     ascii_lower(d.brand) LIKE ascii_lower(?))
* 用 ascii_lower 而不是 ILIKE：实测 39 个探针 × 5 种写法，ILIKE 有 9 处与
  SQLite 不一致（根因是非 ASCII：SQLite 只折叠 ASCII，'CAFÉ CREME' 不匹配
  '%café%'），ascii_lower 是 0 处。ILIKE 还依赖 collation。
* 反斜杠：SQLite 的 LIKE **没有**转义字符，PG 默认是反斜杠。构造模式时把
  反斜杠加倍：``"%" + t.replace("\\\\", "\\\\\\\\") + "%"``。
  实测：不处理的话 'back\\slash' 在 PG 下匹配不到，'\\\\' 反而匹配得到。
* ``%`` 和 ``_`` **不要**转义：用户输入里的通配符现在就是生效的（'Gol%rand'
  是模糊匹配），两个引擎的元字符一样，这个 bug 免费保留。
* 也可以直接写 ``d.title LIKE ?`` —— pool.translate_sql 会自动改写成上面的
  形状。但显式写出来更利于对照表达式 GIN 索引（schema.py 建的就是
  ``public.ascii_lower(col) public.gin_trgm_ops``，表达式必须逐字一致）。
* 查询形状用**扁平 OR**，不要照搬 ``d.id IN (SELECT ... UNION ...)``。
  200k 行实测：非选择性词条上扁平 OR 是 2.2ms，id-IN-UNION 是 536ms
  （HashAggregate 整个 id 集）。

--------------------------------------------------------------------------
二、⚠ COUNT 查询的 bug：必须**刻意**复现（决策 D-8）
--------------------------------------------------------------------------
database.py:2249-2255 在有 cursor 时会把所有文本里含 ``"d.id"`` 的 where 片段
从 count 查询里剔掉——本意是剔掉 keyset 谓词 ``d.id < ?``，但 FTS 快路径的
谓词字面上就以 ``d.id IN (`` 开头，于是被一并剔掉，而 count_params
（第 2207 行就快照好了）里仍带着搜索参数 → 参数个数对不上 → 500。

即：今天 ``/api/results?search=GoldenBrand&cursor=3`` 是 500，
而 ``search=Go&cursor=3``（<3 字符走慢路径，谓词文本里没有 "d.id"）是 200。
黄金场景没覆盖这个组合（scenario.py:214-224 翻页不带 search、搜索不带 cursor）。

**决策：复现这个 bug。** 做法是给 >=3 字符那条分支的谓词套一层带标记的壳：
    "(d.id IS NOT NULL AND (" + flat_or + "))"
行集不变、GIN 计划不变、``"d.id" not in p`` 的过滤照样命中，asyncpg 会抛
"the server expects 0 arguments for this query, N was passed"，
FastAPI 渲染成同样的 500。
→ 因此 ``any(len(t) < 3 for t in terms)`` 这个分支判断**必须保留**，
  它现在唯一的作用就是决定谓词要不要带这个标记。
→ ``[p for p in where_parts if "d.id" not in p]` 这个过滤逐字照抄，别"修好"。
Phase 1.5 会连同 COUNT(*) 重构一起有意修掉。

--------------------------------------------------------------------------
三、分页 / 排序
--------------------------------------------------------------------------
* ``ORDER BY d.id DESC``（next）/ ``ASC``（prev），``LIMIT ?`` 传 limit+1，
  has_more = len > limit，prev 方向在 **Python 里 reverse**，
  next_cursor = items[-1]['id']，prev_cursor = items[0]['id']。全部照抄。
  d.id 是 PK 不会有 NULL，也不会有并列，不需要 tiebreaker。
* iter_results 的 batch 路径用 ``ba.asin > ?`` + ``ORDER BY ba.asin ASC`` 做
  keyset。导出的 CSV 是**逐行**比对的，所以行序是契约。
  batch_asins.asin 已声明 COLLATE "C"，字节序 = SQLite BINARY。
* 凡是排序键可空（screenshots/tasks 的 updated_at 等）：
  DESC 补 ``NULLS LAST``、ASC 补 ``NULLS FIRST``，PG 默认与 SQLite 正好相反。

--------------------------------------------------------------------------
四、其它
--------------------------------------------------------------------------
* iter_results 必须仍然是 ``async def`` + ``yield``（调用方 ``async for``）。
  两条分支（batch / 非 batch）的 SQL 文本要**保持分开**：游标种子在 batch
  路径是 ``""``（text），非 batch 路径是 ``0``（bigint），合并成一条会撞上
  asyncpg 的参数类型推断。
  batch 路径额外加 11 个 ``batch_*`` 别名键，并且会用 ba.asin **覆盖**
  ``d["asin"]``。照抄。
* get_results / get_result_by_asin 末尾调 ``self._hydrate_screenshot_paths``
  （media.py 提供，原地改 items）。
* ``screenshot_path: null`` 是基线里的值。**不要**加 ``COALESCE(col,'')``
  之类的"防御"，那会把 null 变成 ""。
* get_change_stats 返回稀疏 dict（只有出现过的 change_type 才有 key）。
* 这些多语句读路径本来就每条语句一个快照（SQLite 读池如此），
  **不要**加 REPEATABLE READ。
* 导出会长时间占着一条池连接。config.PG_POOL_MAX 要留够余量，
  别让导出饿死 pull_tasks。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from common.pgdb._shared import (  # noqa: F401
    ASIN_DATA_FIELDS,
    _ASIN_DATA_COLUMN_SET,
    _normalize_screenshot_path,
)
from common.pgdb.pool import as_int, text_affinity


# 一个 term 的三列 OR 谓词。写成 ascii_lower(...) LIKE ascii_lower(?) 而不是
# ILIKE：见模块头注释与 OWNERSHIP.md D-5。表达式必须与 schema.py 建的
# 表达式 GIN 索引（public.ascii_lower(col) public.gin_trgm_ops）对得上。
_TERM_OR = ("(ascii_lower(d.asin) LIKE ascii_lower(?)"
            " OR ascii_lower(d.title) LIKE ascii_lower(?)"
            " OR ascii_lower(d.brand) LIKE ascii_lower(?))")


def _like_pattern(t: str) -> str:
    """``%term%``，并把反斜杠加倍。

    SQLite 的 LIKE **没有**转义字符，PG 的 LIKE 默认用反斜杠转义。不加倍的话
    ``'back\\slash'`` 在 PG 下匹配不到（``\\s`` 被吃成字面量 ``s``），而
    ``'\\\\'`` 反而能匹配到——两个方向都错。
    ``%`` 和 ``_`` 故意**不**转义：用户输入里的通配符今天就是生效的
    （``Gol%rand`` 是模糊匹配），两个引擎的元字符一样，这个行为免费保留。
    """
    return "%" + str(t).replace("\\", "\\\\") + "%"


class ResultsReadMixin:
    """只定义方法，绝不定义 __init__。"""

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
        join_parts = []
        count_join_parts = []
        join_params: list = []
        where_parts = []
        where_params: list = []

        # 批次筛选 - 通过 batch_asins JOIN
        if batch_id:
            join_parts.append("JOIN batch_asins ba ON ba.asin = d.asin AND ba.batch_id = ?")
            count_join_parts.append("JOIN batch_asins ba ON ba.asin = d.asin AND ba.batch_id = ?")
            join_params.append(as_int(batch_id))

        # 变动筛选 - 通过 asin_changes JOIN（JOIN params 在 WHERE params 之前绑定）
        if change_filter == "price_stock":
            sub = "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'price_stock'"
            if batch_id:
                sub += " AND batch_id = ?"
                join_params.append(as_int(batch_id))
            sub += ") ac ON ac.asin = d.asin"
            join_parts.append(sub)
            count_join_parts.append(sub)
        elif change_filter == "title_bullets":
            sub = "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'title_bullets'"
            if batch_id:
                sub += " AND batch_id = ?"
                join_params.append(as_int(batch_id))
            sub += ") ac ON ac.asin = d.asin"
            join_parts.append(sub)
            count_join_parts.append(sub)
        elif change_filter == "new":
            if batch_id:
                sub = "JOIN batch_asins ba2 ON ba2.asin = d.asin AND ba2.batch_id = ? AND ba2.is_new = 1"
                join_params.append(as_int(batch_id))
                join_parts.append(sub)
                count_join_parts.append(sub)
            else:
                sub = "JOIN (SELECT DISTINCT asin FROM asin_changes WHERE change_type = 'new') ac ON ac.asin = d.asin"
                join_parts.append(sub)
                count_join_parts.append(sub)

        # 搜索（支持逗号分隔的批量搜索）—— 限长防 DoS
        # PG 侧没有 FTS5：两条分支产生**同一个**扁平 OR 谓词（实测 SQLite 的
        # 快/慢路径在 37 个非空探针上行集完全一致），由 pg_trgm 的表达式 GIN
        # 索引加速；扁平 OR 而不是 d.id IN (... UNION ...)，200k 行实测非选择性
        # 词条 2.2ms vs 536ms（后者要 HashAggregate 整个 id 集）。
        #
        # ⚠ ``any(len(t) < 3 ...)`` 这个分支判断必须保留：它现在唯一的作用是
        # 决定谓词文本里要不要带 "d.id" 这个标记，从而**刻意复现** count 查询
        # 那个崩溃（决策 D-8，详见模块头注释第二节）。
        if search:
            # 单个请求最多 500 字符、最多 10 个关键词，每个关键词截断到 100 字符
            search = str(search)[:500]
            terms = [t.strip()[:100] for t in search.split(",") if t.strip()][:10]
            if terms:
                or_clauses = []
                for t in terms:
                    or_clauses.append(_TERM_OR)
                    like_pattern = _like_pattern(t)
                    where_params.extend([like_pattern, like_pattern, like_pattern])
                flat_or = f"({' OR '.join(or_clauses)})"
                if any(len(t) < 3 for t in terms):
                    # 慢路径：含 < 3 字符的 term。谓词文本里没有 "d.id"，
                    # 所以下面的 count 过滤不会把它剔掉 —— 与 SQLite 一致。
                    where_parts.append(flat_or)
                else:
                    # 快路径：SQLite 这里是 ``d.id IN (SELECT rowid FROM
                    # asin_data_fts ... UNION ...)``。行集与扁平 OR 相同，但
                    # 文本里那个 "d.id" 是 count 过滤的触发条件。套一层恒真的
                    # ``d.id IS NOT NULL`` 把标记还原：行集不变、GIN 计划不变、
                    # ``"d.id" not in p`` 照样命中 -> 参数个数对不上 -> 500。
                    where_parts.append(f"(d.id IS NOT NULL AND {flat_or})")

        # 构建 count 查询参数（join_params + where_params，不含 cursor）
        count_params = join_params + where_params

        # keyset 分页
        if cursor_id is not None:
            if direction == "next":
                where_parts.append("d.id < ?")
            else:
                where_parts.append("d.id > ?")
            where_params.append(as_int(cursor_id))

        params = join_params + where_params

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        join_clause = " ".join(join_parts) if join_parts else ""
        count_join_clause = " ".join(count_join_parts) if count_join_parts else ""

        order = "DESC" if direction == "next" else "ASC"

        # 查询数据（d.id 是 PK，非空且无并列，不需要 NULLS 位置和 tiebreaker）
        sql = f"""
            SELECT d.* FROM asin_data d
            {join_clause}
            WHERE {where_clause}
            ORDER BY d.id {order}
            LIMIT ?
        """
        params.append(limit + 1)  # 多取一条判断 has_more

        async with self.read() as rc, rc.execute(sql, params) as c:
            rows = await c.fetchall()

        items = [dict(r) for r in rows]
        has_more = len(items) > limit
        if has_more:
            items = items[:limit]

        if direction == "prev":
            items.reverse()

        # 注意：读连接已经归还，_hydrate 内部还要再借一条（避免池内嵌套借用）
        await self._hydrate_screenshot_paths(items, batch_id)

        # 查询总数
        count_where = " AND ".join(
            [p for p in (where_parts[:-1] if cursor_id is not None else where_parts)]
        ) if where_parts else "1=1"
        # 移除 cursor 条件
        if cursor_id is not None and where_parts:
            count_where_parts = [p for p in where_parts if "d.id" not in p]
            count_where = " AND ".join(count_where_parts) if count_where_parts else "1=1"

        count_sql = f"SELECT COUNT(*) FROM asin_data d {count_join_clause} WHERE {count_where}"
        async with self.read() as rc, rc.execute(count_sql, count_params) as c:
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
        # 注意：先释放读连接再 _hydrate（其内部还要借读连接），避免池内嵌套借用导致死锁
        async with self.read() as rc, rc.execute(
            "SELECT * FROM asin_data WHERE asin = ?", (text_affinity(asin),)
        ) as c:
            row = await c.fetchone()
        if not row:
            return None
        item = dict(row)
        await self._hydrate_screenshot_paths([item])
        return item

    async def get_asin_changes(self, asin: str) -> List[Dict]:
        """获取 ASIN 的变动历史"""
        async with self.read() as rc, rc.execute(
            "SELECT * FROM asin_changes WHERE asin = ? ORDER BY id DESC",
            (text_affinity(asin),)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    # ==================== 导出操作 ====================

    async def iter_results(self, batch_id: int = None, change_filter: str = "all",
                           batch_size: int = 500, columns: Optional[List[str]] = None):
        """流式迭代结果（keyset 分页，支持 batch_id + change_filter）。
        整个导出（可能数千次 LIMIT 查询、持续数分钟）借用同一条池连接，
        全程不触碰写连接，导出再大也不会拖慢 worker 拉取/上传。
        （反过来说：导出期间那条池连接一直被占着，PG_POOL_MAX 要留够余量。）

        columns: 仅投影这些 asin_data 列（导出实际需要的字段）；None 时回退 d.*。

        批次路径游标用 ba.asin，走 PK 索引 (batch_id, asin)；batch_asins.asin 声明了
        COLLATE "C"，字节序与 SQLite 的 BINARY 一致，所以导出行序逐行可比。
        """
        # 收窄投影：把导出需要的 asin_data 列拼成 "d.col" 列表；
        # 防注入起见与已知列集取交集（列名来自 EXPORTABLE_FIELDS 白名单，这里再兜一层）。
        proj_cols = None
        if columns:
            proj_cols = [c for c in dict.fromkeys(columns) if c in _ASIN_DATA_COLUMN_SET] or None

        # 游标：批次路径按 asin（TEXT），非批次路径按 asin_data.id（bigint）。
        # 两条分支的 SQL 文本必须保持分开——同一条文本上游标类型不一致会撞
        # asyncpg 的参数类型推断。
        cursor: Any = "" if batch_id else 0
        async with self.read() as rc:
            while True:
                if batch_id:
                    d_select = ", ".join(f"d.{c}" for c in proj_cols) if proj_cols else "d.*"
                    joins = [
                        "LEFT JOIN asin_data d ON d.asin = ba.asin",
                        "LEFT JOIN tasks t ON t.batch_id = ba.batch_id AND t.asin = ba.asin",
                    ]
                    join_params: list = []
                    where = ["ba.batch_id = ?", "ba.asin > ?"]
                    where_params: list = [as_int(batch_id), cursor]

                    if change_filter == "price_stock":
                        joins.append(
                            "JOIN (SELECT DISTINCT asin FROM asin_changes "
                            "WHERE change_type='price_stock' AND batch_id=?) ac ON ac.asin = ba.asin"
                        )
                        join_params.append(as_int(batch_id))
                    elif change_filter == "title_bullets":
                        joins.append(
                            "JOIN (SELECT DISTINCT asin FROM asin_changes "
                            "WHERE change_type='title_bullets' AND batch_id=?) ac ON ac.asin = ba.asin"
                        )
                        join_params.append(as_int(batch_id))
                    elif change_filter == "new":
                        where.append("ba.is_new = 1")

                    params = join_params + where_params + [batch_size]
                    sql = f"""
                        SELECT {d_select},
                               ba.asin AS batch_requested_asin,
                               ba.is_new AS batch_is_new,
                               t.status AS batch_task_status,
                               t.error_type AS batch_error_type,
                               t.error_detail AS batch_error_detail,
                               t.retry_count AS batch_retry_count,
                               t.auto_retry_count AS batch_auto_retry_count,
                               t.worker_id AS batch_worker_id,
                               t.updated_at AS batch_task_updated_at,
                               d.updated_at AS batch_asin_data_updated_at,
                               CASE WHEN d.asin IS NULL THEN 0 ELSE 1 END AS batch_has_asin_data
                        FROM batch_asins ba
                        {' '.join(joins)}
                        WHERE {' AND '.join(where)}
                        ORDER BY ba.asin ASC
                        LIMIT ?
                    """

                    async with rc.execute(sql, params) as c:
                        rows = await c.fetchall()
                    if not rows:
                        break
                    for row in rows:
                        d = dict(row)
                        cursor = d["batch_requested_asin"]
                        d["asin"] = d.get("batch_requested_asin") or d.get("asin")
                        yield d
                    continue

                # 非批次路径（导出全部）：主键 id 游标，本就最优。
                # 收窄投影时须显式带上 d.id 作游标。
                if proj_cols:
                    d_select = "d.id, " + ", ".join(f"d.{c}" for c in proj_cols)
                else:
                    d_select = "d.*"
                joins = []
                join_params = []
                where = ["d.id > ?"]
                where_params = [cursor]

                if change_filter == "price_stock":
                    joins.append("JOIN (SELECT DISTINCT asin FROM asin_changes "
                                 "WHERE change_type='price_stock') ac ON ac.asin = d.asin")
                elif change_filter == "title_bullets":
                    joins.append("JOIN (SELECT DISTINCT asin FROM asin_changes "
                                 "WHERE change_type='title_bullets') ac ON ac.asin = d.asin")
                elif change_filter == "new":
                    joins.append("JOIN (SELECT DISTINCT asin FROM asin_changes "
                                 "WHERE change_type='new') ac ON ac.asin = d.asin")

                join_clause = " ".join(joins)
                where_clause = " AND ".join(where)
                params = join_params + where_params + [batch_size]

                sql = f"SELECT {d_select} FROM asin_data d {join_clause} WHERE {where_clause} ORDER BY d.id ASC LIMIT ?"

                async with rc.execute(sql, params) as c:
                    rows = await c.fetchall()
                if not rows:
                    break
                for row in rows:
                    d = dict(row)
                    cursor = d["id"]
                    yield d

    # ==================== 统计 ====================

    async def get_total_asins(self) -> int:
        async with self.read() as rc, rc.execute("SELECT COUNT(*) FROM asin_data") as c:
            return (await c.fetchone())[0]

    async def get_all_asins(self) -> List[str]:
        """获取所有已知 ASIN（用于自动采集）"""
        result = []
        async with self.read() as rc, rc.execute("SELECT asin FROM asin_data ORDER BY id") as c:
            async for row in c:
                result.append(row["asin"])
        return result

    async def get_change_stats(self, batch_id: int = None) -> Dict:
        """获取变动统计"""
        batch_filter = "WHERE batch_id = ?" if batch_id else ""
        params = (as_int(batch_id),) if batch_id else ()

        stats = {}
        sql = f"SELECT change_type, COUNT(DISTINCT asin) as cnt FROM asin_changes {batch_filter} GROUP BY change_type"
        async with self.read() as rc, rc.execute(sql, params) as c:
            async for row in c:
                stats[row["change_type"]] = row["cnt"]
        return stats
