"""common/pgdb/retention.py —— Phase 6：保留期（DROP 分区）+ ack 闩锁。

计划 `.agent/pg_migration_plan.md` §Phase 6 与审计 `.agent/catalog_sync_audit.md`
§3.6 是本文件的规格书。对外那一半（`/status` 的字段、`/ack-prune` 端点）在
`server/api/sync.py` 与 `docs/sync_contract.md`。

    floor = max(硬下界, min(age_floor, ack_floor))

「硬下界」= 剩余磁盘 / 分区数 / 行数三条应急闸门里最狠的那一条。
**不是天数驱动**：天数只是 age_floor 的输入之一，真正决定裁不裁的是磁盘与容量。

------------------------------------------------------------------------
承重约束（每一条都对应一次事故推演，改之前先读完）
------------------------------------------------------------------------

1. **只 DROP 整个分区，绝不 DELETE 行。**
   按 seq range 分区的全部意义就在这里：DROP 是瞬时的、不产生死元组、不需要
   VACUUM、不写 WAL 行。一旦有人在这里写 ``DELETE FROM scrape_events``，
   2000 万行的删除会把 autovacuum 拖垮、把磁盘**先撑大再**回收 —— 正好在磁盘
   已经告急的时候。``_drop_partition()`` 是本文件唯一的删除动作。

2. **只裁前缀，绝不裁中间。** 分区按 lo 排序后从最老的开始，遇到第一个不可裁的
   立刻 break。跳过一个不可裁分区去裁它后面的，会在流中间挖一个洞，而
   ``/records`` 的 409 判据是 ``after_seq + 1 < min(seq)`` —— 洞在 min 之上，
   守卫看不见，消费者会拿到一个**静默跳过了整段**的 200。这是本阶段唯一能造出
   静默丢数据的写法。

3. **``hi <= 下一个待发 seq`` 才准裁。** 判「这个分区还会不会收到新行」只能靠
   序列水位，不能靠 max(seq)：一个当下为空的头部分区照样是写入目标，裁掉它
   之后 relay 立刻撞上 ``no partition of relation ... found for row``，整条流
   停摆。序列只增不减，所以 ``hi <= high_water + 1`` 是「未来不可能有行落进来」
   的充要条件。

4. **``ack_seq`` 的「没有」必须是 ``None``，绝不是 ``0``。** 见 §P6-2 与
   ``read_ack_floor()`` 的注释 —— 这是本阶段被点名的头号陷阱，这里用三层把它
   变成结构上不可能，而不是「今天写对了」。

5. **强制裁剪是持久闩锁。** 写 ``sync_meta.forced_prune_log``，`/status` 一直
   返回直到消费者逐条确认。消费者宕机正是触发前提，所以「响应里放个瞬时布尔」
   等于没有。

6. **``min_available_seq`` 永远现算。** 本文件不写、不读任何形如
   ``min_available_seq`` 的缓存键，并且每次 pass 都会主动检查库里没有人偷偷加
   过（``_assert_no_cached_bounds``）。缓存它就是 Phase 3 那个 409 守卫失效的
   全部方法。

7. **裁剪必须让路给读。** ``DROP TABLE`` 要 ACCESS EXCLUSIVE，它会**排在**读者
   的 ACCESS SHARE 后面 —— 而 PG 的锁队列是 FIFO，一个排队中的 ACCESS EXCLUSIVE
   会把它后面所有新读者一起堵住。所以每次 DROP 都带 ``SET LOCAL lock_timeout``：
   拿不到就放弃，下一轮再来。宁可晚 20 分钟裁，不可把同步流堵死。

8. **专用连接。** 不借池连接（读侧背压面）、更不碰那条唯一写连接（D-2）：
   在写连接上开事务会把某个 accept_results_batch 的提交拖进 DDL 事务里。
   与 relay 同一口径：自己开一条短命连接，用完就关。

------------------------------------------------------------------------
运维旋钮（全部现读环境变量，改完重启进程即生效）
------------------------------------------------------------------------

===============================  ==========  ====================================
环境变量                          默认         说明
===============================  ==========  ====================================
``SYNC_RETENTION_ENABLED``       ``1``       总开关。设 0 = 只观测不裁
``SYNC_RETENTION_INTERVAL_S``    ``1200``    两轮之间的最小间隔（20 min）
``SYNC_RETENTION_AGE_DAYS``      ``30``      时间下界。计划 §0.2：80GB 盘下的建议值
``SYNC_DISK_FLOOR_BYTES``        ``8 GiB``   剩余磁盘低于它 → 越过 ack 强裁 + 记闩锁
``SYNC_DISK_TARGET_BYTES``       ``floor×1.25``  迟滞上沿，裁到这里才停
``SYNC_MAX_RETAINED_PARTITIONS`` ``6``       非未来分区数上限（0 = 不限）
``SYNC_MAX_EVENT_ROWS``          ``6000万``   估算行数上限（0 = 不限）
``SYNC_ACK_SLACK_SEQ``           ``1000``    下界与 ack_seq 之间保底留的余量。
                                             **不得小于契约的 OVERLAP=200**，
                                             小了会被代码抬回 200。
                                             `/status.max_safe_overlap` 回的就是它
``SYNC_RETENTION_LOCK_TIMEOUT``  ``5s``      单次 DROP 的锁等待上限
``PG_RETENTION_LOCK_KEY``        ``…5632``   保留期单例锁（与 relay 的锁分开）
===============================  ==========  ====================================

调大 ``SYNC_ACK_SLACK_SEQ`` 是**对消费者友好**的方向（它可以把 OVERLAP 开得更大），
代价是保留期晚一点才够得着那批数据。调小到 200 以下没有意义 —— 契约已经把
``OVERLAP=200`` 写给了对面。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

from common import config
from common.core.timeutil import iso_utc as _iso

logger = logging.getLogger(__name__)


# ============================================================
# 常量与配置
# ============================================================

#: 契约 §7 建议消费侧 ``OVERLAP = 200``（重叠回拉，宁可重复）。
#: 服务端据此**保底**：保留期下界永远比 ack_seq 低至少这么多，否则一个完全守规矩
#: 的消费者每轮都会拿到 409（Phase 3 verify2 Finding 2 实测过）。
CONSUMER_OVERLAP = 200

#: 保留期单例锁。与 relay 的 0x…31 相邻但不同 —— 两把锁互不影响，
#: 一个实例可以只跑 relay 或只跑保留期。
RETENTION_LOCK_KEY = int(
    os.environ.get("PG_RETENTION_LOCK_KEY", "0x5343524150455632"), 0)

#: 强制裁剪日志的条数上限。超出时**先丢已确认的**，未确认的一条都不丢
#: （闩锁的意义就在于未确认那部分不会自己消失）。
FORCED_PRUNE_LOG_CAP = 200

#: 绝不允许出现在 sync_meta 里的键。任何一个存在都意味着有人把边界缓存了下来，
#: 而那正是保留期竞态打穿 409 守卫的唯一路径（计划 §Phase 6 第 4 条）。
FORBIDDEN_META_KEYS = frozenset({
    "min_available_seq", "min_seq", "available_min_seq",
    "retention_floor", "retention_min_seq", "cached_min_seq",
})

#: sync_meta 上那条把「ack_seq = 0」从库层面挡掉的 CHECK 的名字。
ACK_SEQ_CHECK = "sync_meta_ack_seq_is_never_zero"


class RetentionInvariantError(RuntimeError):
    """保留期的结构性不变式被破坏（不是运行故障，是代码/运维错误）。"""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip(), 0)
    except (TypeError, ValueError):
        logger.warning("环境变量 %s=%r 不是整数，用默认值 %s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning("环境变量 %s=%r 不是数字，用默认值 %s", name, raw, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


# ============================================================
# 下界代数 —— P6-2 的第一层
# ============================================================

def read_ack_floor(raw: Optional[str]) -> Optional[int]:
    """``sync_meta.ack_seq`` 的**唯一**解读入口。``None`` = 消费者没设下界。

    这个函数存在的全部理由是：本文件里不许出现
    ``int(meta.get("ack_seq") or 0)``。那一行就是计划点名的陷阱：

        floor = max(disk, min(age_floor, 0)) = max(disk, 0) = disk

    于是保留期看起来实现了、日志也正常、一行都不裁，直到磁盘满。

    三种输入都映射成「没有下界」：
      * 键不存在      —— 从来没有人 ack 过（全新库、消费者还没上线）
      * ``'0'``       —— 语义上等价于「持久副本里一条都没有」。计划 §5.4 对这个
                        状态的规定就是「按纯时间 + 磁盘执行」，与从未 ack 完全
                        一致，所以两者合流是**语义上正确**的，不是偷懒。
                        库层面还有一条 CHECK 挡着它被写进去（见
                        ``ensure_retention_schema``），这里是第二道。
      * 非数字        —— 手工 UPDATE 写坏了。绝不当 0，绝不抛。

    返回值恒 ``> 0`` 或 ``None``，没有第三种。
    """
    if raw is None:
        return None
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        logger.error("sync_meta.ack_seq=%r 不是整数，按「从未 ack」处理。"
                     "保留期将只按时间 + 磁盘执行。", raw)
        return None
    if v <= 0:
        return None
    return v


def combine_floors(*, age_floor: int, ack_floor: Optional[int],
                   hard_floor: int, ack_slack: int) -> int:
    """``floor = max(hard, min(age, ack - slack))``。P6-1 的公式，逐字。

    * ``ack_floor is None`` ⇒ 消费者不设下界 ⇒ 退化成 ``max(hard, age)``。
      这就是「全新库从不 ack，保留期照样裁」（计划 T9）。
    * ``ack_slack``：P6-5。契约 §7 要求消费者从 ``cursor - OVERLAP`` 回拉，
      下界正好压在 ack_seq 上的话，那次回拉每轮都落在窗口外 → 每轮一个假 409 +
      一次全量对账。所以服务端替消费者留出这段余量。
    * ``hard_floor`` 在 max 的外面 ⇒ 磁盘/容量应急**可以**越过 ack。越过就是
      真的丢数据，所以调用方必须为越过的部分写 forced_prune_log。

    这里的断言不是装饰：``ack_floor == 0`` 一旦出现，整个保留期就静默变成
    no-op，而 no-op 是没有症状的 —— 只有磁盘满那天才发作。宁可现在炸。
    """
    if ack_floor is not None and ack_floor <= 0:
        raise RetentionInvariantError(
            f"ack_floor={ack_floor!r}：0 与 None 是两件事。"
            f"0 会让 min(age_floor, 0) = 0，保留期从此一行不裁。"
            f"「没有 ack」必须用 None 表达，见 read_ack_floor()。")
    if ack_floor is None:
        soft = age_floor
    else:
        soft = min(age_floor, max(ack_floor - ack_slack, 0))
    return max(hard_floor, soft, 0)


# ============================================================
# 分区视图
# ============================================================

class PartitionView:
    """一个分区在保留期眼里的全部信息。纯数据，无行为。"""

    __slots__ = ("name", "lo", "hi", "index", "min_seq", "max_seq",
                 "est_rows", "size_bytes")

    def __init__(self, name: str, lo: Optional[int], hi: Optional[int],
                 index: int, min_seq: Optional[int], max_seq: Optional[int],
                 est_rows: int, size_bytes: int):
        self.name = name
        self.lo = lo
        self.hi = hi
        self.index = index
        self.min_seq = min_seq
        self.max_seq = max_seq
        self.est_rows = est_rows
        self.size_bytes = size_bytes

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "lo": self.lo, "hi": self.hi,
            "min_seq": self.min_seq, "max_seq": self.max_seq,
            "est_rows": self.est_rows, "size_bytes": self.size_bytes,
        }

    def is_future(self, next_seq: int) -> bool:
        """整段都在「下一个待发号」之上 —— relay 预建的未来分区。"""
        return self.lo is not None and self.lo >= next_seq

    def is_sealed(self, next_seq: int) -> bool:
        """未来不可能再有行落进来。承重约束 3。"""
        return self.hi is not None and self.hi <= next_seq

    def below(self, floor: int) -> bool:
        """整段都在 floor 之下（空分区算 True —— 没有任何行会因此丢失）。"""
        return self.max_seq is None or self.max_seq <= floor


_PARTITION_CATALOG_SQL = """
SELECT c.relname                                AS name,
       pg_get_expr(c.relpartbound, c.oid)       AS bound,
       GREATEST(c.reltuples, 0)::bigint         AS est_rows,
       pg_total_relation_size(c.oid)::bigint    AS size_bytes
  FROM pg_class c
  JOIN pg_inherits i  ON i.inhrelid = c.oid
  JOIN pg_class p     ON p.oid = i.inhparent
  JOIN pg_namespace n ON n.oid = p.relnamespace
 WHERE n.nspname = 'scraper' AND p.relname = 'scrape_events'
"""


# ============================================================
# Mixin
# ============================================================

class RetentionMixin:
    """只定义方法，绝不定义 __init__（MRO 约束见 common/pgdb/__init__.py）。"""

    # ---------------- 惰性状态 ----------------
    def _rt(self) -> Dict[str, Any]:
        st = getattr(self, "_retention_state", None)
        if st is None:
            st = {
                "lock": asyncio.Lock(),     # 进程内单飞
                "last_pass": None,          # 上一次 pass 的摘要（内存，非缓存边界）
                "last_pass_monotonic": None,
                "schema_ready": False,
                "counters": {
                    "passes": 0,
                    "partitions_dropped": 0,
                    "forced_drops": 0,
                    "lock_timeouts": 0,
                    "phantom_ack_repaired": 0,
                    "errors": 0,
                },
            }
            self._retention_state = st
        return st

    def _rt_bump(self, key: str, n: int = 1) -> None:
        c = self._rt()["counters"]
        c[key] = c.get(key, 0) + n

    # ---------------- 配置 ----------------
    def retention_config(self) -> Dict[str, Any]:
        """每次现读环境变量 —— 运维可以改完重启进程即生效，测试可以 monkeypatch。

        ``self._retention_overrides``（dict）优先于环境变量，只给测试用；
        生产不要设它。
        """
        cfg: Dict[str, Any] = {
            "enabled": _env_bool("SYNC_RETENTION_ENABLED", True),
            # 两次 pass 之间的最小间隔。审计 §3.6 的「每 10 tick（20 min）一次」。
            "interval_s": _env_float("SYNC_RETENTION_INTERVAL_S", 1200.0),
            # 时间下界：比这更老的**才**允许被裁。计划 §0.2：80GB 盘下建议 30 天。
            # 它是 min() 的一侧，不是驱动量 —— 驱动量是磁盘与容量。
            "age_days": _env_float("SYNC_RETENTION_AGE_DAYS", 30.0),
            # 剩余磁盘的应急下界。低于它就越过 ack 强裁并记闩锁。
            "disk_floor_bytes": _env_int("SYNC_DISK_FLOOR_BYTES", 8 * 1024 ** 3),
            # 迟滞：一旦触发，裁到这个水位才停，免得每轮都贴着门限抖动。
            "disk_target_bytes": _env_int("SYNC_DISK_TARGET_BYTES", 0),
            # 容量闸门（行数按 pg_class.reltuples 估，不 count(*)）。
            "max_retained_partitions": _env_int("SYNC_MAX_RETAINED_PARTITIONS", 6),
            "max_event_rows": _env_int("SYNC_MAX_EVENT_ROWS", 60_000_000),
            # P6-5：下界与 ack_seq 之间保底留出的 seq 余量，必须 ≥ 契约的 OVERLAP。
            "ack_slack_seq": _env_int("SYNC_ACK_SLACK_SEQ", 1000),
            # DROP 的锁等待上限。拿不到就放弃本轮（承重约束 7）。
            "lock_timeout": os.environ.get("SYNC_RETENTION_LOCK_TIMEOUT", "5s"),
            "disk_path": config.PROJECT_DIR,
        }
        cfg.update(getattr(self, "_retention_overrides", None) or {})
        # 余量的硬下限：契约把 OVERLAP=200 写给了消费者，服务端不得比它小。
        cfg["ack_slack_seq"] = max(int(cfg["ack_slack_seq"]), CONSUMER_OVERLAP)
        if not cfg["disk_target_bytes"]:
            cfg["disk_target_bytes"] = int(cfg["disk_floor_bytes"] * 1.25)
        cfg["disk_target_bytes"] = max(int(cfg["disk_target_bytes"]),
                                       int(cfg["disk_floor_bytes"]))
        cfg["consumer_overlap"] = CONSUMER_OVERLAP
        return cfg

    def _free_disk_bytes(self, path: Optional[str] = None) -> Optional[int]:
        """剩余磁盘。取不到返回 None —— 那时**不**触发应急（宁可不裁，也不瞎裁）。

        单独一个方法是为了给测试一个缝：造一次真的磁盘告急代价太大。
        """
        try:
            return shutil.disk_usage(path or config.PROJECT_DIR).free
        except Exception as e:  # noqa: BLE001
            logger.warning("剩余磁盘读不到（本轮不触发应急裁剪）: %s", e)
            return None

    # ---------------- schema：把 ack_seq=0 从库层面挡掉 ----------------
    async def ensure_retention_schema(self, conn=None) -> bool:
        """给 ``sync_meta`` 装上「ack_seq 永远不是 0」的 CHECK。幂等。

        P6-2 的第二层。``read_ack_floor()`` 保证**读**的时候 0 不会变成下界；
        这一条保证 0 **根本存不进去** —— 包括一次手工 UPDATE、一次未来的
        「初始化一下 ack_seq」提交、一次从旧库导入。

        装之前先自愈：库里已经躺着一个 ``ack_seq='0'`` 的话（陷阱已经踩过了），
        直接删掉那一行 —— 它与「从未 ack」语义等价（见 read_ack_floor），
        所以删除不丢任何信息，而留着会让 ADD CONSTRAINT 失败、防线装不上。

        由 ``SchemaMixin.init_tables()`` 在 connect() 期调用，也在每轮 pass 开头
        再调一次（发现约束已在就立刻返回，只花一次目录查询）。
        """
        own = conn is None
        if own:
            conn = await self._open_retention_conn()
        try:
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_constraint con "
                "  JOIN pg_class rel ON rel.oid = con.conrelid "
                "  JOIN pg_namespace n ON n.oid = rel.relnamespace "
                " WHERE n.nspname = 'scraper' AND rel.relname = 'sync_meta' "
                "   AND con.conname = $1", ACK_SEQ_CHECK)
            if exists:
                self._rt()["schema_ready"] = True
                return False

            phantom = await conn.fetchval(
                "SELECT v FROM scraper.sync_meta WHERE k = 'ack_seq'")
            if phantom is not None and read_ack_floor(phantom) is None:
                await conn.execute(
                    "DELETE FROM scraper.sync_meta WHERE k = 'ack_seq'")
                self._rt_bump("phantom_ack_repaired")
                logger.error(
                    "自愈：删掉了 scraper.sync_meta 里的幽灵 ack_seq=%r。"
                    "它与「从未 ack」等价，但会让 min(age_floor, 0) = 0，"
                    "保留期从此一行不裁直到磁盘满（计划 §Phase 6 点名的陷阱）。",
                    phantom)

            try:
                await conn.execute(
                    f"ALTER TABLE scraper.sync_meta ADD CONSTRAINT {ACK_SEQ_CHECK} "
                    f"CHECK (k <> 'ack_seq' OR v ~ '^[1-9][0-9]*$')")
            except asyncpg.DuplicateObjectError:
                pass                      # 两个实例同时引导
            except asyncpg.UndefinedTableError:
                # 事件流表还没建（init_event_stream 尚未跑）。不是错误，下次再来。
                return False
            self._rt()["schema_ready"] = True
            logger.info("保留期防线就绪：sync_meta 上的 %s（ack_seq 不可能是 0）",
                        ACK_SEQ_CHECK)
            return True
        finally:
            if own:
                await conn.close()

    async def _open_retention_conn(self) -> asyncpg.Connection:
        """保留期专用连接（承重约束 8）。"""
        return await asyncpg.connect(
            dsn=self.dsn,
            statement_cache_size=0,
            command_timeout=config.PG_COMMAND_TIMEOUT,
            server_settings={"search_path": "public"},
        )

    # ---------------- P6-4：谁都不许缓存边界 ----------------
    async def _assert_no_cached_bounds(self, conn) -> None:
        """sync_meta 里出现任何边界缓存键就炸。

        ``min_available_seq`` 现算是 409 守卫成立的前提：缓存值必然落后于一次
        DROP，而落后的下界会让一个真的掉了窗的游标拿到 200 + 一段跳过了被裁区间
        的数据。没有任何一侧会察觉。
        所以这不是「建议不要缓存」，是每轮 pass 都体检一次的硬约束。
        """
        rows = await conn.fetch(
            "SELECT k FROM scraper.sync_meta WHERE k = ANY($1::text[])",
            sorted(FORBIDDEN_META_KEYS))
        if rows:
            raise RetentionInvariantError(
                f"sync_meta 里出现了边界缓存键 {[r['k'] for r in rows]}。"
                f"min_available_seq 必须永远现算（计划 §Phase 6）：缓存值落后于一次"
                f"分区 DROP，就会让掉窗的游标拿到 200 而不是 409，静默丢数据。")

    # ---------------- 观测（只读，/status 直接吃这份） ----------------
    async def retention_observe(self, conn) -> Dict[str, Any]:
        """算出当前的三条下界与容量压力。**只读**，可以在别人的只读快照里跑。

        故意不接受「用缓存」这个选项：调用方每次拿到的都是现算值。
        """
        cfg = self.retention_config()
        parts = await self._collect_partitions(conn)
        next_seq = await self._next_seq(conn)
        meta = dict(await self._read_meta_kv(conn))

        row = await conn.fetchrow(
            "SELECT min(seq) AS min_seq, max(seq) AS max_seq "
            "FROM scraper.scrape_events")
        min_seq = row["min_seq"] if row else None
        max_seq = row["max_seq"] if row else None

        age_floor, horizon = await self._age_floor(conn, cfg, max_seq)
        ack_seq = read_ack_floor(meta.get("ack_seq"))
        free_disk = self._free_disk_bytes(cfg["disk_path"])

        live = [p for p in parts if not p.is_future(next_seq)]
        est_rows = sum(p.est_rows for p in live)
        hard_floor, hard_reasons = self._hard_floor(cfg, live, free_disk, est_rows)
        floor = combine_floors(age_floor=age_floor, ack_floor=ack_seq,
                               hard_floor=hard_floor,
                               ack_slack=cfg["ack_slack_seq"])
        drops = self._plan_drops(parts, next_seq, floor)
        forced = _unacknowledged(_parse_log(meta.get("forced_prune_log")))

        return {
            "enabled": cfg["enabled"],
            "age_days": cfg["age_days"],
            "age_horizon_utc": _iso(horizon),
            "age_floor_seq": age_floor,
            # None = 从未 ack。契约要求消费侧区分 null 与 0（见 §3）。
            "ack_seq": ack_seq,
            "ack_slack_seq": cfg["ack_slack_seq"],
            "ack_floor_seq": (None if ack_seq is None
                              else max(ack_seq - cfg["ack_slack_seq"], 0)),
            "hard_floor_seq": hard_floor,
            "hard_floor_reasons": hard_reasons,
            "effective_floor_seq": floor,
            "min_seq": min_seq,
            "max_seq": max_seq,
            "next_seq": next_seq,
            "free_disk_bytes": free_disk,
            "disk_floor_bytes": cfg["disk_floor_bytes"],
            "disk_target_bytes": cfg["disk_target_bytes"],
            "disk_pressure": bool(free_disk is not None
                                  and free_disk < cfg["disk_floor_bytes"]),
            "max_retained_partitions": cfg["max_retained_partitions"],
            "retained_partitions": len(live),
            "max_event_rows": cfg["max_event_rows"],
            "estimated_rows": est_rows,
            "partitions": [p.as_dict() for p in parts],
            "droppable_now": [p.name for p in drops],
            "forced_prune_pending": len(forced),
            "last_pass": self._rt()["last_pass"],
            "counters": dict(self._rt()["counters"]),
        }

    # ---------------- 一次裁剪 ----------------
    async def maybe_run_retention(self) -> Optional[Dict[str, Any]]:
        """维护协程的入口：到点了才真跑。异常由调用方兜（绝不能打断维护协程）。

        节流时钟在 ``finally`` 里推 —— **跳过和失败也要计时**。否则：
        单例锁被另一个实例握着时，每一次心跳（120s）都会重试并打一行日志；
        撞上 ``_assert_no_cached_bounds`` 这种持久性错误时更是每 120s 一条 ERROR。
        本来就该 20 分钟才响一次的东西，没必要因为它失败了就响十倍。
        """
        cfg = self.retention_config()
        if not cfg["enabled"]:
            return None
        st = self._rt()
        last = st["last_pass_monotonic"]
        if last is not None and (time.monotonic() - last) < cfg["interval_s"]:
            return None
        try:
            return await self.run_retention_pass()
        finally:
            st["last_pass_monotonic"] = time.monotonic()

    async def run_retention_pass(self, *, dry_run: bool = False,
                                 conn=None) -> Dict[str, Any]:
        """一轮保留期。返回摘要（可直接进日志/`/status`）。

        单飞三层：进程内 ``asyncio.Lock``、库级 ``pg_try_advisory_lock``、
        以及「拿不到就跳过」。两个实例同时裁同一批分区不会丢数据（DROP 幂等），
        但会互相把对方堵在锁队列里，而那正是承重约束 7 要躲的。
        """
        cfg = self.retention_config()
        st = self._rt()
        if st["lock"].locked():
            return {"skipped": "already_running"}

        async with st["lock"]:
            own = conn is None
            if own:
                conn = await self._open_retention_conn()
            got_lock = False
            try:
                if own:
                    got_lock = await conn.fetchval(
                        "SELECT pg_try_advisory_lock($1)", RETENTION_LOCK_KEY)
                    if not got_lock:
                        logger.info("保留期单例锁被别的实例持有，跳过本轮")
                        return {"skipped": "lock_held_elsewhere"}
                summary = await self._retention_pass_locked(conn, cfg, dry_run)
            except Exception as e:  # noqa: BLE001
                self._rt_bump("errors")
                logger.error("保留期本轮失败：%s: %s", type(e).__name__, e)
                raise
            finally:
                # 收尾一律吞异常：这段在 CancelledError 传播途中也会跑
                # （close() 会取消维护协程），那时 await 本身就可能再抛一次。
                # 连接关掉，session 级的 advisory lock 由服务端自动释放，
                # 所以即使 unlock 没发出去也不会留下一把没人放的锁。
                if got_lock:
                    try:
                        await conn.execute("SELECT pg_advisory_unlock($1)",
                                           RETENTION_LOCK_KEY)
                    except BaseException:  # noqa: BLE001
                        pass
                if own:
                    try:
                        await conn.close()
                    except BaseException:  # noqa: BLE001
                        pass

        st["last_pass"] = summary
        self._rt_bump("passes")
        return summary

    async def _retention_pass_locked(self, conn, cfg: Dict[str, Any],
                                     dry_run: bool) -> Dict[str, Any]:
        await self.ensure_retention_schema(conn)
        await self._assert_no_cached_bounds(conn)

        parts = await self._collect_partitions(conn)
        next_seq = await self._next_seq(conn)
        meta = dict(await self._read_meta_kv(conn))
        max_seq = await conn.fetchval("SELECT max(seq) FROM scraper.scrape_events")

        age_floor, horizon = await self._age_floor(conn, cfg, max_seq)
        ack_seq = read_ack_floor(meta.get("ack_seq"))
        soft_floor = combine_floors(age_floor=age_floor, ack_floor=ack_seq,
                                    hard_floor=0,
                                    ack_slack=cfg["ack_slack_seq"])

        dropped: List[Dict[str, Any]] = []
        forced: List[Dict[str, Any]] = []
        stopped = None

        # 前缀扫描：只从最老的一端裁，遇到第一个不可裁的立刻停（承重约束 2）。
        for p in self._ordered(parts):
            live = [q for q in parts if not q.is_future(next_seq)]
            if len(live) <= 1:
                stopped = "last_partition"
                break
            if not p.is_sealed(next_seq):
                # 头部（还在写）或未来分区。裁它 = relay 下一条 INSERT 无处可去。
                stopped = "not_sealed"
                break

            # 两条判据都要过，而**第二条才是承重的**（见 _visible_floor_after）：
            #   1. p.max_seq <= floor      —— 不销毁任何一行高于下界的记录
            #   2. 裁完之后消费者看到的 min_available_seq - 1 <= floor
            #      —— 不把 409 判据抬到下界之上
            visible = self._visible_floor_after(parts, p.name, next_seq)
            if p.below(soft_floor) and visible <= soft_floor:
                reason, is_forced = "floor", False
            else:
                pressure = self._pressure(cfg, parts, next_seq)
                if not pressure:
                    stopped = ("above_floor" if not p.below(soft_floor)
                               else "would_break_overlap")
                    break
                reason, is_forced = pressure, True

            entry = {
                "partition": p.name,
                "from_seq": p.min_seq if p.min_seq is not None else p.lo,
                "to_seq": p.max_seq if p.max_seq is not None else (
                    None if p.hi is None else p.hi - 1),
                "est_rows": p.est_rows,
                "size_bytes": p.size_bytes,
                "reason": reason,
            }
            if dry_run:
                dropped.append({**entry, "forced": is_forced, "dry_run": True})
                parts = [q for q in parts if q.name != p.name]
                continue

            ok = await self._drop_partition(conn, p.name, cfg["lock_timeout"])
            if not ok:
                stopped = "lock_timeout"
                break
            self._rt_bump("partitions_dropped")
            dropped.append({**entry, "forced": is_forced})
            parts = [q for q in parts if q.name != p.name]

            if is_forced:
                self._rt_bump("forced_drops")
                logged = await self.record_forced_prune(conn, {
                    **entry,
                    "ack_seq_at_time": ack_seq,
                    "soft_floor_seq": soft_floor,
                    "free_disk_bytes": self._free_disk_bytes(cfg["disk_path"]),
                })
                forced.append(logged)
                logger.error(
                    "强制裁剪：分区 %s（seq %s..%s，约 %s 行）被应急下界（%s）裁掉，"
                    "而它高于安全下界 %s（ack_seq=%s）。已写入 forced_prune_log #%s，"
                    "`/status` 会一直返回它直到消费者确认。",
                    p.name, entry["from_seq"], entry["to_seq"], p.est_rows,
                    reason, soft_floor, ack_seq, logged.get("id"))
        else:
            stopped = "no_more_partitions"

        summary = {
            "at": _iso(datetime.now(timezone.utc)),
            "dry_run": dry_run,
            "age_horizon_utc": _iso(horizon),
            "age_floor_seq": age_floor,
            "ack_seq": ack_seq,
            "ack_slack_seq": cfg["ack_slack_seq"],
            "soft_floor_seq": soft_floor,
            "dropped": dropped,
            "dropped_count": len(dropped),
            "forced_count": len(forced),
            "stopped_because": stopped,
            "free_disk_bytes": self._free_disk_bytes(cfg["disk_path"]),
        }
        if dropped:
            logger.info("保留期%s %d 个分区（强制 %d）：%s",
                        "【演练】本该裁掉" if dry_run else "裁掉",
                        len(dropped), len(forced),
                        [d["partition"] for d in dropped])
        return summary

    async def _drop_partition(self, conn, name: str, lock_timeout: str) -> bool:
        """``DROP TABLE`` 一个分区。拿不到锁返回 False（不是异常，是常态）。

        为什么带 lock_timeout：DROP 要父表的 ACCESS EXCLUSIVE，它会排在正在跑的
        ``/records`` 后面；而 PG 的锁队列是 FIFO，排队中的 ACCESS EXCLUSIVE 会把
        它**后面**所有新读者一起堵住。实测（Phase 3 verify2）一次 DROP 在读者
        后面堵了 3s 以上。宁可这轮不裁。
        """
        timeout = _safe_interval(lock_timeout)
        try:
            async with conn.transaction():
                await conn.execute(f"SET LOCAL lock_timeout = '{timeout}'")
                await conn.execute(f'DROP TABLE scraper."{name}"')
            return True
        except (asyncpg.LockNotAvailableError, asyncpg.QueryCanceledError) as e:
            self._rt_bump("lock_timeouts")
            logger.warning(
                "裁剪分区 %s 拿不到锁（%s），本轮放弃，下一轮再来。"
                "读者一直很长时是正常现象，不必告警。", name, type(e).__name__)
            return False

    # ---------------- 下界的三个输入 ----------------
    async def _age_floor(self, conn, cfg: Dict[str, Any],
                         max_seq: Optional[int]):
        """时间下界 = 「还没老到可以裁的那批行」里最小的 seq 减一。

        为什么**不是** ``max(seq) WHERE recorded_at < horizon``（审计 §3.6 的写法）：
        那个式子对时钟前跳没有抵抗力 —— 一行被写成很旧的 recorded_at 却带着很高的
        seq，就会把下界一次性抬到那个高 seq，把它下面所有**还很新**的行一起判死。
        取「最老的一条新行」减一，方向正好相反：时钟异常只会让保留期**更保守**。
        计划 §2.1 也写了「排序权威是 seq，不是任何时间戳」。
        """
        horizon = datetime.now(timezone.utc) - timedelta(days=cfg["age_days"])
        fresh_min = await conn.fetchval(
            "SELECT min(seq) FROM scraper.scrape_events WHERE recorded_at >= $1",
            horizon)
        if fresh_min is not None:
            return max(int(fresh_min) - 1, 0), horizon
        # 一行都不「新」：整张表都可以按时间裁（真裁不裁还要过 ack 与前缀规则）。
        return int(max_seq or 0), horizon

    def _hard_floor(self, cfg, live: Sequence[PartitionView],
                    free_disk: Optional[int], est_rows: int):
        """容量应急下界 —— 只看**结构性**的两条（分区数 / 行数），磁盘那条是
        迭代出来的（每裁一个重新量一次），所以不在这里出数。

        返回 ``(floor_seq, reasons)``。没有压力时是 ``(0, [])``。
        """
        reasons: List[str] = []
        floor = 0
        ordered = self._ordered(live)
        cap = int(cfg["max_retained_partitions"])
        if cap > 0 and len(ordered) > cap:
            sacrifice = ordered[:len(ordered) - cap]
            top = max((p.max_seq for p in sacrifice if p.max_seq is not None),
                      default=0)
            if top > floor:
                floor = top
            reasons.append(f"partitions>{cap}")
        rows_cap = int(cfg["max_event_rows"])
        if rows_cap > 0 and est_rows > rows_cap:
            acc, top = est_rows, 0
            for p in ordered:
                if acc <= rows_cap:
                    break
                acc -= p.est_rows
                if p.max_seq is not None:
                    top = max(top, p.max_seq)
            floor = max(floor, top)
            reasons.append(f"rows>{rows_cap}")
        if free_disk is not None and free_disk < int(cfg["disk_floor_bytes"]):
            reasons.append("disk")
        return floor, reasons

    def _pressure(self, cfg, parts, next_seq) -> Optional[str]:
        """现在还有没有「必须越过 ack 继续裁」的理由。每次裁完都重新问一遍。"""
        live = [p for p in parts if not p.is_future(next_seq)]
        if int(cfg["max_retained_partitions"]) > 0 and \
                len(live) > int(cfg["max_retained_partitions"]):
            return "partition_cap"
        est_rows = sum(p.est_rows for p in live)
        if int(cfg["max_event_rows"]) > 0 and est_rows > int(cfg["max_event_rows"]):
            return "row_cap"
        free = self._free_disk_bytes(cfg["disk_path"])
        if free is None:
            return None                    # 量不到就不瞎裁
        # 迟滞：跌破 floor 才开始，裁到 target 才停。
        started = bool((self._rt().get("_disk_emergency")))
        if free < int(cfg["disk_floor_bytes"]):
            self._rt()["_disk_emergency"] = True
            return "disk_floor"
        if started and free < int(cfg["disk_target_bytes"]):
            return "disk_target"
        self._rt()["_disk_emergency"] = False
        return None

    # ---------------- forced_prune_log：持久闩锁（P6-3） ----------------
    async def record_forced_prune(self, conn, entry: Dict[str, Any]) -> Dict[str, Any]:
        """往闩锁里追加一条。返回写进去的那条（带 id）。

        读改写包在一个事务里并对那一行 ``FOR UPDATE`` —— 两轮 pass 或
        「一轮 pass + 一次消费者确认」并发时，裸读改写会丢掉其中一边的更新，
        而丢掉的如果是新写入的那条，就等于一次数据丢失**没有留下任何记录**。
        """
        row = {
            "from_seq": entry.get("from_seq"),
            "to_seq": entry.get("to_seq"),
            # 计划要求逐字记这四项：(from_seq, to_seq, ack_seq_at_time, ts)
            "ack_seq_at_time": entry.get("ack_seq_at_time"),
            "ts": _iso(datetime.now(timezone.utc)),
            "partition": entry.get("partition"),
            "reason": entry.get("reason"),
            "est_rows": entry.get("est_rows"),
            "size_bytes": entry.get("size_bytes"),
            "soft_floor_seq": entry.get("soft_floor_seq"),
            "free_disk_bytes": entry.get("free_disk_bytes"),
            # 兼容旧读法：Phase 3 的用例按这个名字取「裁到哪」。
            "dropped_through_seq": entry.get("to_seq"),
        }
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO scraper.sync_meta (k, v) "
                "VALUES ('forced_prune_log', '[]') ON CONFLICT (k) DO NOTHING")
            raw = await conn.fetchval(
                "SELECT v FROM scraper.sync_meta WHERE k = 'forced_prune_log' "
                "FOR UPDATE")
            log = _parse_log(raw)
            row["id"] = _next_log_id(log)
            log.append(row)
            log = _trim_log(log)
            await conn.execute(
                "UPDATE scraper.sync_meta SET v = $1 WHERE k = 'forced_prune_log'",
                json.dumps(log, ensure_ascii=False))
        return row

    async def forced_prune_entries(self, conn, *,
                                   include_acknowledged: bool = False):
        raw = await conn.fetchval(
            "SELECT v FROM scraper.sync_meta WHERE k = 'forced_prune_log'")
        log = _parse_log(raw)
        return log if include_acknowledged else _unacknowledged(log)

    async def acknowledge_forced_prunes(self, conn, *,
                                        ids: Optional[Sequence[Any]] = None,
                                        acknowledge_all: bool = False
                                        ) -> Dict[str, Any]:
        """消费者逐条确认。返回 ``{acknowledged, unknown, remaining}``。

        闩锁**不会**因为确认而消失得无影无踪：条目留在日志里、只是打上
        ``acknowledged_at``，`/status` 不再返回它。事后要复盘「那次到底裁掉了
        什么」时还查得到（容量上限内）。
        """
        want = None if acknowledge_all else {str(i) for i in (ids or [])}
        now = _iso(datetime.now(timezone.utc))
        acked: List[Any] = []
        unknown: List[str] = []
        async with conn.transaction():
            raw = await conn.fetchval(
                "SELECT v FROM scraper.sync_meta WHERE k = 'forced_prune_log' "
                "FOR UPDATE")
            log = _parse_log(raw)
            seen = set()
            for item in log:
                if not isinstance(item, dict):
                    continue
                ident = item.get("id")
                if ident is not None:
                    seen.add(str(ident))
                if item.get("acknowledged_at"):
                    continue
                if want is None or (ident is not None and str(ident) in want):
                    item["acknowledged_at"] = now
                    acked.append(ident)
            if want is not None:
                unknown = sorted(want - seen)
            log = _trim_log(log)
            if raw is not None:
                await conn.execute(
                    "UPDATE scraper.sync_meta SET v = $1 "
                    "WHERE k = 'forced_prune_log'",
                    json.dumps(log, ensure_ascii=False))
        if acked:
            logger.warning("消费者确认了 %d 条强制裁剪记录：%s", len(acked), acked)
        return {"acknowledged": acked, "unknown": unknown,
                "remaining": _unacknowledged(log)}

    # ---------------- 小工具 ----------------
    async def _read_meta_kv(self, conn):
        rows = await conn.fetch("SELECT k, v FROM scraper.sync_meta")
        return [(r["k"], r["v"]) for r in rows]

    async def _next_seq(self, conn) -> int:
        """下一个会被发出去的 seq。承重约束 3 的判据。

        复用 relay 的 ``_seq_high_water()``（它认 ``is_called``，全新序列返回 0），
        +1 就是「下一个号」。直接读 ``last_value`` 会把全新库误判成已发过 1。
        """
        return int(await self._seq_high_water(conn)) + 1

    async def _collect_partitions(self, conn) -> List[PartitionView]:
        from common.pgdb.relay import _partition_index, parse_partition_bound

        rows = await conn.fetch(_PARTITION_CATALOG_SQL)
        out: List[PartitionView] = []
        for r in rows:
            idx = _partition_index(r["name"])
            if idx is None:
                continue
            lo, hi = parse_partition_bound(r["bound"])
            mm = await conn.fetchrow(
                f'SELECT min(seq) AS lo, max(seq) AS hi FROM scraper."{r["name"]}"')
            out.append(PartitionView(
                name=r["name"], lo=lo, hi=hi, index=idx,
                min_seq=mm["lo"] if mm else None,
                max_seq=mm["hi"] if mm else None,
                est_rows=int(r["est_rows"] or 0),
                size_bytes=int(r["size_bytes"] or 0)))
        return self._ordered(out)

    @staticmethod
    def _ordered(parts: Sequence[PartitionView]) -> List[PartitionView]:
        """按区间下界升序；MINVALUE（lo=None）排最前。"""
        return sorted(parts, key=lambda p: (0 if p.lo is None else 1, p.lo or 0))

    def _visible_floor_after(self, parts: Sequence[PartitionView],
                             dropping: str, next_seq: int) -> int:
        """裁掉 ``dropping`` 之后，``/records`` 的 409 判据会用的那个下界减一。

        **P6-5 的真正修法。** 只让下界（floor）与 ``ack_seq`` 保持 OVERLAP 余量
        是不够的 —— 实测（scratchpad/why409.py）：

            soft_floor  = 39999050        （已经比 ack 低了整整 1000）
            p1.max_seq  = 20000400        （远在下界之下，一行都没多裁）
            裁完 min(seq) = 40000001      ← 跳到了下界**之上** 950
            消费者按契约从 cursor-200 = 39999850 回拉 -> 409

        原因是 seq 允许有空洞（计划 §2.1：序列非事务性，回滚的批次会烧号；
        分区边界处更是天然一大段）。守卫比的是 ``min(seq)``，不是 floor，
        所以余量必须留在**守卫看得见的那个量**上，否则等于没留。

        于是判据从「这个分区的行够不够老」升级成「裁完之后消费者的窗口会落在
        哪里」。这条同时蕴含了 P6-1 的 ``max(seq) <= floor``
        （幸存分区的行必然高于被裁分区的行），所以它是更强的那一条。

        表会被裁空时返回「已发出去过的最高号」—— 与 ``_window()`` 的空表分支
        （``min_available = head + 1``）逐字同源。
        """
        mins = [p.min_seq for p in parts
                if p.name != dropping and p.min_seq is not None]
        if not mins:
            return max(next_seq - 1, 0)
        return min(mins) - 1

    def _plan_drops(self, parts: Sequence[PartitionView], next_seq: int,
                    floor: int) -> List[PartitionView]:
        """只做展示用的「现在能裁哪些」（不含磁盘迭代那一段）。"""
        out: List[PartitionView] = []
        rest = list(parts)
        live = len([p for p in parts if not p.is_future(next_seq)])
        for p in self._ordered(parts):
            if live - len(out) <= 1:
                break
            if not p.is_sealed(next_seq) or not p.below(floor):
                break
            if self._visible_floor_after(rest, p.name, next_seq) > floor:
                break
            out.append(p)
            rest = [q for q in rest if q.name != p.name]
        return out


# ============================================================
# 模块级小工具（无状态，方便单测）
# ============================================================

# `_iso` 的本地副本已在 Phase 4.4 删除 —— 它与 server/api/sync.py 那份逐字节
# 相同（含 `.replace("+00:00","Z")`）。真源在 common/core/timeutil.iso_utc，
# 本模块在文件头 import 它并保留 `_iso` 这个名字（调用点一字未动）。
# ⚠ 不要改成 `from server.api.sync import _iso`：common 是底座，依赖 server 是
# 分层倒置。


_INTERVAL_OK = re.compile(r"^\d+(ms|s|min)?$")


def _safe_interval(raw: Any, default: str = "5s") -> str:
    """``SET LOCAL lock_timeout`` 的值必须内插进 SQL（不能绑参），所以先校验。

    来源是环境变量，也就是运维手里的字符串 —— 白名单比转义可靠。
    """
    s = str(raw or "").strip()
    if not _INTERVAL_OK.match(s):
        logger.warning("SYNC_RETENTION_LOCK_TIMEOUT=%r 形状不对，用 %s", raw, default)
        return default
    return s


def _parse_log(raw: Optional[str]) -> List[Any]:
    """闩锁的 JSON → 条目列表。**解析不了也绝不当空处理。**

    返回值会被 ``record_forced_prune`` 原样写回去，所以「坏了就返回 []」
    等于用下一次强制裁剪把上一次的记录**擦掉** —— 而这个列表里的每一条都是
    一段永久丢失的数据的唯一凭据。所以坏值被包成一条保留原文的条目带着走，
    它没有 ``id``（只能用 ``all: true`` 清），于是必然有人来看一眼。
    形状与 ``server/api/sync.py:_forced_prune_log`` 的降级分支一致。
    """
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        logger.error("forced_prune_log 不是合法 JSON，原文保留成一条无 id 的条目"
                     "（绝不丢弃 —— 它是数据丢失的唯一凭据）: %r", str(raw)[:200])
        return [{"raw": str(raw), "note": "forced_prune_log 不是合法 JSON"}]
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def _unacknowledged(log: Sequence[Any]) -> List[Any]:
    return [e for e in log
            if not (isinstance(e, dict) and e.get("acknowledged_at"))]


def _next_log_id(log: Sequence[Any]) -> int:
    top = 0
    for e in log:
        if isinstance(e, dict):
            try:
                top = max(top, int(e.get("id") or 0))
            except (TypeError, ValueError):
                continue
    return top + 1


def _trim_log(log: List[Any]) -> List[Any]:
    """容量上限：**先丢已确认的**，未确认的一条都不丢。"""
    if len(log) <= FORCED_PRUNE_LOG_CAP:
        return log
    pending = [e for e in log
               if isinstance(e, dict) and not e.get("acknowledged_at")]
    done = [e for e in log
            if not (isinstance(e, dict) and not e.get("acknowledged_at"))]
    room = max(FORCED_PRUNE_LOG_CAP - len(pending), 0)
    kept = done[-room:] if room else []
    merged = kept + pending
    merged.sort(key=lambda e: (isinstance(e, dict) and e.get("id")) or 0)
    return merged


__all__ = [
    "RetentionMixin",
    "RetentionInvariantError",
    "CONSUMER_OVERLAP",
    "RETENTION_LOCK_KEY",
    "FORCED_PRUNE_LOG_CAP",
    "FORBIDDEN_META_KEYS",
    "ACK_SEQ_CHECK",
    "read_ack_floor",
    "combine_floors",
]
