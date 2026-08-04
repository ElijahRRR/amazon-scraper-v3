"""common/pgdb/media.py —— 截图 + 卖家发现（F-009）+ 变体展开。

OWNS（10 个方法）:
    get_pending_screenshots        database.py:2291
    update_screenshot_status       database.py:2298
    get_screenshot_progress        database.py:2333
    _get_done_screenshot_path      database.py:2008   ← 走 self._db（写连接）
    _get_done_screenshot_paths     database.py:2033   ← 走 self.read()
    _hydrate_screenshot_paths      database.py:2071   ← **原地修改** items
    create_seller_batch            database.py:1443
    accept_seller_discovery_result database.py:1500
    get_seller_batch_progress      database.py:1616
    expand_batch_variants          database.py:851    ← 调 self.create_tasks

对外欠账（别人依赖本文件）:
    results_write.py -> _get_done_screenshot_path(asin, batch_id)
    results_read.py  -> _hydrate_screenshot_paths(items, batch_id)
依赖别人:
    tasks.py -> create_tasks(...)

--------------------------------------------------------------------------
⚠ 本文件的方法**黄金基线一条都没覆盖**
--------------------------------------------------------------------------
64 步场景里没有任何一步碰截图落地、seller 发现或变体展开。
所以：每个方法都要自己写 pytest，逐键断言返回形状（见 tests/pgdb/helpers.py
提供的 scratch 库夹具）。没有网可以兜着。

--------------------------------------------------------------------------
移植要点
--------------------------------------------------------------------------
* _get_done_screenshot_path 用 ``self._db``、_get_done_screenshot_paths 用
  ``self.read()``。两者不同**不是**笔误，保持原样。
* 两者都靠 ``ORDER BY updated_at DESC, id DESC LIMIT 1`` 决定"哪张截图胜出"。
  PG 的 DESC 默认 NULLS FIRST——一行 updated_at 为 NULL 就会窜到最前面，
  改掉每一条结果/导出里的 screenshot_path。**必须**写成
  ``ORDER BY updated_at DESC NULLS LAST, id DESC``。
* _hydrate_screenshot_paths 是**原地改** items 的（没有返回值），
  并且会把无效占位值统一成 None（_shared 的 _normalize_screenshot_path）。
* update_screenshot_status 里两条 UPDATE 的加锁顺序照抄，返回 bool。
  rowcount 来自 ConnProxy，已经是 int。
* create_seller_batch:
  - discover_mode 非法值 raise ValueError；空 seller_ids 返回 (0, 0)。
  - 返回 **2-tuple** ``(batch_id, inserted)``，app.py:1067 直接解包。
  - ``inserted`` 同样来自 total_changes 差值 → 换成单条 set-based INSERT +
    命令标签（做法见 tasks.py 的 create_tasks 说明）。
* accept_seller_discovery_result:
  - 返回 dict **原样**就是 HTTP 响应体（app.py:1626 ``return result``），键是
    {"accepted", "stale", "discovered", "new_asins", "detail_tasks_created"}。
  - ``detail_tasks_created`` 又是一个 total_changes 差值。
  - lease 门与 results_write 那条同构：rowcount==0 即 stale，必须是 int。
  - seller_discoveries 是复合主键，``ON CONFLICT DO NOTHING`` 有合法仲裁者。
* get_seller_batch_progress 返回
  {batch_id, discover: {...}, detail: {...}, discovered_asins, discover_mode}。
* expand_batch_variants 返回新增任务数，内部调 ``self.create_tasks``。
  app.py:380 的 _completion_watcher 会循环调用它直到收敛，返回值是收敛判据。
* seller_discoveries.discovered_at 是唯一一个**依赖列默认值**并直接出现在
  响应里的时间戳（app.py:1106），同时还是 ORDER BY 键（app.py:1108）。
  schema.py 已经把默认值写成 to_char(clock_timestamp() AT TIME ZONE 'UTC', ...)。
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from common.pgdb._shared import _normalize_screenshot_path  # noqa: F401

_TODO = "common/pgdb/media.py 尚未实现"


class MediaMixin:
    """只定义方法，绝不定义 __init__。"""

    # ---------------- 截图 ----------------
    async def get_pending_screenshots(self, batch_id: int,
                                      limit: int = 50) -> List[Dict]:
        raise NotImplementedError(_TODO)

    async def update_screenshot_status(self, asin: str, batch_id: int, status: str,
                                       file_path: str = None,
                                       error: str = None) -> bool:
        raise NotImplementedError(_TODO)

    async def get_screenshot_progress(self, batch_id: int) -> Dict:
        """{pending, processing, done, failed, total}；total 是四者之和
        （在 total 自身被塞进 dict **之前**用 sum(stats.values()) 算的）。"""
        raise NotImplementedError(_TODO)

    async def _get_done_screenshot_path(self, asin: str,
                                        batch_id: int = None) -> Optional[str]:
        """走 self._db（写连接）。"""
        raise NotImplementedError(_TODO)

    async def _get_done_screenshot_paths(self, asins: List[str],
                                         batch_id: int = None) -> Dict[str, str]:
        """走 self.read()。"""
        raise NotImplementedError(_TODO)

    async def _hydrate_screenshot_paths(self, items: List[Dict],
                                        batch_id: int = None):
        """**原地修改** items，无返回值。"""
        raise NotImplementedError(_TODO)

    # ---------------- 卖家发现（F-009）----------------
    async def create_seller_batch(self, name: str, seller_ids: List[str],
                                  discover_mode: str = "with_detail",
                                  zip_code: str = "10001",
                                  needs_screenshot: bool = False) -> Tuple[int, int]:
        """返回 (batch_id, inserted_tasks)；非法 discover_mode raise ValueError。"""
        raise NotImplementedError(_TODO)

    async def accept_seller_discovery_result(self, task_id: int, worker_id: str,
                                             lease_epoch: int, batch_id: int,
                                             seller_id: str,
                                             items: List[Dict],
                                             meta: Optional[Dict] = None) -> Dict:
        """{"accepted","stale","discovered","new_asins","detail_tasks_created"}
        —— 原样即 HTTP 响应体。"""
        raise NotImplementedError(_TODO)

    async def get_seller_batch_progress(self, batch_id: int) -> Dict:
        raise NotImplementedError(_TODO)

    # ---------------- 变体展开 ----------------
    async def expand_batch_variants(self, batch_id: int) -> int:
        """返回新增任务数；内部调 self.create_tasks（tasks.py）。"""
        raise NotImplementedError(_TODO)
