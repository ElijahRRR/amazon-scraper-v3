"""增量导出的保留期下界 —— 被裁空的流必须 409，不能 200 空。

真机实测逼出来的：本机把事件流 ``TRUNCATE`` 掉之后，
``/api/v1/sync/status`` 正确报 ``min_available_seq=676 / max_seq=675``，
同一时刻 ``/api/export/incremental?cursor=0`` 却返回 **200 + records: []**。

根因是导出端点在调 ``_window()`` 之前先把两个下界压成了一个数：

    max(_as_int(min_raw) or 0, _as_int(min_after) or 0)

空表时 ``min(seq)`` 是 NULL -> ``None`` -> ``or 0`` -> **0**，而 ``_window``
判空靠的正是 ``min_seq is None``，于是它走进「非空」分支返回 ``min_available=0``，
而 ``cursor + 1 < 0`` 永假 —— **409 永远发不出来**。

后果正是 ``_window`` 的 docstring 点名要防的那一种：一个丢了全部数据的消费者
拿到 200 空页，于是永远等下去，两侧都不告警。``/api/v1/sync/records`` 一直是
对的（它把 ``min_raw`` 原样传进去，两次窗口各自算完再比），导出这一份是抄的时候
走了捷径。

``tests/test_incremental_export.py`` 里一条 409 用例都没有，所以它一路绿到真机。
"""
from __future__ import annotations

import httpx
import pytest

pytestmark = pytest.mark.asyncio

from tests.pgdb.test_sync_api import _seed  # noqa: E402  复用同一份直插助手


TOKEN = "test-export-token"


@pytest.fixture
def export_env(pgdb, monkeypatch):
    import server.app as srv
    monkeypatch.setenv("DB_BACKEND", "postgres")
    monkeypatch.setenv("EXPORT_TOKEN", TOKEN)
    monkeypatch.setattr(srv, "db", pgdb, raising=False)
    return srv


@pytest.fixture
def client(export_env):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=export_env.app),
                             base_url="http://export-test")


async def _get(c, cursor, limit=10):
    return await c.get("/api/export/incremental",
                       params={"cursor": cursor, "limit": limit},
                       headers={"X-Export-Token": TOKEN})


async def _prune_to_empty(pgconn, gen, seqs):
    """播种后整表删空，并把 max_seq_ever 留在原处 —— 等价于保留期把分区 DROP 光。"""
    await _seed(pgconn, gen, seqs)
    await pgconn.execute(
        "INSERT INTO scraper.sync_meta (k,v) VALUES ('max_seq_ever',$1) "
        "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", str(max(seqs)))
    await pgconn.execute("DELETE FROM scraper.scrape_events")


async def test_pruned_to_empty_409s_a_stale_cursor(client, pgdb, pgconn):
    await _prune_to_empty(pgconn, pgdb.event_gen, [1000, 1001])
    async with client as c:
        r = await _get(c, 0)
        assert r.status_code == 409, r.text
        b = r.json()
        assert b["error"] == "cursor_below_retention"
        # 键名是 min_available_cursor 而不是 sync 那边的 min_available_seq：
        # 对外契约统一说 cursor，运维面才说 seq。两套命名不要串。
        assert b["min_available_cursor"] == 1002
        assert b["cursor"] == 0 and b["max_cursor"] == 1001


async def test_pruned_to_empty_still_200s_a_caught_up_cursor(client, pgdb, pgconn):
    """已经追平的消费者不该被误伤 —— 它什么都没漏，只是暂时没有新数据。"""
    await _prune_to_empty(pgconn, pgdb.event_gen, [1000, 1001])
    async with client as c:
        r = await _get(c, 1001)
        assert r.status_code == 200, r.text
        b = r.json()
        assert b["records"] == []
        assert b["has_more"] is False
        assert b["next_cursor"] == 1001, "空页不许推进游标"


async def test_brand_new_empty_stream_is_200_not_409(client, pgdb, pgconn):
    """全新空库（从没写过）：cursor=0 是 200 空，不是 409。

    与「被裁空」的区别全在 max_seq_ever 上。分不清这两种，要么新消费者一上来
    就被 409 挡住，要么老消费者丢了数据还以为自己是最新的。
    """
    async with client as c:
        r = await _get(c, 0)
        assert r.status_code == 200, r.text
        assert r.json()["records"] == []


async def test_non_empty_stream_409s_below_its_min(client, pgdb, pgconn):
    """常态：表非空、底部被裁过，落后的游标照样 409。"""
    gen = pgdb.event_gen
    await _seed(pgconn, gen, [500, 501, 502])
    async with client as c:
        r = await _get(c, 100)
        assert r.status_code == 409, r.text
        assert r.json()["min_available_cursor"] == 500

        r = await _get(c, 499)
        assert r.status_code == 200, r.text
        assert [x["cursor"] for x in r.json()["records"]] == [500, 501, 502]
