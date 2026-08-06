"""F-009 卖家采集四个端点的替代网（Phase 3.4）。

**为什么要单独有这份文件**：这四个端点在黄金 78 步里一步都没有，而且
**补不进去** —— ``POST /api/upload-sellers`` 的响应里含
``sellers_{%Y%m%d_%H%M%S}`` 批次名，不被 ``tests/golden/harness.py`` 的
``_TS_RE`` 覆盖（那个正则认的是 ``YYYY-MM-DD HH:MM:SS`` 形态），逐次不同，
录进基线就是每跑必红。于是把它们从 ``server/app.py`` 拆到
``server/api/sellers.py`` 这一步**没有黄金网可依赖**，这份用例就是那张网。

消掉非确定源的办法是显式传 ``batch_name``：那是响应里唯一的时间依赖，
其余字段（batch_id 自增、计数）在一次性的隔离库里是确定的。

写成 ``unittest.TestCase`` 而不是裸 ``def test_*``，是因为门禁里
``python -m unittest discover -s tests`` 是两列之一，它的加载器只认
``TestCase`` 子类 —— 裸函数用例在那一列会被静默跳过（`tests/conftest.py`
的长注释记了同一个坑的另一面）。

覆盖面按「拆分会怎么坏」来选，不是按行覆盖率：
  * 三个上传分支（txt / csv / xlsx）都要走到 —— 它们在 handler 里是
    ``filename.endswith`` 分派，搬模块时最容易漏 ``io`` / ``openpyxl`` 的 import。
  * 两条 4xx —— ``HTTPException`` 是从新模块抛的，得确认状态码和 detail 原样。
  * upload → pull → seller-result → progress → discoveries 的**串联** ——
    ``/api/tasks/seller-result`` 是跨节搬过来的（原本在 worker 节下），
    这条链子是「跨模块搬运没改路由匹配、也没丢 `_worker_registry` 写点」的证据。
  * ``discoveries`` 的过滤 / 翻页 / 夹逼 —— 那段 f-string 拼 WHERE + ``?``
    占位符必须两个后端都成立，所以本文件跟着 ``DB_BACKEND`` 走，两列各验一遍。
"""
from __future__ import annotations

import io
import unittest

import openpyxl

from tests.golden.harness import isolated_server

# 两个真实形态的卖家 ID；第三个只以 URL 参数形式出现，验证 URL 抽取分支。
_SELLER_A = "A2L77EE7U53NWQ"
_SELLER_B = "A1PA6795UKMFR9"
_SELLER_C = "A3JWKAKR8XB7XF"

_UPLOAD_TXT = (
    f"{_SELLER_A}\n"
    f"https://www.amazon.com/s?me={_SELLER_B}\n"
    f"junk, {_SELLER_A} ; ?seller={_SELLER_C.lower()}\n"
    "not-a-seller-id\n"
).encode()


def _upload(client, *, name, payload=_UPLOAD_TXT, filename="s.txt",
            mode="with_detail", ctype="text/plain"):
    return client.post(
        "/api/upload-sellers",
        files={"file": (filename, payload, ctype)},
        data={"batch_name": name, "discover_mode": mode},
    )


class SellerUploadTests(unittest.TestCase):

    def test_txt_upload_dedupes_and_keeps_order(self):
        """裸 ID / URL 参数两种形态都要认，重复只算一次，顺序按出现先后。"""
        with isolated_server() as (client, _ctx):
            r = _upload(client, name="t_txt")
            self.assertEqual(r.status_code, 200, r.text)
            body = r.json()
            self.assertEqual(body["batch_name"], "t_txt")
            self.assertEqual(body["discover_mode"], "with_detail")
            # A 出现两次只算一次；C 只以小写 URL 参数出现，也要被抽出来
            self.assertEqual(body["total_sellers"], 3)
            self.assertEqual(body["inserted_tasks"], 3)
            self.assertIsInstance(body["batch_id"], int)

    def test_csv_and_xlsx_branches(self):
        """三个分派分支里除 txt 外的两个：csv 走 decode，xlsx 走 openpyxl。"""
        with isolated_server() as (client, _ctx):
            r = _upload(client, name="t_csv", filename="s.csv",
                        payload=f"seller\n{_SELLER_A},{_SELLER_B}\n".encode(),
                        ctype="text/csv")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json()["total_sellers"], 2)

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append([_SELLER_A])
            ws.append([f"https://www.amazon.com/s?me={_SELLER_B}"])
            buf = io.BytesIO()
            wb.save(buf)
            wb.close()
            r = _upload(
                client, name="t_xlsx", filename="s.xlsx", payload=buf.getvalue(),
                ctype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json()["total_sellers"], 2)

    def test_rejects_bad_mode_and_empty_file(self):
        """两条 4xx 的码与 detail 都是契约的一部分（前端按 detail 显示）。"""
        with isolated_server() as (client, _ctx):
            r = _upload(client, name="t_bad", mode="nope")
            self.assertEqual(r.status_code, 400)
            self.assertEqual(r.json()["detail"], "非法 discover_mode: nope")

            r = _upload(client, name="t_empty", payload=b"nothing here at all\n")
            self.assertEqual(r.status_code, 400)
            self.assertIn("未识别到任何 seller ID", r.json()["detail"])


class SellerChainTests(unittest.TestCase):
    """upload → pull → seller-result → progress → discoveries 的串联。

    这条链子横跨 `server/api/sellers.py` 与 `server/app.py`（拉任务那一环
    还在 app.py），是 Phase 3.4 跨节搬运的主要证据。
    """

    def test_upload_pull_submit_progress_discoveries(self):
        with isolated_server() as (client, _ctx):
            up = _upload(client, name="chain")
            self.assertEqual(up.status_code, 200, up.text)
            batch_id = up.json()["batch_id"]

            # 刚上传：discover 全在 pending，detail 还没衍生，一条发现都没有
            prog = client.get(f"/api/seller-batches/{batch_id}/progress")
            self.assertEqual(prog.status_code, 200)
            p0 = prog.json()
            self.assertEqual(p0["batch_id"], batch_id)
            self.assertEqual(p0["discover"]["pending"], 3)
            self.assertEqual(p0["discover"]["done"], 0)
            self.assertEqual(p0["detail"]["total"], 0)

            pull = client.get("/api/tasks/pull",
                              params={"worker_id": "seller-w1", "count": 10})
            self.assertEqual(pull.status_code, 200, pull.text)
            tasks = [t for t in pull.json()["tasks"]
                     if t.get("task_type") == "discover_seller"]
            self.assertEqual(len(tasks), 3, "三个 seller 应各出一条 discover 任务")

            task = next(t for t in tasks if t["asin"] == _SELLER_A)
            sub = client.post("/api/tasks/seller-result", json={
                "task_id": task["id"],
                "batch_id": task["batch_id"],
                "worker_id": "seller-w1",
                "lease_epoch": task["lease_epoch"],
                "seller_id": _SELLER_A,
                "items": [
                    {"asin": "B00SELLER1", "list_title": "T1",
                     "list_price": "$1.00", "list_image": "http://x/1.jpg"},
                    {"asin": "B00SELLER2", "list_title": "T2",
                     "list_price": "$2.00", "list_image": "http://x/2.jpg"},
                ],
                "meta": {"pages": 1},
            })
            self.assertEqual(sub.status_code, 200, sub.text)
            s = sub.json()
            self.assertTrue(s["accepted"])
            self.assertFalse(s["stale"])
            self.assertEqual(s["discovered"], 2)
            # with_detail：发现的 ASIN 要衍生成主队列的详情任务
            self.assertEqual(s["detail_tasks_created"], 2)

            p1 = client.get(f"/api/seller-batches/{batch_id}/progress").json()
            self.assertEqual(p1["discover"]["done"], 1)
            self.assertEqual(p1["discover"]["pending"], 0)   # 另两条已被租出
            self.assertEqual(p1["detail"]["total"], 2)

            disc = client.get(f"/api/seller-batches/{batch_id}/discoveries")
            self.assertEqual(disc.status_code, 200)
            d = disc.json()
            self.assertEqual(d["limit"], 200)
            self.assertEqual(d["offset"], 0)
            self.assertEqual({i["asin"] for i in d["items"]},
                             {"B00SELLER1", "B00SELLER2"})
            self.assertEqual({i["seller_id"] for i in d["items"]}, {_SELLER_A})
            for item in d["items"]:
                # 列清单是显式写死的，多一列少一列都是改契约
                self.assertEqual(set(item), {"seller_id", "asin", "list_title",
                                             "list_price", "list_image",
                                             "discovered_at"})

    def test_seller_result_requires_task_id_and_seller_id(self):
        with isolated_server() as (client, _ctx):
            r = client.post("/api/tasks/seller-result",
                            json={"task_id": 1, "seller_id": ""})
            self.assertEqual(r.status_code, 400)
            self.assertEqual(r.json()["detail"], "task_id 和 seller_id 必填")


class SellerDiscoveriesQueryTests(unittest.TestCase):
    """``discoveries`` 那段 f-string 拼 WHERE + ``?`` 占位符的行为。

    过滤值会被 ``.strip().upper()``；``limit`` 夹到 [1, 1000]、``offset`` 夹到 >= 0。
    两个后端都要一致（`?` 占位符在 PG 侧由 common/pgdb 翻译）。
    """

    def _seed(self, client):
        up = _upload(client, name="q")
        batch_id = up.json()["batch_id"]
        pull = client.get("/api/tasks/pull",
                          params={"worker_id": "seller-w2", "count": 10})
        by_seller = {t["asin"]: t for t in pull.json()["tasks"]
                     if t.get("task_type") == "discover_seller"}
        for idx, sid in enumerate((_SELLER_A, _SELLER_B)):
            t = by_seller[sid]
            client.post("/api/tasks/seller-result", json={
                "task_id": t["id"], "batch_id": t["batch_id"],
                "worker_id": "seller-w2", "lease_epoch": t["lease_epoch"],
                "seller_id": sid,
                "items": [{"asin": f"B00Q{idx}{n}XXXXX"[:10], "list_title": "",
                           "list_price": "", "list_image": ""} for n in range(3)],
                "meta": {},
            })
        return batch_id

    def test_filter_paging_and_clamping(self):
        with isolated_server() as (client, _ctx):
            batch_id = self._seed(client)
            base = f"/api/seller-batches/{batch_id}/discoveries"

            allr = client.get(base).json()
            self.assertEqual(len(allr["items"]), 6)

            # 过滤值前后有空格、且是小写 —— handler 会 strip+upper
            filtered = client.get(base, params={"seller_id": f"  {_SELLER_B.lower()} "}).json()
            self.assertEqual(len(filtered["items"]), 3)
            self.assertEqual({i["seller_id"] for i in filtered["items"]}, {_SELLER_B})

            paged = client.get(base, params={"limit": 2, "offset": 2}).json()
            self.assertEqual(paged["limit"], 2)
            self.assertEqual(paged["offset"], 2)
            self.assertEqual(len(paged["items"]), 2)

            clamped = client.get(base, params={"limit": 99999, "offset": -5}).json()
            self.assertEqual(clamped["limit"], 1000)
            self.assertEqual(clamped["offset"], 0)
            self.assertEqual(len(clamped["items"]), 6)

            floor = client.get(base, params={"limit": 0}).json()
            self.assertEqual(floor["limit"], 1)
            self.assertEqual(len(floor["items"]), 1)

    def test_unknown_batch_is_empty_not_404(self):
        """空结果是 200 + 空列表 —— 前端靠这个区分"没有发现"与"批次不存在"。"""
        with isolated_server() as (client, _ctx):
            r = client.get("/api/seller-batches/999999/discoveries")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json(), {"items": [], "limit": 200, "offset": 0})


if __name__ == "__main__":
    unittest.main()
