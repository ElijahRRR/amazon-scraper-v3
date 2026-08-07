"""``POST /api/upload`` 撞名 -> **409 Conflict**，绝不静默合并。

------------------------------------------------------------------------
这份文件钉的是什么
------------------------------------------------------------------------
改动之前，同名批次第二次上传会拿到 **200 + 既有 batch_id**。审计实测复现出来的
后果有三条，它们其实是**同一个根因**（``INSERT OR IGNORE`` 整行不插、既有行
一个字段都不更新，而 HTTP 响应回显的是**请求**里的值）：

  1. 本次的新 ASIN 被悄悄并进上一个批次 —— 「一次采集」的语义破掉，
     两边都不报错（Phase 4.7 已经证明"撞名是 no-op"这个直觉是错的）；
  2. ``inserted`` 在清单部分重叠时是**非零**，看起来像成功新建了一个批次；
  3. **第二次的 external_id / callback_url 被静默丢弃**，可响应照样把请求里的
     ``callback_url`` 回显回去 —— 调用方以为回调注册好了，回调永远不触发。

改动之后：撞名 = 409，响应体带既有批次的 ``batch_id`` / ``batch_name`` /
``status_url``，调用方可以直接接着轮询。由此得到三条对外可依赖的性质，
下面每一条都有一个用例：

  * **POST /api/upload 从此可以安全重试**（推翻 erpAPI 那条「POST 永不自动重试」）：
    网络超时后重发，上一次若其实成功了，拿到的是 409 + 那个 batch_id，
    而不是又建一个批次、也不会悄悄并进去；
  * 批次名**不需要毫秒精度**去躲开合并；
  * **200 恒等于「新建了一个批次」**，``inserted`` 的歧义随之消失。

------------------------------------------------------------------------
为什么写成 unittest.TestCase 而不是裸 test_ 函数
------------------------------------------------------------------------
门禁里 ``python -m unittest discover -s tests`` 的加载器只认 TestCase 子类，
裸函数在那两列会被**静默跳过**。这里跟着 ``DB_BACKEND`` 走 ``isolated_server``，
所以四道 runner 门（pytest × 2 后端、unittest × 2 后端）全都盖得到。

黄金基线也录了撞名那一步（``upload_batch_a_duplicate`` 已改成 expect=409），
但基线只比得了「状态码 + 响应体」；「既有批次**没被改动**」这件事要查批次自己的
状态，基线不方便录，所以落在这里。
"""
from __future__ import annotations

import unittest

from tests.golden.harness import isolated_server

_NAME = "conflict_batch_a"
_ASINS_1 = ["B00CONF001", "B00CONF002"]
#: 与第一批**只重叠一个**。旧行为下 ``inserted`` 会是 1（非零）——
#: 「inserted=0 才算撞名」这个读法当年就是这么破的。
_ASINS_2 = ["B00CONF002", "B00CONF999"]

#: 公网 IP 字面量：``_is_safe_callback_url`` 对 IP 字面量**不做 DNS 解析**
#: （见该函数「合法公网 IP 字面量，无需 DNS 解析」那条 return），所以用例
#: 不碰网络、结果不随 DNS 抖动。用域名会走 ``loop.getaddrinfo``。
_CB_1 = "http://8.8.8.8/hook-one"
_CB_2 = "http://8.8.4.4/hook-two"


def _upload(client, asins, *, name=_NAME, external_id=None, callback_url=None):
    data = {"batch_name": name, "zip_code": "10001", "needs_screenshot": "false"}
    if external_id is not None:
        data["external_id"] = external_id
    if callback_url is not None:
        data["callback_url"] = callback_url
    return client.post(
        "/api/upload",
        files={"file": (f"{name}.txt", "\n".join(asins).encode(), "text/plain")},
        data=data,
    )


class UploadBatchNameConflictTests(unittest.TestCase):

    # ---------------- 409 本身 ----------------

    def test_duplicate_name_is_409_not_a_silent_merge(self):
        with isolated_server() as (client, _ctx):
            first = _upload(client, _ASINS_1, external_id="ext-1", callback_url=_CB_1)
            self.assertEqual(first.status_code, 200, first.text)
            batch_id = first.json()["batch_id"]

            second = _upload(client, _ASINS_2, external_id="ext-2", callback_url=_CB_2)
            self.assertEqual(
                second.status_code, 409,
                f"同名批次必须 409，实际 {second.status_code}: {second.text}")
            detail = second.json()["detail"]
            self.assertEqual(detail["error"], "batch_name_conflict")
            self.assertEqual(detail["batch_id"], batch_id,
                             "409 里必须给出**既有**批次的 id，调用方要拿它接着轮询")
            self.assertEqual(detail["batch_name"], _NAME)
            self.assertTrue(detail["status_url"].endswith(
                f"/api/batches/{_NAME}/status"), detail["status_url"])

    def test_the_status_url_in_the_409_actually_works(self):
        """409 给的 ``status_url`` 必须是能直接打的 —— 否则那个字段只是装饰。"""
        with isolated_server() as (client, _ctx):
            self.assertEqual(_upload(client, _ASINS_1).status_code, 200)
            detail = _upload(client, _ASINS_2).json()["detail"]
            # 用路径部分打（base_url 在 TestClient 下是 http://testserver）
            path = detail["status_url"].split("testserver", 1)[-1]
            r = client.get(path)
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json()["batch_id"], detail["batch_id"])

    # ---------------- 409 之后，既有批次一个字节都没动 ----------------

    def test_conflicting_upload_does_not_touch_the_existing_batch(self):
        """撞名那一发既不加 ASIN、也不改 external_id / callback_url。

        这条是旧坑 b + c 的**根因断言**：旧行为下 ``batches`` 表里留着第一次的
        external_id / callback_url，第二次的被静默丢弃，而响应回显的是请求值。
        """
        with isolated_server() as (client, _ctx):
            self.assertEqual(
                _upload(client, _ASINS_1, external_id="ext-1",
                        callback_url=_CB_1).status_code, 200)

            before = client.get(f"/api/batches/{_NAME}/status").json()
            self.assertEqual(_upload(client, _ASINS_2, external_id="ext-2",
                                     callback_url=_CB_2).status_code, 409)
            after = client.get(f"/api/batches/{_NAME}/status").json()

            self.assertEqual(after["external_id"], "ext-1",
                             "既有批次的 external_id 不许被撞名那一发改掉")
            self.assertEqual(after["callback"]["url"], _CB_1,
                             "既有批次的 callback_url 不许被撞名那一发改掉")
            self.assertEqual(after["stats"]["total"], before["stats"]["total"],
                             f"撞名不许往既有批次里塞任务（{_ASINS_2} 里有一个全新 ASIN）")
            self.assertEqual(after["stats"]["total"], len(_ASINS_1))

    def test_the_brand_new_asin_of_the_conflicting_upload_is_not_queued(self):
        """B00CONF999 从没在第一批里出现过，撞名之后它也不该出现在任何地方。"""
        with isolated_server() as (client, _ctx):
            self.assertEqual(_upload(client, _ASINS_1).status_code, 200)
            self.assertEqual(_upload(client, _ASINS_2).status_code, 409)
            r = client.get("/api/tasks/pull", params={
                "worker_id": "conf-w", "count": 50, "enable_screenshot": "false"})
            self.assertEqual(r.status_code, 200, r.text)
            asins = sorted(t["asin"] for t in r.json()["tasks"])
            self.assertEqual(asins, sorted(_ASINS_1))

    # ---------------- 重试安全性（推翻「POST 永不自动重试」） ----------------

    def test_post_upload_is_safe_to_retry(self):
        """同一个请求连发三次：一个 200 两个 409，全程只有**一个**批次。

        这正是「网络超时后可以直接重发」的形式化版本：重试既不会造出第二个批次，
        也不会把内容并进第一个，而且每次都能拿回同一个 batch_id。
        """
        with isolated_server() as (client, _ctx):
            first = _upload(client, _ASINS_1, external_id="ext-1")
            self.assertEqual(first.status_code, 200, first.text)
            batch_id = first.json()["batch_id"]

            for attempt in range(2):
                with self.subTest(retry=attempt):
                    again = _upload(client, _ASINS_1, external_id="ext-1")
                    self.assertEqual(again.status_code, 409, again.text)
                    self.assertEqual(again.json()["detail"]["batch_id"], batch_id)

            names = [b["name"] for b in client.get("/api/batches").json()["batches"]]
            self.assertEqual(names.count(_NAME), 1, names)

    def test_two_hundred_now_means_a_new_batch_was_created(self):
        """200 恒等于「新建了一个批次」—— 旧坑 c（inserted 语义不明）就此消失。

        两个不同的批次名各拿一个 200，各自是独立批次；同名的第二发是 409。
        于是调用方判定「建成了吗」只需要看状态码，不必再去解读 ``inserted``。
        """
        with isolated_server() as (client, _ctx):
            a = _upload(client, _ASINS_1, name="conf_a")
            b = _upload(client, _ASINS_1, name="conf_b")     # 同样的 ASIN，另一个名字
            self.assertEqual(a.status_code, 200, a.text)
            self.assertEqual(b.status_code, 200, b.text)
            self.assertNotEqual(a.json()["batch_id"], b.json()["batch_id"])
            # 同一批 ASIN 进第二个批次，inserted 照样是 2（batch_asins/tasks 按批次唯一）
            self.assertEqual(b.json()["inserted"], len(_ASINS_1))

    # ---------------- 回显不撒谎 ----------------

    def test_200_echoes_what_was_actually_stored(self):
        """200 的 external_id / callback_url 回显的是**存下来的**值。

        ``external_id`` 服务端会截到 120 字符——回显请求值时这个截断看不见，
        回显存储值时它必须显出来。这条同时钉住「回显的是哪一份」这件事本身。
        """
        long_ext = "E" * 200
        with isolated_server() as (client, _ctx):
            r = _upload(client, _ASINS_1, external_id=long_ext, callback_url=_CB_1)
            self.assertEqual(r.status_code, 200, r.text)
            body = r.json()
            self.assertEqual(len(body["external_id"]), 120,
                             "回显的必须是存下来的（已截断到 120），不是请求里那 200 个字符")
            self.assertEqual(body["callback_url"], _CB_1)

            status = client.get(f"/api/batches/{_NAME}/status").json()
            self.assertEqual(body["external_id"], status["external_id"])
            self.assertEqual(body["callback_url"], status["callback"]["url"])


class CreateBatchCompatibilityTests(unittest.TestCase):
    """``create_batch`` 的既有签名与既有行为一个字都没变。

    自动调度器（``_auto_scrape_scheduler`` / ``/api/schedules/{name}/run``）与
    卖家批次都走 ``create_batch``，它们**不该**被 409 这件事波及 —— 它们要的
    就是「拿到那个批次的 id」，撞名与否无所谓。所以撞名判定做成了新方法
    ``create_batch_if_absent``（真源），``create_batch`` 退化成丢掉第二个返回值
    的薄壳，签名与返回类型一字未动。

    ⚠ 本文件里**一律不直接 await 那两个 db 方法**：``isolated_server()`` 内部
    对 PG 会 ``asyncio.run`` 去建 scratch 库，从一个已经在跑的事件循环里进它
    会当场 ``RuntimeError``；而 ``srv.db`` 的连接又绑在 TestClient 自己那个
    循环上，换个循环去用它在 asyncpg 下必炸。所以行为断言全部走 HTTP
    （撞名的 id 语义在 409 响应体里看得见），签名断言纯静态。
    PG 侧对 ``create_batch_if_absent`` 的直接单元覆盖在
    ``tests/pgdb/test_batches.py``。
    """

    def test_create_batch_signature_is_untouched(self):
        """既有调用方按关键字传参，改名/改默认值就是破坏契约。跟着 DB_BACKEND 走。"""
        import inspect

        from common.dbfactory import get_database_class

        cls = get_database_class()
        expected = [
            ("self", inspect.Parameter.empty), ("name", inspect.Parameter.empty),
            ("needs_screenshot", False), ("is_auto", False),
            ("external_id", None), ("callback_url", None),
            ("expand_variants", False),
        ]
        self.assertEqual(
            [(p.name, p.default)
             for p in inspect.signature(cls.create_batch).parameters.values()],
            expected, "create_batch 的签名是既有调用方的契约，本轮不许动")
        self.assertEqual(
            [(p.name, p.default)
             for p in inspect.signature(cls.create_batch_if_absent).parameters.values()],
            expected, "新方法与 create_batch 同签名，只有返回值多一位")

    def test_create_batch_if_absent_is_in_the_public_api_contract(self):
        """新增 db 方法必须进 PUBLIC_API（否则两侧签名一致性根本不会被检查）。

        ``common/pgdb/__init__.py`` 的三道导入期断言已经在 import 时把这条
        变成硬约束；这里再钉一次，是为了让「忘了登记」在**看得懂的用例名**下
        失败，而不是变成一句 import 期的 AssertionError。
        SQLite-only 部署（装不上 asyncpg）跳过。
        """
        try:
            from common.pgdb import PUBLIC_API
        except ImportError as e:                      # pragma: no cover
            self.skipTest(f"pgdb 不可用: {e}")
        self.assertIn("create_batch_if_absent", PUBLIC_API)

    def test_conflict_still_reuses_the_existing_id_and_still_burns_one(self):
        """两条既有性质，全部经 HTTP 观察：

        1. 撞名返回的是**既有**批次的 id（自动调度器依赖它，409 响应体也依赖它）；
        2. 撞名那一发**照样烧号** —— 黄金基线的 ``golden_batch_b`` 是 id 3
           而不是 2 就是这么来的。改成「先 SELECT 再决定插不插」会当场把基线打红。
        """
        with isolated_server() as (client, _ctx):
            first = _upload(client, _ASINS_1, name="burn_a")
            self.assertEqual(first.status_code, 200, first.text)
            base_id = first.json()["batch_id"]

            dup = _upload(client, _ASINS_1, name="burn_a")
            self.assertEqual(dup.status_code, 409, dup.text)
            self.assertEqual(dup.json()["detail"]["batch_id"], base_id)

            nxt = _upload(client, _ASINS_1, name="burn_b")
            self.assertEqual(nxt.status_code, 200, nxt.text)
            self.assertEqual(
                nxt.json()["batch_id"], base_id + 2,
                "撞名那一发必须烧掉一个自增号（两个后端实测一致的既有行为）")


if __name__ == "__main__":
    unittest.main()
