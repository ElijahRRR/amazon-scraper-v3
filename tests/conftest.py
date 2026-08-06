"""tests/ 全树共用的 pytest 配置。

**这里现在是空的，这是刻意的结果，不是忘了写。** 下面记录为什么。

## 曾经有什么

这里曾有一个 autouse 夹具 ``_restore_current_event_loop``：每个用例跑完，
把被置空的「当前线程事件循环」补回去。它服务的是唯一一个受害者——
``tests/test_session_slot.py`` 的 ``run()`` 助手，它当时写作
``asyncio.get_event_loop().run_until_complete(coro)``，依赖那个**进程级全局槽位**。

置空该槽位的路径都是正当用法（``asyncio.run(...)`` 结束时关闭循环并置空）：

* ``tests/golden/harness.py:206-207`` ``_pg_scratch_db`` 建库/删库
* ``tests/test_golden_with_relay.py:86`` 抽干事件流
* ``pytest-asyncio`` 每个 async 用例

它们按字母序都排在 ``test_session_slot.py`` 之前，于是那 31 个用例是否通过，
成了「收集顺序 × 后端 × runner」的函数。

## 为什么这个修法是错的（B6）

``conftest.py`` 是 **pytest 专属**的，``unittest`` 根本不读它。而
``unittest discover -s tests`` 是本仓库最早的 runner，两个后端都必须能跑。
结果就是同一份代码两个 runner 两种结论——实测于 HEAD ``fea7395``：

    DB_BACKEND=postgres python -m unittest discover -s tests
      -> Ran 51 tests ... FAILED (errors=26, skipped=4)
         26 × RuntimeError: There is no current event loop in thread 'MainThread'
    DB_BACKEND=postgres python -m pytest tests/ -q
      -> 429 passed, 4 skipped                      ← 夹具把同一个缺陷盖住了

SQLite 下那两个文件走 skipTest、不跑 ``asyncio.run``，所以只有 Postgres 侧翻红。

一个「只在 pytest 下生效的修复」并不能让缺陷消失，只能让它对 pytest 隐身。
留着它，下一个依赖全局槽位的用例还会重演一遍 B6：pytest 绿、unittest 红。

## 现在的修法

根因消灭在源头：``tests/test_session_slot.py`` 的 ``run()`` 改为自持一个事件循环，
不再读那个全局槽位（那里有完整注释）。全树 ``grep -rn get_event_loop tests/``
此后只剩注释，**没有任何用例依赖当前循环槽位**。

于是这份夹具成了死代码，予以删除；``tests/pgdb/conftest.py`` 里那份同样如此。
删除是经过实测的，不是推断——把两份夹具同时停用、只留根因修复：

    pytest tests/ -q                     -> 427 passed, 6 skipped
    DB_BACKEND=postgres pytest tests/ -q -> 429 passed, 4 skipped

反证同样做了（还原 ``run()``、仍停用两份夹具）：

    DB_BACKEND=postgres pytest tests/ -q -> 26 failed, 403 passed, 4 skipped

即：这两份夹具当年确实在干活，只是干的是「替 pytest 遮住缺陷」这份活。

## 留给后来者的约束

不要在测试里用 ``asyncio.get_event_loop()``。要跑协程就用 ``asyncio.run(...)``，
或者像 ``test_session_slot.py`` 那样自己持有循环。这条约束由
``tests/test_runner_parity.py::EventLoopOwnershipTests`` 看守，它是 unittest
用例，两个 runner 都会执行。
"""
