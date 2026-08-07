"""common/core/idents.py —— 标识符（ASIN 等）的正则真源。

只依赖标准库 ``re``。**worker 侧也 import 本模块**（worker/parser.py），
所以这里永远不许 import ``common.database`` / ``common.pgdb`` / 任何第三方包：
worker 的运行环境不装 aiosqlite / asyncpg。

------------------------------------------------------------------------
``ASIN_RE`` —— 收口的是哪两处
------------------------------------------------------------------------
Phase 4.2 之前有两份**逐字节相同**的 ``re.compile(r'^B[0-9A-Z]{9}$')``：

* ``server/app.py:_ASIN_RE``    —— ``_normalize_asin``，上传/查询的入口校验；
* ``worker/parser.py:_ASIN_RE`` —— ``parse_seller_listing``，卖家列表页里
  ``data-asin`` 的过滤。

两者过滤的是同一个东西（「这串字符是不是一个 ASIN」），分叉了就会出现
「服务端收得下、worker 认不出」或者反过来的静默丢数据。现在两边都从这里 import。

**行为逐字保留，包括下面这两条容易被当成 bug 的地方**——它们今天两份副本里都在，
本次搬迁不修，改了就是行为变更：

* ``$`` 不是 ``\\Z``：``"B0G6KPHQ4G\\n"`` **能**匹配（Python 的 ``$`` 允许
  末尾一个换行）。要收紧得单独立项，别在搬迁里顺手改。
* 正则**自己不做大小写归一**，``[0-9A-Z]`` 只认大写。归一在调用侧：
  ``server/app.py:_normalize_asin`` 与 ``worker/parser.py:parse_seller_listing``
  都先 ``.strip().upper()``，而 ``worker/parser.py:_extract_page_asin`` 的
  **regex 兜底分支**（canonical / currentAsin）拿原始 HTML 直接匹配、**不归一**,
  它的 selectolax 分支却归一。那是**调用侧**的真分叉，Phase 4.2 只登记不改。

------------------------------------------------------------------------
⚠ 不要「顺手统一」进来的东西
------------------------------------------------------------------------
``server/api/export.py:_VARIANT_PAGE_ASIN_RE`` = ``re.compile(r"\\bpage=([A-Z0-9]{10})\\b", re.IGNORECASE)``
**是另一条规则，故意留在原地**，三处都不一样：

  1. 它是**搜索**用的（``\\b...\\b`` 锚在词边界），不是整串校验（``^...$``）；
  2. 它**不要求 B 前缀**，是 ``[A-Z0-9]{10}``；
  3. 它带 ``re.IGNORECASE``。

它从 ``variant_offset`` 的 ``error_detail`` 自由文本里捞 ``page=XXXXXXXXXX``，
输入是我们自己拼的诊断串、不是用户提交的 ASIN。把它换成 ``ASIN_RE`` 会让
所有非 B 开头的 variant 诊断串**静默**捞不到值（导出列「实际页面ASIN」变空），
而黄金基线里没有这一步、不会响。export.py 那边也留了反向指针。

``worker/parser.py:_extract_page_asin`` 里的局部 ``ASIN_RE = r'B[0-9A-Z]{9}'``
同样**不搬**：它是个**无锚点的裸片段**，专门用来拼进更大的正则
（``<input ... value="(' + ASIN_RE + ')"`` 等四处）。它和本模块的编译对象
不是一个类型、也不是一个用途，"统一"它需要先把那四条拼接正则改写，
属于行为改动，不在搬迁范围内。
"""
import re

#: 整串 ASIN 校验：``B`` + 9 位大写字母/数字。见上方 docstring 的两条保留行为。
ASIN_RE = re.compile(r'^B[0-9A-Z]{9}$')

__all__ = ["ASIN_RE"]
