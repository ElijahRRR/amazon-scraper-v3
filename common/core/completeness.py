"""common/core/completeness.py —— ``completeness`` 位图的唯一真源（契约 §6.4）。

**零依赖**：本模块不 import 任何东西（连标准库都不需要）。这是硬要求，不是巧合——
``worker/parser.py`` import 它，而 worker 的运行环境不装 aiosqlite / asyncpg。
同理它**不放在 ``common/pgdb/schema.py``**：那是 PG 后端的 DDL 模块，
让 worker 去 import 它就是把数据库驱动拖进采集进程，且分层方向整个倒过来
（worker 不该依赖 pgdb）。

------------------------------------------------------------------------
位的含义（**已对外公布，不许改数值**）
------------------------------------------------------------------------
判据是 **HTML 区块是否存在**，不是解析出来的值是否非空。二者不等价：
「区块在但里面是空的」和「区块被整块删掉」解析值完全一样（都是 ""/"N/A"），
而后者正是软降级页的特征。category 只有面包屑一个数据源，好页→降级页→好页
于是就是两次哈希翻转、两次误复审 —— §6.5 那道合取门要拦的就是这个。

    bit0 = 1  BREADCRUMB  面包屑区块存在
    bit1 = 2  DETAIL      详情表存在
    bit2 = 4  IMAGE       主图集存在
    bit3 = 8  MEASURED    这次采集**真的测量过**上面三项

``0`` 表示 **未测量**，不是「三项都缺」。「测过且三项都缺」是 ``8``，两者必须
能区分 —— 契约 §6.4 用整段写明了这个区别。

------------------------------------------------------------------------
Phase 4.5：为什么要收成一份
------------------------------------------------------------------------
收口之前判据有**两份独立实现**：``server/api/sync.py`` 的 ``_completeness_ok``
与 ``server/api/export_incremental.py:_to_record`` 里的手工重算。
契约 §4.3 的判据改一次要改两处，漏一处就是
``/api/v1/sync/records.completeness_ok`` 与 ``/api/export/incremental.completeness_ok``
**对同一行给出不同答案** —— 两个消费者、两个结论，而且没有任何一侧会报错。
现在两处都调本模块的 ``completeness_ok()``，走同一条代码。

⚠ **这句话是风险陈述，不是「修好了一个已发生的 bug」**（Phase 4.5-4.8 审计要求写清）：
收口**之前**两侧的答案其实是一致的 —— 旧的 export_incremental 算
``(c & 8)`` 与 ``(c & 7) == 7``，与 sync 的 ``int(v or 0)`` 版本对任何整数输入
都是同一个算式，而 ``completeness`` 是 PG 的 integer 列、asyncpg 恒回 int，
走不到类型差异那一支。
所以 4.5 消掉的是**将来判据改动时漏改一处**的风险，不是当时线上已经存在的分歧。
（收口后已实测：16 种取值逐行比对两个端点，DISAGREEMENTS: 0，且是同一个函数对象。）

位常量本身此前有三份（``worker/parser.py`` 的 ``COMPLETENESS_*``、
``common/pgdb/schema.py`` 的 ``EVENT_COMPLETENESS_*``、``server/api/sync.py`` 的
``COMPLETENESS_MEASURED_BIT`` / ``COMPLETENESS_REQUIRED_MASK``）。三处**各自保留
原有别名**（对外可见的导入名一个不动），只把**值的来源**换成这里。
``common/pgdb/relay.py:134-135`` 从 schema 导入 ``EVENT_COMPLETENESS_*``，
别名保留 ⇒ 它一个字节都不用改。

⚠ ``worker/parser.py`` 那边只换常量的**来源**，不换任何数值、不换任何逻辑：
解析产出必须逐字节不变（C3 / D-27），已实测（改前改后 sha256 相同）。
"""

#: bit0 —— 面包屑区块存在。
BREADCRUMB = 1
#: bit1 —— 详情表存在。
DETAIL = 2
#: bit2 —— 主图集存在。
IMAGE = 4
#: bit3 —— 这次采集真的测量过上面三项。
MEASURED = 8

#: 四位全置的上界。relay 的归一化用它做值域检查（``0 <= v <= MAX``）。
MAX = 15

#: 「内容齐全」需要的三位（不含 MEASURED）。**是 BREADCRUMB|DETAIL|IMAGE，
#: 不是随手写的 7** —— 将来加位时这行会自动跟上，写死的 7 不会。
REQUIRED_MASK = BREADCRUMB | DETAIL | IMAGE


def completeness_ok(value) -> bool:
    """契约 §4.3 的 ``completeness_ok``：**合取式**。

    既要「测过」（MEASURED 位置上），又要「三块齐全」（低三位全置）。
    两个条件缺一不可，这正是它不能写成 ``value == MAX`` 的原因——
    那样写在**将来加第五位**的时候会静默变成恒 False。

    ``None`` 与 0 都当成「未测量」⇒ False。
    """
    v = int(value or 0)
    return bool(v & MEASURED) and (v & REQUIRED_MASK) == REQUIRED_MASK


__all__ = [
    "BREADCRUMB",
    "DETAIL",
    "IMAGE",
    "MEASURED",
    "MAX",
    "REQUIRED_MASK",
    "completeness_ok",
]
