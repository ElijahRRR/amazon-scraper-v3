"""common/core/dbtables.py —— 两个后端共用的表清单 / 分块大小 / LIKE 模式（唯一真源）。

* ``CLEAR_TABLES`` —— ``clear_all_data`` 的删除清单。两侧各有一份实现
  （SQLite 删 ``sqlite_sequence``、PG 做 identity RESTART），但**删哪些表**
  必须是同一份。
* ``ASIN_DELETE_CHUNK`` / ``ASIN_DELETE_TABLES`` —— ``delete_asins`` 的分块大小
  与表清单。分块边界分叉 = 两个后端发出的语句序列不同，出事时对不上号。
* ``search_like_pattern`` —— ``%term%``，**不转义**。读路径（``get_results``）
  与删除路径（``find_asins_by_search``）必须用同一个，否则同一个 search
  在 GET 和 DELETE 下选中不同的行（D-16 记的就是这个事故）。

注意：SQLite 侧的 ``SEARCH_TERM_OR``（带 ``?`` 占位符的 SQL 片段）**不在这里**，
它留在 ``common/database.py``——那是方言相关的 SQL，不是共享的纯 Python 常量。
"""

# `DELETE /api/database` 清库的删除顺序（Database.clear_all_data 用）。
# 子表在前、父表在后 —— PG 侧有真外键，顺序在那边是正确性问题；SQLite 侧
# 顺序无所谓。两个后端**共用这一份**（pgdb 经 common/pgdb/_shared.py 再导出），
# 分叉 = 两个后端「清空」之后剩下的东西悄悄不同。
CLEAR_TABLES = ("asin_changes", "asin_data", "batch_asins", "tasks",
                "screenshots", "batches")

# 按 ASIN 删除时的分块大小。SQLite 的 SQLITE_MAX_VARIABLE_NUMBER 默认 999，
# 一条 `IN (?,?,...)` 不能超过它；500 是安全值。PG 没有这个限制，但**必须
# 用同一个值**：分块边界不同 = 两个后端发出的语句序列不同，一旦某一块中途
# 失败（两侧都在一个事务里，所以只会整体回滚），排障时对不上号。
ASIN_DELETE_CHUNK = 500

# 按 ASIN 删除要清的四张表（顺序：子表在前）。asin_data 最后 —— PG 侧
# asin_changes / screenshots / batch_asins 都可能引用它。
ASIN_DELETE_TABLES = ("asin_changes", "screenshots", "batch_asins", "asin_data")


def search_like_pattern(t: str) -> str:
    """``%term%``，**不做任何转义**。

    SQLite 的 LIKE 没有转义字符，所以模式原样传下去；PG 侧靠 SQL 里的
    ``ESCAPE ''`` 关掉默认的反斜杠转义来对齐（决策 D-16）。
    ``%`` 与 ``_`` 故意不转义：用户输入里的通配符今天就是生效的
    （``Gol%rand`` 是模糊匹配），两个引擎的元字符一样，这个行为免费保留。
    """
    return "%" + str(t) + "%"


__all__ = [
    "CLEAR_TABLES",
    "ASIN_DELETE_CHUNK",
    "ASIN_DELETE_TABLES",
    "search_like_pattern",
]
