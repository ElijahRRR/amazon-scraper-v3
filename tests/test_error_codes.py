"""错误码注册表的漂移守卫。

`server/api/sync.py:_err` 的第二个位置实参是**机器读**的稳定枚举（契约 §5.5 的伪码
就是 `ALARM(r.error)`）。它今天散在 46 处调用点上，靠人眼保持一致——这个文件把它
变成封闭集 + 静态守卫，模式照抄 `tests/pgdb/test_sync_api.py::test_valid_outcomes_matches_the_schema`。

三件事：

1. **调用点 ⊆ ERROR_CODES**：AST 扫 `server/` 全树，取每处 `_err(...)` 的第二个位置实参。
2. **文档里出现的码 ⊆ ERROR_CODES**：方向性守卫。**刻意不做双向相等**——
   `docs/sync_contract.md` 的状态码表只覆盖 `/records` 一个端点，而 `gen_mismatch`、
   `ack_ahead_of_stream` 属于 `/ack`、`/ack-prune`，双向比对会当场假红。
3. **`_err` 的 `extra` 不许覆盖保留键**：钉住那个地雷的修法（剔除，不是 raise）。

纯静态分析 + 一次纯函数调用，不碰 DB，两个后端跑出来一样。
"""
from __future__ import annotations

import ast
import os
import re
import unittest

from common import config
from server.api.sync import ERROR_CODES, _ERR_RESERVED_KEYS, _err

SERVER_DIR = os.path.join(config.PROJECT_DIR, "server")
DOCS_DIR = os.path.join(config.PROJECT_DIR, "docs")

#: 码的字面形状：全小写 + 下划线。用来把文档里的反引号片段筛成「像个码的东西」。
_CODE = r"[a-z][a-z0-9_]*"


def _err_call_sites():
    """产出 `(相对路径, 行号, 第二个位置实参节点或 None)`。

    两种调用形态都要认：`sync.py` 内部的 `_err(...)`（`ast.Name`）与
    `export_incremental.py` 的 `_sync._err(...)`（`ast.Attribute`）。
    """
    sites = []
    for root, _dirs, files in os.walk(SERVER_DIR):
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            path = os.path.join(root, fname)
            rel = os.path.relpath(path, config.PROJECT_DIR)
            with open(path, encoding="utf-8") as fh:
                tree = ast.parse(fh.read(), filename=path)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                if isinstance(func, ast.Name):
                    name = func.id
                elif isinstance(func, ast.Attribute):
                    name = func.attr
                else:
                    continue
                if name != "_err":
                    continue
                arg = node.args[1] if len(node.args) > 1 else None
                sites.append((rel, node.lineno, arg))
    return sites


def _documented_codes():
    """从 `docs/*.md` 里捞出「以错误码身份出现」的字符串。

    三种出现形态，**只认这三种**——放宽到「所有反引号片段」会把 `postgres`、
    `DB_BACKEND` 这类词当成码，守卫就会假红：

    * JSON 例子里的 ``"error": "xxx"``
    * 状态码后面紧跟的反引号词：``409 `gen_mismatch` ``
    * 表头有一列正好是 `` `error` `` 的 markdown 表，取那一列
    """
    found = {}
    for fname in sorted(os.listdir(DOCS_DIR)):
        if not fname.endswith(".md"):
            continue
        with open(os.path.join(DOCS_DIR, fname), encoding="utf-8") as fh:
            text = fh.read()
        codes = set()
        codes.update(re.findall(r'"error"\s*:\s*"(%s)"' % _CODE, text))
        codes.update(re.findall(
            r"\b(?:400|401|403|404|409|422|429|500|503)\s+`(%s)`" % _CODE, text))
        col = None
        for line in text.split("\n"):
            if not line.strip().startswith("|"):
                col = None
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if "`error`" in cells:
                col = cells.index("`error`")
                continue
            if col is not None and col < len(cells):
                codes.update(re.findall(r"`(%s)`" % _CODE, cells[col]))
        for code in codes:
            found.setdefault(code, fname)
    return found


class ErrorCodeRegistryTests(unittest.TestCase):
    """两个后端都要跑：它守的是源码与文档，与后端无关。"""

    def test_the_scanner_actually_finds_the_call_sites(self):
        """先钉住扫描器自己没瞎。

        AST 守卫最常见的失效方式是「一处都没扫到，于是永远绿」——
        改了 `_err` 的名字/调用形态而没改本文件，就会落进这个坑。
        实测 46 处（`server/api/sync.py` 40 + `export_incremental.py` 6）。
        """
        sites = _err_call_sites()
        self.assertGreaterEqual(
            len(sites), 40,
            f"只扫到 {len(sites)} 处 _err 调用点，扫描器多半失效了")

    def test_every_err_call_site_uses_a_registered_code(self):
        """46 处调用点的第二个位置实参必属 ERROR_CODES，且必须是字面量。

        非字面量（变量/f-string）同样算失败：码一旦能在运行期拼出来，
        这个封闭集就守不住任何东西了。
        """
        offenders = []
        for rel, lineno, arg in _err_call_sites():
            if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
                offenders.append((rel, lineno, "<非字面量或缺参>"))
            elif arg.value not in ERROR_CODES:
                offenders.append((rel, lineno, arg.value))
        self.assertEqual(
            offenders, [],
            f"这些 _err 调用点用了不在 ERROR_CODES 里的码: {offenders}")

    def test_documented_codes_are_a_subset_of_the_registry(self):
        """文档里出现的码 ⊆ ERROR_CODES（**单向**，理由见文件头第 2 条）。"""
        documented = _documented_codes()
        self.assertTrue(documented, "一个文档码都没捞到，提取器多半失效了")
        offenders = sorted((code, where) for code, where in documented.items()
                           if code not in ERROR_CODES)
        self.assertEqual(
            offenders, [],
            f"文档里写了 ERROR_CODES 里没有的码: {offenders}")

    def test_internal_error_is_registered(self):
        """全局 500 处理器（`server/app.py`）用的码，不经过 `_err`，容易被漏掉。"""
        self.assertIn("internal_error", ERROR_CODES)

    def test_batch_name_conflict_is_registered(self):
        """`POST /api/upload` 撞名 409 用的码，同样不经过 `_err`。

        它是 `HTTPException(409, {...})`，形状是 FastAPI 的
        `{"detail": {"error": ..., ...}}`，与 `_err` 的扁平体不同，所以文件头
        第 1 条那道 AST 扫描对它是**结构性**盲区。这条断言是它唯一的注册守卫：
        直接比对 `server/app.py` 里那个常量的**取值**，而不是重抄一遍字面量，
        免得两边各改各的还双双绿着。
        """
        from server.app import _BATCH_NAME_CONFLICT_CODE

        self.assertIn(_BATCH_NAME_CONFLICT_CODE, ERROR_CODES)

    def test_extra_cannot_clobber_the_reserved_keys(self):
        """`_err` 的 `extra` 撞上保留键时被**剔除**，不是覆盖、也不是抛。

        抛的话，一个本该回 409 `cursor_below_retention` 的请求会变成 500 ——
        用错误处理的 bug 换掉一次正确的错误处理。
        """
        self.assertEqual(_ERR_RESERVED_KEYS,
                         frozenset({"error", "detail", "server_time_utc"}))
        # `detail` 进不了 `**extra`（它是 `_err` 的具名形参，同名 kwarg 直接
        # TypeError），留在集合里纯属防御：哪天签名变了也不会突然可覆盖。
        resp = _err(409, "cursor_below_retention", "人读的说明",
                    error="oops", server_time_utc="oops",
                    min_available_seq=7)
        import json
        body = json.loads(bytes(resp.body).decode("utf-8"))
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(body["error"], "cursor_below_retention")
        self.assertEqual(body["detail"], "人读的说明")
        self.assertNotEqual(body["server_time_utc"], "oops")
        self.assertEqual(body["min_available_seq"], 7, "非保留键必须照常带上")


if __name__ == "__main__":
    unittest.main()
