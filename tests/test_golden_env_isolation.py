"""黄金基线必须与「跑测试的这台机器」无关。

起因是一次真实的误报：开发机的 `.env` 里配了 `PROXY_URL`，同一份 64 步录制在
两个后端上都报 4 处差异（`settings.proxy_url` 出现在 `/api/settings`、
`/api/worker/sync`、`/api/settings/reset` 前后共四步里）。代码一个字节没改，
差异纯粹来自环境——沙箱里没配代理所以一直是绿的。

这类漏洞的危险不在于误报本身，而在于**方向**：夹具隔离不全时，基线在配了环境
变量的机器上永远红，在没配的机器上永远绿，两边都会开始不信任这份基线。

所以这里不测「跑一遍看看绿不绿」（那只能验证当前这台机器），而是 AST 扫描出
「`_default_settings()` 用到的、且由环境变量驱动的」config 属性全集，要求它被
`harness._ENV_DERIVED_SETTINGS` 完全覆盖。以后谁再加一个 env 驱动的设置项，
这条会在他合并之前失败，而不是等某台机器上的基线莫名其妙变红。
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from tests.golden.harness import _ENV_DERIVED_SETTINGS  # noqa: E402


def _module_ast(rel_path: str) -> ast.Module:
    with open(os.path.join(_ROOT, rel_path), encoding="utf-8") as f:
        return ast.parse(f.read(), filename=rel_path)


def _reads_environ(node: ast.AST) -> bool:
    """RHS 里是否出现 os.environ（含 int(os.environ.get(...)) 这类包装）。"""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == "environ":
            return True
    return False


def _literal_default(node: ast.AST):
    """RHS 恰好是 os.environ.get(KEY, <常量>) 时返回那个常量，否则返回 _NO_DEFAULT。"""
    if not (isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and _reads_environ(node.func)
            and len(node.args) == 2):
        return _NO_DEFAULT
    try:
        return ast.literal_eval(node.args[1])
    except ValueError:
        return _NO_DEFAULT


_NO_DEFAULT = object()


def env_driven_config_names() -> dict:
    """common/config.py 里所有「模块级、由环境变量赋值」的属性名 -> 声明的默认值。"""
    out = {}
    for stmt in _module_ast("common/config.py").body:
        if not (isinstance(stmt, ast.Assign) and _reads_environ(stmt.value)):
            continue
        for target in stmt.targets:
            if isinstance(target, ast.Name):
                out[target.id] = _literal_default(stmt.value)
    return out


def config_names_used_by_default_settings() -> set:
    """server/app.py 的 _default_settings() 里读到的 config.<NAME> 全集。"""
    tree = _module_ast("server/app.py")
    fn = next((n for n in tree.body
               if isinstance(n, ast.FunctionDef) and n.name == "_default_settings"), None)
    assert fn is not None, "_default_settings 不见了——本文件的前提失效，请更新"
    return {
        sub.attr for sub in ast.walk(fn)
        if isinstance(sub, ast.Attribute)
        and isinstance(sub.value, ast.Name) and sub.value.id == "config"
    }


class GoldenEnvIsolationTests(unittest.TestCase):

    def test_every_env_driven_setting_is_pinned_by_the_harness(self):
        leaking = env_driven_config_names().keys() & config_names_used_by_default_settings()
        missing = leaking - _ENV_DERIVED_SETTINGS.keys()
        self.assertEqual(
            missing, set(),
            "这些 config 属性由环境变量驱动、又会出现在 /api/settings 的响应里，"
            "但夹具没钉住它们 —— 基线会跟着机器的 .env 变红/变绿。"
            f"请在 tests/golden/harness.py 的 _ENV_DERIVED_SETTINGS 里补上：{sorted(missing)}")

    def test_pinned_values_match_the_declared_defaults(self):
        """钉住的值必须等于 config.py 声明的默认值。

        钉成别的值同样能让基线稳定，但那样重放的就不是**录制时**的形态了：
        基线是在环境变量缺省的机器上录的，钉别的值等于悄悄换掉被比较的对象。
        """
        declared = env_driven_config_names()
        for name, pinned in _ENV_DERIVED_SETTINGS.items():
            self.assertIn(name, declared,
                          f"{name} 已经不是环境变量驱动的了，_ENV_DERIVED_SETTINGS 该删掉它")
            if declared[name] is not _NO_DEFAULT:
                self.assertEqual(
                    pinned, declared[name],
                    f"{name} 钉的是 {pinned!r}，但 config.py 声明的默认值是 {declared[name]!r}")

    def test_the_scan_actually_finds_something(self):
        """自检：扫描逻辑一旦静默失效（比如 AST 结构变了），上面两条会假绿。"""
        self.assertIn("PROXY_URL", env_driven_config_names())
        self.assertIn("PROXY_URL", config_names_used_by_default_settings())

    def test_harness_pins_before_lifespan_runs(self):
        """钉住必须发生在 TestClient 进入 lifespan（其中 _load_settings）之前。

        顺序错了照样会漏：`_load_settings()` 在 lifespan 里把 `_default_settings()`
        的结果拷进 `_runtime_settings`，之后再改 config 已经没用了。
        """
        with open(os.path.join(_ROOT, "tests/golden/harness.py"), encoding="utf-8") as f:
            src = f.read()
        pin_at = src.index("_ENV_DERIVED_SETTINGS.items()")
        client_at = src.index("with TestClient(srv.app) as client")
        self.assertLess(pin_at, client_at,
                        "钉住 env 派生设置的代码跑到 TestClient 之后去了")


if __name__ == "__main__":
    unittest.main()
