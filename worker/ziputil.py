"""
邮编生效判定（纯函数，无重依赖，便于单测）。

Tier 2a：把"切邮编后是否生效"的判断从一次独立的验证 GET，折叠到我们本来就要采的
商品页 HTML 上。核心信号是 Amazon 每个页面顶部的 glow 配送地址挂件
`id="glow-ingress-line2"`（形如 "New York 10001" / "Altoona 16602"）。
"""
import re
from typing import Optional

# glow 配送地址第二行：城市 + 邮编
_GLOW_LINE2_RE = re.compile(r'id="glow-ingress-line2"[^>]*>\s*([^<]+)')
# 非美国区域的货币标识（页面串区时出现）
_NON_US_CURRENCY = ("CNY", "¥", "€", "£", "JP¥")


def zip_effective_in_html(zip_code: str, text: str) -> Optional[bool]:
    """从任意 Amazon 页面 HTML 判断目标配送邮编是否已生效。

    返回：
      True  —— glow 挂件显示了目标邮编（已生效）
      False —— 明确未生效：glow 显示的是**其它**邮编，或页面出现非美国货币标识
      None  —— 无法判定：页面既无 glow 挂件、也无异常货币标识
               （调用方按"宽松通过"处理，与旧版独立验证 GET 的行为一致）

    关键：与 parser 的 `_slx_parse_zip_code` 不同，这里**不做**"取不到就回退成请求值"
    的兜底——取不到返回 None，取到但不符返回 False，避免把脏数据误判为已生效。
    """
    if not zip_code or not text:
        return None
    try:
        m = _GLOW_LINE2_RE.search(text)
        if m:
            location_text = m.group(1).strip()
            return zip_code in location_text
        head = text[:50000]
        for indicator in _NON_US_CURRENCY:
            if indicator in head:
                return False
        return None
    except Exception:
        return None
