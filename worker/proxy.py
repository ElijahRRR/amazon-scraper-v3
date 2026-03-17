"""
Amazon ASIN 采集系统 v3 - 代理管理模块

TPS 模式（每次请求换 IP）：
- 固定代理地址（帐密认证），如 http://user:pwd@host:port
- 每次 HTTP 请求通过代理自动获取不同出口 IP
"""
import asyncio
import logging
from typing import Optional, Dict, Tuple

from common import config

logger = logging.getLogger(__name__)


class ProxyManager:
    """
    TPS 代理管理器
    每次请求通过固定代理地址自动换 IP，无需复杂的通道管理
    """

    def __init__(self):
        self._proxy_url: str = config.PROXY_URL or ""
        self._total_blocked = 0
        self._total_requests = 0

        if self._proxy_url:
            masked = self._proxy_url.split("@")[-1] if "@" in self._proxy_url else self._proxy_url
            logger.info(f"代理初始化: TPS 模式, proxy=***@{masked}")
        else:
            logger.warning("代理未配置 (PROXY_URL 为空)")

    async def get_proxy(self) -> Optional[str]:
        """获取代理地址"""
        self._total_requests += 1
        return self._proxy_url or None

    async def report_blocked(self):
        """报告被封锁（TPS 模式下 IP 自动换，只记录统计）"""
        self._total_blocked += 1
        logger.warning(f"代理被封 (第 {self._total_blocked} 次)，下次请求将自动换 IP")

    def get_stats(self) -> Dict:
        masked = self._proxy_url.split("@")[-1] if "@" in self._proxy_url else self._proxy_url
        return {
            "mode": "tps",
            "proxy": f"***@{masked}" if self._proxy_url else "not configured",
            "total_requests": self._total_requests,
            "total_blocked": self._total_blocked,
        }


# ==================== 全局单例 ====================

_proxy_manager: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
