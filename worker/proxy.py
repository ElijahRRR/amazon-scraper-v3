"""
Amazon ASIN 采集系统 v3 - 代理管理模块

TPS 模式（每次请求换 IP）：
- 固定代理地址（帐密认证），如 http://user:pwd@host:port
- 每次 HTTP 请求通过代理自动获取不同出口 IP
- 无需多通道/channel 管理，简单可靠
"""
import asyncio
import time
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
        self.mode = "tps"
        self._proxy_url: str = config.PROXY_URL or ""
        self._total_blocked = 0
        self._total_requests = 0

        if self._proxy_url:
            # 隐藏密码输出
            masked = self._proxy_url.split("@")[-1] if "@" in self._proxy_url else self._proxy_url
            logger.info(f"代理初始化: TPS 模式, proxy=***@{masked}")
        else:
            logger.warning("代理未配置 (PROXY_URL 为空)")

    # ==================== 公共接口 ====================

    async def get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """获取代理地址 (proxy_url, None)"""
        self._total_requests += 1
        return self._proxy_url or None, None

    async def report_blocked(self, channel: int = None):
        """报告被封锁（TPS 模式下 IP 自动换，只记录统计）"""
        self._total_blocked += 1
        logger.warning(f"代理被封 (第 {self._total_blocked} 次)，下次请求将自动换 IP")

    async def wait_for_rotation(self):
        """TPS 模式无需等待轮换，每次请求自动换 IP"""
        await asyncio.sleep(2)

    def get_available_channel(self) -> Optional[int]:
        """TPS 模式无 channel"""
        return None

    def all_channels_blocked(self) -> bool:
        return False

    def get_channel_proxy_url(self, channel_id: int) -> Optional[str]:
        return self._proxy_url

    def get_tunnel_proxy_url(self) -> str:
        return self._proxy_url

    async def init_tunnel(self):
        """兼容接口：TPS 模式直接使用 PROXY_URL"""
        if not self._proxy_url:
            self._proxy_url = config.PROXY_URL
        if self._proxy_url:
            logger.info(f"代理就绪: {self._proxy_url.split('@')[-1]}")
            return 1
        logger.error("PROXY_URL 未配置!")
        return 0

    def switch_mode(self, new_mode: str):
        pass

    def reconfigure_tunnel(self, channels: int, rotate_interval: int):
        pass

    async def change_ip(self) -> bool:
        """TPS 模式每次请求自动换 IP，无需手动操作"""
        return True

    async def handle_ip_rotation(self):
        """TPS 模式无 IP 轮换概念"""
        return False

    def time_to_next_rotation(self) -> float:
        return 0

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
_proxy_manager_lock = asyncio.Lock()


async def get_proxy_manager_async() -> ProxyManager:
    global _proxy_manager
    if _proxy_manager is None:
        async with _proxy_manager_lock:
            if _proxy_manager is None:
                _proxy_manager = ProxyManager()
    return _proxy_manager


def get_proxy_manager() -> ProxyManager:
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
