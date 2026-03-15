"""
Amazon ASIN 采集系统 v3 - 代理管理模块

隧道代理模式：
- 固定隧道地址（帐密认证），如 http://user:pwd@host:port
- 多通道独立出口 IP（通过 password:channel_id 区分）
- 定时自动换 IP（由代理服务端处理）
- Session 槽位管理：被封时标记，轮询分发
"""
import asyncio
import time
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
from urllib.parse import urlparse

from common import config

logger = logging.getLogger(__name__)


@dataclass
class ChannelState:
    """单个会话槽位的运行时状态"""
    channel_id: int
    proxy_url: str = ""
    blocked: bool = False
    blocked_at: float = 0
    request_count: int = 0
    last_request_at: float = 0

    def reset_for_rotation(self):
        self.blocked = False
        self.blocked_at = 0
        self.request_count = 0


class ProxyManager:
    """
    隧道代理管理器
    通过 TUNNEL_PROXY_URL 配置代理地址，支持多通道并行
    """

    def __init__(self):
        self.mode = "tunnel"
        self._tunnel_proxy_url: str = config.TUNNEL_PROXY_URL or ""
        self._channels: Dict[int, ChannelState] = {}
        self._round_robin_index = 0
        self._rotation_at: float = 0
        self._all_blocked_event = asyncio.Event()
        self._all_blocked_event.set()
        self._tunnel_init_lock = asyncio.Lock()

        # 初始化通道
        num_channels = config.TUNNEL_CHANNELS
        for i in range(1, num_channels + 1):
            self._channels[i] = ChannelState(channel_id=i)
        self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL

        # 统计
        self._total_blocked = 0

        logger.info(f"代理管理器初始化: {num_channels} 通道, "
                    f"{config.TUNNEL_ROTATE_INTERVAL}s 轮换")

    @staticmethod
    def _build_channel_url(parsed, channel_id: int) -> str:
        """构建带通道号的代理 URL: password:channel_id"""
        return (
            f"http://{parsed.username}:{parsed.password}:{channel_id}"
            f"@{parsed.hostname}:{parsed.port}"
        )

    def switch_mode(self, new_mode: str):
        """兼容 v2 的 switch_mode 接口"""
        pass

    def reconfigure_tunnel(self, channels: int, rotate_interval: int):
        """运行时重配隧道参数"""
        channels = max(1, int(channels))
        rotate_interval = max(10, int(rotate_interval))

        old_channels = self._channels
        new_channels: Dict[int, ChannelState] = {}

        parsed = urlparse(self._tunnel_proxy_url) if self._tunnel_proxy_url else None

        for ch_id in range(1, channels + 1):
            channel_url = self._build_channel_url(parsed, ch_id) if parsed else ""
            if ch_id in old_channels:
                ch = old_channels[ch_id]
                ch.proxy_url = channel_url
                new_channels[ch_id] = ch
            else:
                new_channels[ch_id] = ChannelState(
                    channel_id=ch_id,
                    proxy_url=channel_url,
                )

        self._channels = new_channels
        self._round_robin_index = 0
        self._rotation_at = time.monotonic() + rotate_interval

        if all(not ch.blocked for ch in self._channels.values()):
            self._all_blocked_event.set()

        logger.info(f"隧道重配: channels={channels}, rotate={rotate_interval}s")

    # ==================== 公共接口 ====================

    async def get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """获取代理 (proxy_url, channel_id)"""
        if channel is None:
            channel = self.get_available_channel()
        if channel is None:
            return None, None

        ch_state = self._channels[channel]
        ch_state.request_count += 1
        ch_state.last_request_at = time.monotonic()
        return ch_state.proxy_url, channel

    async def report_blocked(self, channel: int = None):
        """报告被封锁"""
        self._total_blocked += 1
        if channel is None or channel not in self._channels:
            return
        ch_state = self._channels[channel]
        ch_state.blocked = True
        ch_state.blocked_at = time.monotonic()
        blocked_count = sum(1 for ch in self._channels.values() if ch.blocked)
        logger.warning(f"槽位 {channel} 被封 ({blocked_count}/{len(self._channels)})")

        if self.all_channels_blocked():
            self._all_blocked_event.clear()
            logger.error("全部槽位被封，等待自动轮换...")

    async def wait_for_rotation(self):
        """等待 IP 轮换（全部槽位被封时调用）"""
        remaining = max(0, self._rotation_at - time.monotonic())
        if remaining > 0:
            logger.info(f"等待自动轮换 ({remaining:.0f}s)...")
            await asyncio.sleep(remaining)

    def get_available_channel(self) -> Optional[int]:
        """获取一个可用槽位（轮询分发）"""
        available = [ch for ch in self._channels.values()
                     if not ch.blocked and ch.proxy_url]
        if not available:
            return None
        self._round_robin_index = (self._round_robin_index + 1) % len(available)
        return available[self._round_robin_index].channel_id

    def all_channels_blocked(self) -> bool:
        return all(ch.blocked for ch in self._channels.values())

    def get_channel_proxy_url(self, channel_id: int) -> Optional[str]:
        ch = self._channels.get(channel_id)
        return ch.proxy_url if ch and ch.proxy_url else None

    def get_tunnel_proxy_url(self) -> str:
        return self._tunnel_proxy_url

    async def init_tunnel(self):
        """隧道模式初始化：用 TUNNEL_PROXY_URL 为每个通道生成代理 URL"""
        async with self._tunnel_init_lock:
            base_url = config.TUNNEL_PROXY_URL
            if not base_url:
                logger.error("TUNNEL_PROXY_URL 未配置!")
                return 0

            self._tunnel_proxy_url = base_url
            parsed = urlparse(base_url)

            for ch in self._channels.values():
                # 所有通道共享同一代理地址
                ch.proxy_url = base_url
                ch.reset_for_rotation()

            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            logger.info(f"隧道代理就绪: {parsed.hostname}:{parsed.port} "
                        f"→ {len(self._channels)} 通道")
            return len(self._channels)

    async def change_ip(self) -> bool:
        """隧道代理的 IP 由服务端自动轮换，客户端只需重置槽位状态"""
        for ch in self._channels.values():
            ch.reset_for_rotation()
        self._all_blocked_event.set()
        logger.info("所有槽位已重置（等待代理服务端自动换 IP）")
        return True

    async def handle_ip_rotation(self):
        """处理 IP 轮换（定时重置槽位状态）"""
        now = time.monotonic()
        if now < self._rotation_at:
            return False

        logger.info("IP 轮换周期到达，重置所有槽位")
        for ch in self._channels.values():
            ch.reset_for_rotation()
        self._all_blocked_event.set()
        self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
        return True

    def time_to_next_rotation(self) -> float:
        return max(0, self._rotation_at - time.monotonic())

    def get_stats(self) -> Dict:
        return {
            "mode": self.mode,
            "tunnel_proxy": self._tunnel_proxy_url[:30] + "..." if self._tunnel_proxy_url else "",
            "total_blocked": self._total_blocked,
            "channels": {
                ch.channel_id: {
                    "blocked": ch.blocked,
                    "request_count": ch.request_count,
                }
                for ch in self._channels.values()
            },
            "next_rotation_in": int(self.time_to_next_rotation()),
            "blocked_channels": sum(1 for ch in self._channels.values() if ch.blocked),
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
