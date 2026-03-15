"""
Amazon 产品采集系统 v2 - 代理管理模块

支持两种模式（共用同一个快代理 API）：
- TPS 模式：每次请求自动换 IP（API 获取 1 个代理，缓存复用）
- 隧道模式：固定隧道地址，多通道独立出口 IP，定时自动换 IP

隧道模式核心规则（快代理 TPS 隧道 + 多通道）：
1. 隧道代理地址固定，启动时获取一次即可
2. 每个 channel 通过 password:channel_id 指定独立通道 → 独立出口 IP
3. 每个换 IP 周期（如 60s）自动切换出口 IP
4. 每个周期内可调 ChangeTpsIp API 手动换 IP（最多 2 次）
5. 每通道 3Mbps，总带宽 5Mbps（相比单通道提升 67%）
"""
import asyncio
import re
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse

import httpx

from common import config

logger = logging.getLogger(__name__)


# ==================== 会话槽位状态（隧道模式专用）====================

@dataclass
class ChannelState:
    """
    单个会话槽位的运行时状态。

    每个 channel 通过 password:channel_id 指定独立通道，
    拥有独立出口 IP + 独立 Cookie / 指纹，最大化反爬效果。
    """
    channel_id: int                     # 槽位编号 1-N
    proxy_url: str = ""                 # 隧道代理地址（所有槽位相同）
    blocked: bool = False               # 该 Session 是否被封（Cookie 级别）
    blocked_at: float = 0               # 封锁时间戳（monotonic）
    request_count: int = 0              # 当前周期内请求计数
    last_request_at: float = 0          # 上次请求时间

    def reset_for_rotation(self):
        """IP 轮换时重置槽位状态"""
        self.blocked = False
        self.blocked_at = 0
        self.request_count = 0


# ==================== 代理管理器 ====================

class ProxyManager:
    """
    统一代理管理器，通过 config.PROXY_MODE 区分行为：
    - "tps": 原有 TPS 逻辑（每次请求换 IP，单代理缓存）
    - "tunnel": 隧道模式（固定隧道地址，定时换 IP，多 Session 槽位）
    """

    def __init__(self):
        self.mode = config.PROXY_MODE

        # --- TPS 模式状态 ---
        self._current_proxy: Optional[str] = None
        self._proxy_expire_at: float = 0
        self._refresh_interval = config.PROXY_REFRESH_INTERVAL
        self._last_fetch_time: float = 0
        self._fetch_lock = asyncio.Lock()

        # --- 隧道模式状态 ---
        self._tunnel_proxy_url: str = ""     # 固定隧道地址（所有请求共享）
        self._channels: Dict[int, ChannelState] = {}
        self._round_robin_index = 0          # 轮询计数器
        self._rotation_at: float = 0         # 下次 IP 轮换时间点
        self._all_blocked_event = asyncio.Event()
        self._all_blocked_event.set()         # 初始不阻塞
        self._tunnel_init_lock = asyncio.Lock()
        self._change_ip_count = 0            # 当前周期内 ChangeTpsIp 调用次数
        self._last_change_ip_at: float = 0   # 上次 ChangeTpsIp 时间

        if self.mode == "tunnel":
            for i in range(1, config.TUNNEL_CHANNELS + 1):
                self._channels[i] = ChannelState(channel_id=i)
            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            logger.info(f"隧道模式初始化：{config.TUNNEL_CHANNELS} 会话槽位，"
                        f"{config.TUNNEL_ROTATE_INTERVAL}s 轮换周期")

        # --- 公共统计 ---
        self._total_fetched = 0
        self._total_errors = 0
        self._total_blocked = 0

    @staticmethod
    def _build_channel_url(parsed, channel_id: int) -> str:
        """构建带通道号的代理 URL: password:channel_id"""
        return (
            f"http://{parsed.username}:{parsed.password}:{channel_id}"
            f"@{parsed.hostname}:{parsed.port}"
        )

    def switch_mode(self, new_mode: str):
        """运行时切换代理模式（settings sync 检测到 proxy_mode 变化时调用）"""
        if new_mode == self.mode:
            return
        old_mode = self.mode
        self.mode = new_mode
        if new_mode == "tunnel":
            self._channels.clear()
            self._tunnel_proxy_url = ""
            for i in range(1, config.TUNNEL_CHANNELS + 1):
                self._channels[i] = ChannelState(channel_id=i)
            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            self._all_blocked_event.set()
            self._change_ip_count = 0
            logger.info(f"代理模式切换: {old_mode} → {new_mode} "
                        f"({config.TUNNEL_CHANNELS} 会话槽位)")
        else:
            self._channels.clear()
            self._tunnel_proxy_url = ""
            self._current_proxy = None
            self._proxy_expire_at = 0
            logger.info(f"代理模式切换: {old_mode} → {new_mode}")

    def reconfigure_tunnel(self, channels: int, rotate_interval: int):
        """
        运行时重配隧道参数（仅 tunnel 模式）：
        - 调整会话槽位数量
        - 重置下一次轮换计时
        """
        if self.mode != "tunnel":
            return

        channels = max(1, int(channels))
        rotate_interval = max(10, int(rotate_interval))

        old_channels = self._channels
        new_channels: Dict[int, ChannelState] = {}

        # 为新通道生成带通道号的 proxy URL
        if self._tunnel_proxy_url:
            parsed = urlparse(self._tunnel_proxy_url)
        else:
            parsed = None

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

        logger.info(f"隧道参数重配：channels={channels}, rotate={rotate_interval}s")

    # ==================== 公共接口 ====================

    async def get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """
        获取代理。

        返回: (proxy_url, channel_id)
        - TPS 模式: channel_id 固定为 None
        - 隧道模式: channel_id 为分配的槽位编号，proxy_url 为统一隧道地址
        """
        if self.mode == "tps":
            proxy = await self._tps_get_proxy()
            return proxy, None
        else:
            return await self._tunnel_get_proxy(channel)

    async def report_blocked(self, channel: int = None):
        """
        报告被封锁。

        - TPS 模式: 强制刷新代理
        - 隧道模式: 标记该 Session 槽位为被封（Cookie 级别）
        """
        self._total_blocked += 1
        if self.mode == "tps":
            return await self._tps_report_blocked()
        else:
            return await self._tunnel_report_blocked(channel)

    async def wait_for_rotation(self):
        """等待 IP 轮换（仅隧道模式，全部槽位被封时调用）"""
        if self.mode != "tunnel":
            return
        # 全部被封 → 尝试手动换 IP
        changed = await self.change_ip()
        if changed:
            return  # 换 IP 成功，立即返回
        # 手动换 IP 失败（次数用尽等），等待自动轮换
        remaining = max(0, self._rotation_at - time.monotonic())
        if remaining > 0:
            logger.info(f"⏳ 全部槽位被封且手动换 IP 不可用，"
                        f"等待自动轮换（{remaining:.0f}s）...")
            await asyncio.sleep(remaining)

    def get_available_channel(self) -> Optional[int]:
        """获取一个可用槽位（轮询分发），返回 None 表示全部被封"""
        if self.mode != "tunnel":
            return None
        available = [ch for ch in self._channels.values()
                     if not ch.blocked and ch.proxy_url]
        if not available:
            return None
        self._round_robin_index = (self._round_robin_index + 1) % len(available)
        return available[self._round_robin_index].channel_id

    def all_channels_blocked(self) -> bool:
        """是否全部槽位都被封（仅隧道模式）"""
        if self.mode != "tunnel":
            return False
        return all(ch.blocked for ch in self._channels.values())

    def get_channel_proxy_url(self, channel_id: int) -> Optional[str]:
        """获取指定槽位的代理 URL"""
        ch = self._channels.get(channel_id)
        if ch and ch.proxy_url:
            return ch.proxy_url
        return None

    def get_tunnel_proxy_url(self) -> str:
        """获取统一隧道代理地址"""
        return self._tunnel_proxy_url

    async def init_tunnel(self):
        """
        隧道模式启动初始化：
        1. 优先使用 TUNNEL_PROXY_URL（固定帐密隧道，跳过 API）
        2. 否则调 API 获取隧道代理地址
        3. 为每个 channel 生成带通道号的 URL（password:channel_id）
        """
        async with self._tunnel_init_lock:
            # 优先使用固定隧道地址（帐密模式）
            is_fixed = bool(config.TUNNEL_PROXY_URL)
            if is_fixed:
                base_url = config.TUNNEL_PROXY_URL
                logger.info(f"🔧 使用固定隧道代理: {base_url[:30]}...")
            else:
                logger.info("🔧 获取隧道代理地址...")
                proxies = await self._fetch_proxies_from_api(num=1)
                if not proxies:
                    logger.error("❌ 获取隧道代理失败：API 返回空")
                    return 0
                base_url = proxies[0]

            self._tunnel_proxy_url = base_url

            parsed = urlparse(base_url)
            for ch in self._channels.values():
                if is_fixed:
                    # 固定隧道：所有通道共享同一代理地址（不追加 channel_id）
                    ch.proxy_url = base_url
                else:
                    ch.proxy_url = self._build_channel_url(parsed, ch.channel_id)
                ch.reset_for_rotation()

            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            self._change_ip_count = 0
            self._total_fetched += 1
            logger.info(f"✅ 隧道代理就绪：{parsed.hostname}:{parsed.port} → {len(self._channels)} 个独立通道")
            for ch in self._channels.values():
                logger.info(f"  通道 {ch.channel_id}: ...:{ch.channel_id}@{parsed.hostname}:{parsed.port}")
            return len(self._channels)

    async def change_ip(self) -> bool:
        """
        调用 ChangeTpsIp API 手动换 IP。
        规则：每个轮换周期内最多调用 2 次，最快 1 秒 1 次。
        返回 True 表示换 IP 成功。
        """
        # 检查周期内调用次数
        if self._change_ip_count >= 2:
            logger.warning("⚠️ ChangeTpsIp 本周期已调用 2 次，跳过")
            return False

        # 最快 1 秒 1 次
        now = time.monotonic()
        elapsed = now - self._last_change_ip_at
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)

        try:
            # 从当前 proxy_api_url 动态派生 ChangeTpsIp URL（支持运行时切换凭证）
            api_url = config.TUNNEL_CHANGE_IP_URL
            proxy_api = getattr(config, "PROXY_API_URL_AUTH", "")
            if proxy_api and "secret_id=" in proxy_api and "signature=" in proxy_api:
                import urllib.parse
                parsed = urllib.parse.urlparse(proxy_api)
                params = urllib.parse.parse_qs(parsed.query)
                sid = params.get("secret_id", [""])[0]
                sig = params.get("signature", [""])[0]
                if sid and sig:
                    api_url = f"https://tps.kdlapi.com/api/changetpsip/?secret_id={sid}&signature={sig}"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(api_url)
                data = resp.json()

            if data.get("code") == 0:
                new_ip = data.get("data", {}).get("new_ip", "unknown")
                self._change_ip_count += 1
                self._last_change_ip_at = time.monotonic()
                # 重置所有槽位的封锁状态（新 IP 可能绕过封锁）
                for ch in self._channels.values():
                    ch.reset_for_rotation()
                self._all_blocked_event.set()
                logger.info(f"🔄 ChangeTpsIp 成功: 新 IP={new_ip} "
                            f"(本周期第 {self._change_ip_count} 次)")
                return True
            else:
                msg = data.get("msg", str(data))
                logger.warning(f"⚠️ ChangeTpsIp 失败: {msg}")
                return False

        except Exception as e:
            logger.error(f"❌ ChangeTpsIp 异常: {e}")
            return False

    async def handle_ip_rotation(self):
        """
        处理 IP 轮换：检查是否到达轮换时间点。
        隧道代理地址不变，出口 IP 由代理服务自动切换。
        返回 True 表示 IP 已轮换（调用者需 rebuild sessions）。
        """
        now = time.monotonic()
        if now < self._rotation_at:
            return False

        logger.info("🔄 IP 轮换时间到达，出口 IP 已自动切换")
        # 重置所有槽位状态
        for ch in self._channels.values():
            ch.reset_for_rotation()
        self._all_blocked_event.set()
        # 重置周期计数器
        self._change_ip_count = 0
        self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
        return True

    def time_to_next_rotation(self) -> float:
        """距离下次 IP 轮换的秒数"""
        return max(0, self._rotation_at - time.monotonic())

    def get_stats(self) -> Dict:
        """获取代理统计信息"""
        now = time.monotonic()
        stats = {
            "mode": self.mode,
            "total_fetched": self._total_fetched,
            "total_errors": self._total_errors,
            "total_blocked": self._total_blocked,
        }
        if self.mode == "tps":
            stats.update({
                "current_proxy": self._current_proxy,
                "proxy_valid": self._current_proxy is not None and now < self._proxy_expire_at,
                "expire_in": max(0, int(self._proxy_expire_at - now)),
            })
        else:
            stats.update({
                "tunnel_proxy": self._tunnel_proxy_url[:30] + "..." if self._tunnel_proxy_url else "",
                "channels": {
                    ch.channel_id: {
                        "blocked": ch.blocked,
                        "request_count": ch.request_count,
                    }
                    for ch in self._channels.values()
                },
                "next_rotation_in": int(self.time_to_next_rotation()),
                "blocked_channels": sum(1 for ch in self._channels.values() if ch.blocked),
                "change_ip_remaining": max(0, 2 - self._change_ip_count),
            })
        return stats

    # ==================== API 调用（两种模式共用）====================

    def _make_api_url(self, num: int = 1) -> str:
        """构造 API URL，修改 num 参数为指定值"""
        url = config.PROXY_API_URL_AUTH
        if "num=" in url:
            url = re.sub(r'num=\d+', f'num={num}', url)
        else:
            url += f"&num={num}"
        return url

    async def _fetch_proxies_from_api(self, num: int = 1) -> List[str]:
        """
        调用快代理 API 获取代理地址。
        返回: 代理 URL 列表，如 ["http://user:pwd@host:port", ...]
        """
        self._last_fetch_time = time.monotonic()
        api_url = self._make_api_url(num)

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(api_url)
                data = resp.json()
        except Exception as e:
            logger.error(f"代理 API 请求异常: {e}")
            self._total_errors += 1
            return []

        if data.get("code") != 0:
            logger.error(f"代理 API 返回错误: {data}")
            self._total_errors += 1
            return []

        proxy_list = data.get("data", {}).get("proxy_list", [])
        results = []
        for proxy_str in proxy_list:
            parts = proxy_str.split(":")
            if len(parts) == 4:
                ip, port, user, pwd = parts
                results.append(f"http://{user}:{pwd}@{ip}:{port}")
            elif len(parts) == 2:
                ip, port = parts
                results.append(f"http://{ip}:{port}")
            else:
                results.append(f"http://{proxy_str}")

        return results

    # ==================== TPS 模式内部实现 ====================

    async def _tps_get_proxy(self) -> Optional[str]:
        """TPS: 获取当前可用代理，过期则自动刷新"""
        # 固定隧道代理：跳过 API，直接返回
        if config.TUNNEL_PROXY_URL:
            self._current_proxy = config.TUNNEL_PROXY_URL
            return self._current_proxy
        now = time.monotonic()
        if self._current_proxy and now < self._proxy_expire_at:
            return self._current_proxy
        return await self._tps_refresh_proxy()

    async def _tps_refresh_proxy(self) -> Optional[str]:
        """TPS: 从快代理 API 获取新的隧道代理（线程安全）"""
        async with self._fetch_lock:
            now = time.monotonic()
            if self._current_proxy and now < self._proxy_expire_at:
                return self._current_proxy

            elapsed = now - self._last_fetch_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

            for attempt in range(3):
                try:
                    proxies = await self._fetch_proxies_from_api(num=1)
                    if proxies:
                        self._current_proxy = proxies[0]
                        self._proxy_expire_at = time.monotonic() + self._refresh_interval
                        self._total_fetched += 1
                        logger.info(f"获取代理: {self._current_proxy}")
                        return self._current_proxy
                    logger.warning(f"代理 API 返回空结果 (尝试 {attempt+1}/3)")
                except Exception as e:
                    logger.error(f"获取代理失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

            self._total_errors += 1
            return self._current_proxy

    async def _tps_report_blocked(self):
        """TPS: 报告代理被封锁，强制过期触发重新获取"""
        self._total_blocked += 1
        if config.TUNNEL_PROXY_URL:
            # 固定隧道代理：地址不变，只记录封锁（IP 轮换由代理服务端处理）
            logger.warning(f"代理被封（第 {self._total_blocked} 次），固定隧道无需刷新")
            return self._current_proxy
        logger.warning(f"代理被封（第 {self._total_blocked} 次），触发刷新")
        self._proxy_expire_at = 0
        self._current_proxy = None
        return await self._tps_refresh_proxy()

    # ==================== 隧道模式内部实现 ====================

    async def _tunnel_get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """隧道: 获取指定槽位（或自动分配）的代理 URL"""
        if channel is None:
            channel = self.get_available_channel()
        if channel is None:
            return None, None

        ch_state = self._channels[channel]
        ch_state.request_count += 1
        ch_state.last_request_at = time.monotonic()
        return ch_state.proxy_url, channel

    async def _tunnel_report_blocked(self, channel: int):
        """隧道: 标记 Session 槽位被封（Cookie 级别封锁）"""
        if channel is None or channel not in self._channels:
            return
        ch_state = self._channels[channel]
        ch_state.blocked = True
        ch_state.blocked_at = time.monotonic()
        blocked_count = sum(1 for ch in self._channels.values() if ch.blocked)
        logger.warning(f"🚫 槽位 {channel} Session 被封"
                       f"（已封 {blocked_count}/{len(self._channels)}）")

        if self.all_channels_blocked():
            self._all_blocked_event.clear()
            logger.error("❌ 全部 Session 被封！尝试 ChangeTpsIp...")


# ==================== 全局单例 ====================

_proxy_manager: Optional[ProxyManager] = None
_proxy_manager_lock = asyncio.Lock()


async def get_proxy_manager_async() -> ProxyManager:
    """获取全局代理管理器实例（异步安全单例）"""
    global _proxy_manager
    if _proxy_manager is None:
        async with _proxy_manager_lock:
            if _proxy_manager is None:
                _proxy_manager = ProxyManager()
    return _proxy_manager


def get_proxy_manager() -> ProxyManager:
    """获取全局代理管理器实例（同步调用，适用于初始化阶段）"""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
