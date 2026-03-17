"""
Amazon 产品采集系统 v3 - Session 管理模块
使用 curl_cffi 模拟浏览器 TLS 指纹
正确实现邮编设置（POST 到 address-change.html）
Cookie jar 管理

TPS 模式：单个 AmazonSession，全局共享，每次请求自动换 IP
"""
import asyncio
import random
import re
import logging
import time
from typing import Optional, Dict, Any, List

from curl_cffi import CurlHttpVersion
from curl_cffi.requests import AsyncSession, Response

from common import config
from worker.proxy import ProxyManager

# CAPTCHA 自动识别（可选依赖）
try:
    from amazoncaptcha import AmazonCaptcha
    _CAPTCHA_AVAILABLE = True
except ImportError:
    _CAPTCHA_AVAILABLE = False

logger = logging.getLogger(__name__)


class AmazonSession:
    """
    Amazon 会话管理器
    每个实例维护独立的 cookie jar 和 session
    """

    AMAZON_BASE = "https://www.amazon.com"
    ZIP_CHANGE_URL = "https://www.amazon.com/gp/delivery/ajax/address-change.html"

    def __init__(self, proxy_manager: ProxyManager, zip_code: str = None,
                 max_clients: int = None):
        """
        Args:
            proxy_manager: 代理管理器
            zip_code: 配送邮编
            max_clients: 连接池大小（HTTP/1.1 下为最大 TCP 连接数）
        """
        self.proxy_manager = proxy_manager
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self._max_clients = max_clients or config.MAX_CLIENTS
        self._session: Optional[AsyncSession] = None
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._request_count = 0
        self._last_url: Optional[str] = None
        # 智能指纹轮换：随机选择浏览器 profile（UA + impersonate + sec-ch-ua 三者匹配）
        profile = random.choice(config.BROWSER_PROFILES)
        self._impersonate = profile["impersonate"]
        self._sec_ch_ua = profile["sec_ch_ua"]
        self._user_agent = random.choice(profile["user_agents"])
        # 根据 UA 选择平台
        if "Windows" in self._user_agent:
            self._platform = '"Windows"'
        elif "Macintosh" in self._user_agent:
            self._platform = '"macOS"'
        else:
            self._platform = '"Linux"'

    async def initialize(self) -> bool:
        """
        初始化 session：
        1. 创建 curl_cffi 会话
        2. 访问 Amazon 首页获取 cookies（带重试）
        3. POST 设置邮编

        带锁保护：多个协程同时调用时，只有第一个执行初始化，其余等待并复用结果
        """
        async with self._init_lock:
            if self._initialized:
                return True

            for init_attempt in range(3):
                try:
                    proxy = await self.proxy_manager.get_proxy()

                    self._session = AsyncSession(
                        impersonate=self._impersonate,
                        timeout=config.REQUEST_TIMEOUT,
                        proxy=proxy,
                        max_clients=self._max_clients,
                        # 必须显式指定 V1_1，否则 impersonate 通过 ALPN 协商 HTTP/2
                        # HTTP/1.1: 每请求独立 TCP 连接，丢包隔离
                        # 注意：CurlHttpVersion.V1_1 的整数值 = 2 (CURL_HTTP_VERSION_1_1)
                        http_version=CurlHttpVersion.V1_1,
                    )

                    # 1. 访问首页获取初始 cookies
                    headers = self._build_headers()
                    resp = await self._session.get(
                        self.AMAZON_BASE,
                        headers=headers,
                    )

                    if resp.status_code >= 300:
                        logger.warning(f"首页返回 {resp.status_code}，重试 ({init_attempt+1}/3)")
                        await self._session.close()
                        self._session = None
                        await asyncio.sleep(3)
                        continue

                    # 2. 设置邮编（带重试）
                    zip_ok = False
                    for zip_attempt in range(3):
                        if await self._set_zip_code():
                            zip_ok = True
                            break
                        logger.warning(f"📍 邮编设置失败 (尝试 {zip_attempt+1}/3)")
                        await asyncio.sleep(1)

                    if not zip_ok:
                        logger.warning(f"⚠️ 邮编设置 3 次全失败，放弃当前代理 (初始化 {init_attempt+1}/3)")
                        await self._session.close()
                        self._session = None
                        await self.proxy_manager.report_blocked()
                        await asyncio.sleep(2)
                        continue

                    # 3. 验证邮编是否生效
                    verified = await self._verify_zip_code()
                    if not verified:
                        logger.warning(f"⚠️ 邮编验证失败（页面未反映 {self.zip_code}），放弃当前代理 (初始化 {init_attempt+1}/3)")
                        await self._session.close()
                        self._session = None
                        await self.proxy_manager.report_blocked()
                        await asyncio.sleep(2)
                        continue

                    self._initialized = True
                    logger.info(f"✅ Session 初始化成功 (邮编: {self.zip_code})")
                    return True

                except Exception as e:
                    logger.error(f"❌ Session 初始化失败 (尝试 {init_attempt+1}/3): {e}")
                    if self._session:
                        await self._session.close()
                        self._session = None
                    if init_attempt < 2:
                        await asyncio.sleep(3)
                        continue

            logger.error("❌ Session 初始化失败，已重试 3 次")
            return False

    async def _set_zip_code(self) -> bool:
        """通过 POST 请求设置配送邮编"""
        try:
            if self._session is None:
                return False
            cookies = self._session.cookies
            session_id = None
            for cookie in cookies.jar:
                if cookie.name == "session-id":
                    session_id = cookie.value
                    break

            headers = self._build_headers()
            headers.update({
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.amazon.com/",
                "Origin": "https://www.amazon.com",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "anti-csrftoken-a2z": cookies.get("csm-hit", ""),
            })

            data = {
                "locationType": "LOCATION_INPUT",
                "zipCode": self.zip_code,
                "storeContext": "generic",
                "deviceType": "web",
                "pageType": "Gateway",
                "actionSource": "glow",
            }

            resp = await self._session.post(
                self.ZIP_CHANGE_URL,
                headers=headers,
                data=data,
            )

            if resp.status_code == 200:
                try:
                    result = resp.json()
                    if result.get("isValidAddress") == 1:
                        logger.info(f"📍 邮编设置成功: {self.zip_code}")
                        return True
                    else:
                        logger.warning(f"📍 邮编设置响应: {result}")
                        return False
                except Exception:
                    logger.info(f"📍 邮编设置请求已发送 (200)")
                    return True
            else:
                logger.warning(f"📍 邮编设置返回 {resp.status_code}")
                return False

        except Exception as e:
            logger.error(f"📍 邮编设置异常: {e}")
            return False

    async def _verify_zip_code(self) -> bool:
        """验证邮编是否实际生效"""
        try:
            headers = self._build_headers(referer="https://www.amazon.com/")
            resp = await self._session.get(
                self.AMAZON_BASE,
                headers=headers,
            )
            if resp.status_code != 200:
                return False

            text = resp.text
            import re
            zip_match = re.search(r'id="glow-ingress-line2"[^>]*>\s*([^<]+)', text)
            if zip_match:
                location_text = zip_match.group(1).strip()
                if self.zip_code in location_text:
                    logger.info(f"📍 邮编验证通过: {location_text}")
                    return True
                else:
                    logger.warning(f"📍 邮编验证不匹配: 期望 {self.zip_code}, 页面显示 '{location_text}'")
                    return False

            non_us_indicators = ['CNY', '¥', '€', '£', 'JP¥']
            for indicator in non_us_indicators:
                if indicator in text[:50000]:
                    logger.warning(f"📍 邮编验证失败: 页面包含非美国货币标识 '{indicator}'")
                    return False

            logger.info(f"📍 邮编验证: 未找到 location widget，但无异常货币标识")
            return True

        except Exception as e:
            logger.error(f"📍 邮编验证异常: {e}")
            return False

    def _build_headers(self, referer: str = None) -> Dict[str, str]:
        """构建反指纹请求头"""
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": self._user_agent,
            "Upgrade-Insecure-Requests": "1",
            "sec-ch-ua": self._sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": self._platform,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

        if referer:
            headers["Referer"] = referer
            headers["Sec-Fetch-Site"] = "same-origin"
        elif self._last_url:
            headers["Referer"] = self._last_url
            headers["Sec-Fetch-Site"] = "same-origin"

        return headers

    async def fetch_product_page(self, asin: str, max_recv_speed: int = 0) -> Optional[Response]:
        """采集 Amazon 商品页面"""
        if not self._initialized:
            await self.initialize()

        if self._session is None:
            logger.warning(f"⚠️ Session 未就绪，跳过 ASIN={asin}")
            return None

        # v3: 加 ?th=1&psc=1 获取具体变体价格（而非价格区间）
        url = f"{self.AMAZON_BASE}/dp/{asin}?th=1&psc=1"
        referer = self._last_url or f"{self.AMAZON_BASE}/"
        headers = self._build_headers(referer=referer)

        try:
            resp = await self._session.get(
                url,
                headers=headers,
                max_recv_speed=max_recv_speed,
            )

            self._last_url = url
            self._request_count += 1

            if resp.status_code == 200 and len(resp.content) < 1000:
                logger.warning(f"⚠️ ASIN={asin} 响应体过短 ({len(resp.content)} bytes)，视为空页面")
                return None

            return resp
        except Exception as e:
            logger.error(f"❌ 请求失败 ASIN={asin}: {e}")
            return None

    async def fetch_product_page_by_url(self, url: str) -> Optional[Response]:
        """请求任意 Amazon URL（用于 AOD AJAX 等辅助请求）"""
        if not self._initialized or self._session is None:
            return None
        try:
            headers = self._build_headers(referer=self._last_url or f"{self.AMAZON_BASE}/")
            resp = await self._session.get(url, headers=headers)
            return resp
        except Exception as e:
            logger.debug(f"辅助请求失败: {e}")
            return None

    def is_ready(self) -> bool:
        """检查 session 是否已初始化并可用"""
        return self._initialized and self._session is not None

    def is_captcha(self, response: Response) -> bool:
        """检测响应是否为 CAPTCHA 验证码页面"""
        if response is None:
            return False
        text = response.text
        if "captcha" in response.url.lower():
            return True
        if "validateCaptcha" in text or "Robot Check" in text:
            return True
        return False

    def is_blocked(self, response: Response) -> bool:
        """检测是否被 Amazon 封锁"""
        if response is None:
            return False

        if response.status_code == 404:
            return False

        if response.status_code in (403, 503):
            return True

        if self.is_captcha(response):
            return True

        text = response.text
        if "api-services-support@amazon.com" in text and len(text) < 20000:
            return True

        return False

    async def solve_captcha(self, response: Response) -> bool:
        """
        尝试自动解决 CAPTCHA 验证码。

        流程：
        1. 从 CAPTCHA 页面 HTML 提取图片 URL 和表单字段
        2. 使用 amazoncaptcha 库本地 OCR 识别
        3. 提交解决方案到 Amazon 验证端点
        4. 验证是否成功（返回非验证码页面）

        Returns:
            True: 验证码解决成功，session 可继续使用
            False: 解决失败（库不可用 / 提取失败 / 识别失败 / 验证失败）
        """
        if not _CAPTCHA_AVAILABLE:
            return False

        if response is None or self._session is None:
            return False

        try:
            text = response.text

            # 提取 CAPTCHA 图片 URL
            img_match = re.search(
                r'<img[^>]+src=["\']([^"\']*captcha[^"\']*)["\']',
                text, re.IGNORECASE
            )
            if not img_match:
                logger.debug("CAPTCHA: 未找到验证码图片 URL")
                return False

            img_url = img_match.group(1)
            if img_url.startswith("//"):
                img_url = "https:" + img_url

            # 提取表单隐藏字段 (amzn, amzn-r)
            amzn_match = re.search(r'name=["\']amzn["\'][^>]+value=["\']([^"\']*)["\']', text)
            amzn_r_match = re.search(r'name=["\']amzn-r["\'][^>]+value=["\']([^"\']*)["\']', text)

            amzn_val = amzn_match.group(1) if amzn_match else ""
            amzn_r_val = amzn_r_match.group(1) if amzn_r_match else ""

            # 使用 amazoncaptcha 本地 OCR 解决
            captcha = AmazonCaptcha.fromlink(img_url)
            solution = captcha.solve()

            if solution == "Not solved" or not solution:
                logger.warning(f"CAPTCHA: OCR 识别失败 (img: {img_url[:80]})")
                return False

            logger.info(f"CAPTCHA: OCR 识别结果 = '{solution}'")

            # 提交验证码解决方案
            validate_url = f"{self.AMAZON_BASE}/errors/validateCaptcha"
            params = {
                "amzn": amzn_val,
                "amzn-r": amzn_r_val,
                "field-keywords": solution,
            }

            headers = self._build_headers(referer=response.url)
            verify_resp = await self._session.get(
                validate_url,
                params=params,
                headers=headers,
            )

            # 验证是否成功：返回页面不再包含 CAPTCHA
            if verify_resp.status_code == 200 and not self.is_captcha(verify_resp):
                logger.info("CAPTCHA: 验证码解决成功，session 可继续使用")
                return True
            else:
                logger.warning(f"CAPTCHA: 验证提交后仍被拦截 (HTTP {verify_resp.status_code})")
                return False

        except Exception as e:
            logger.warning(f"CAPTCHA: 解决过程异常: {e}")
            return False

    def is_404(self, response: Response) -> bool:
        """检测商品是否不存在"""
        return response.status_code == 404

    async def close(self):
        """关闭会话"""
        if self._session:
            await self._session.close()
            self._session = None
            self._initialized = False

    @property
    def stats(self) -> Dict:
        """获取会话统计"""
        return {
            "initialized": self._initialized,
            "zip_code": self.zip_code,
            "request_count": self._request_count,
            "user_agent": self._user_agent[:50] + "...",
        }


