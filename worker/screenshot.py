"""
独立截图进程：与采集 Worker 完全隔离的 asyncio 事件循环。

流程：每张截图渲染完立即上传 Server，上传成功后删除 HTML 源文件。
不积攒等待批次完成，确保截图实时上传。

通信协议（基于文件系统）：
  screenshot_cache/html/{batch_name}/{asin}.html  — 采集 Worker 写入的 HTML
  screenshot_cache/html/{batch_name}/_scraping_done — 采集完成标记（主 Worker 写入）
  screenshot_cache/_uploaded_{batch_name}           — 批次全部完成标记（通知主 Worker 门控）

启动方式：由 worker.py 作为子进程启动，传入 server_url 参数。
"""

import asyncio
import logging
import os
import shutil
import sys
import time
from typing import Optional

import httpx

logger = logging.getLogger("screenshot_worker")


class ScreenshotWorker:
    def __init__(self, server_url: str, base_dir: str = None,
                 browsers_count: int = 1, pages_per_browser: int = 1,
                 proxy_url: str = None):
        self.server_url = server_url
        self.base_dir = base_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "screenshot_cache"
        )
        self.html_dir = os.path.join(self.base_dir, "html")
        self._browsers_count = browsers_count
        self._pages_per_browser = pages_per_browser
        self._proxy_url = proxy_url
        self._concurrency = browsers_count * pages_per_browser
        self._browser_slots = []
        self._browser_lock = asyncio.Lock()
        self._browser_counter = 0
        self._render_count = 0
        self._restart_every = 200
        self._running = True
        self._http_client: Optional[httpx.AsyncClient] = None

    @staticmethod
    def _parse_proxy(proxy_url: str) -> dict:
        """解析 http://user:pwd@host:port 为 Playwright proxy dict"""
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url)
        result = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            result["username"] = parsed.username
        if parsed.password:
            result["password"] = parsed.password
        return result

    async def start(self):
        """主循环：扫描 HTML → 渲染 → 立即上传 → 删除 HTML → 检查批次完成"""
        os.makedirs(self.html_dir, exist_ok=True)
        self._http_client = httpx.AsyncClient(timeout=30)
        logger.info(f"截图独立进程启动（并发: {self._concurrency}, 监控: {self.html_dir}）")

        try:
            while self._running:
                pending = self._scan_pending()
                if not pending:
                    # 没有待处理的 HTML，检查是否有已完成的批次
                    self._check_batch_completion()
                    await asyncio.sleep(1)
                    continue

                await self._process_pending(pending)
                # 处理完一轮后立即检查批次完成状态
                self._check_batch_completion()
        except KeyboardInterrupt:
            pass
        finally:
            await self._close_browsers()
            if self._http_client:
                await self._http_client.aclose()
            logger.info("截图独立进程退出")

    def _scan_pending(self) -> list:
        """扫描所有待处理的 HTML 文件，返回 [(batch_name, asin, html_path), ...]"""
        pending = []
        if not os.path.isdir(self.html_dir):
            return pending

        for batch_name in os.listdir(self.html_dir):
            batch_dir = os.path.join(self.html_dir, batch_name)
            if not os.path.isdir(batch_dir):
                continue
            # 已上传完成的批次跳过
            if os.path.exists(os.path.join(self.base_dir, f"_uploaded_{batch_name}")):
                continue

            for fname in os.listdir(batch_dir):
                if fname.endswith(".html") and not fname.startswith("_"):
                    asin = fname[:-5]
                    html_path = os.path.join(batch_dir, fname)
                    pending.append((batch_name, asin, html_path))

        return pending

    async def _process_pending(self, pending: list):
        """并发处理：渲染 → 上传 → 删除 HTML"""
        logger.info(f"处理 {len(pending)} 张待截图")
        sem = asyncio.Semaphore(self._concurrency)

        async def process_one(batch_name, asin, html_path):
            async with sem:
                await self._render_upload_cleanup(batch_name, asin, html_path)

        tasks = [
            asyncio.create_task(process_one(b, a, p))
            for b, a, p in pending
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        # 定期重启浏览器回收内存（Chromium 长期运行会内存泄漏）
        self._render_count += len(pending)
        if self._render_count >= self._restart_every:
            logger.info(f"已渲染 {self._render_count} 张，重启浏览器回收内存")
            await self._close_browsers()
            self._render_count = 0

    async def _render_upload_cleanup(self, batch_name: str, asin: str, html_path: str):
        """混合方案：离线渲染优先（快），失败则在线兜底（准）"""
        # 第 1 步：离线渲染（读取保存的 HTML，本地渲染，速度快）
        png_bytes = None
        try:
            with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                html_content = f.read()
            png_bytes = await self._render_screenshot(html_content, asin)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.debug(f"离线渲染异常 {asin}: {e}")

        # 第 2 步：如果离线渲染失败或截图太小，在线兜底
        if not png_bytes or len(png_bytes) < 15000:
            if png_bytes:
                logger.info(f"离线截图太小({len(png_bytes)}B)，在线兜底: {asin}")
            else:
                logger.info(f"离线渲染失败，在线兜底: {asin}")
            png_bytes = await self._screenshot_live_page(asin)

        # 第 3 步：上传
        if png_bytes and len(png_bytes) > 5000:
            upload_ok = await self._upload_screenshot(batch_name, asin, png_bytes)
            if upload_ok:
                logger.info(f"截图完成: {asin} ({len(png_bytes)//1024}KB)")
                try:
                    os.remove(html_path)
                except OSError:
                    pass
            else:
                logger.warning(f"截图上传失败: {asin}")
        else:
            logger.warning(f"截图最终失败: {asin}")
            try:
                await self._http_client.post(
                    f"{self.server_url}/api/tasks/screenshot/fail",
                    json={"asin": asin, "batch_name": batch_name, "error": "both_failed"},
                    timeout=5,
                )
            except Exception:
                pass
            try:
                os.remove(html_path)
            except OSError:
                pass

    async def _screenshot_live_page(self, asin: str) -> Optional[bytes]:
        """在线兜底：临时启动带代理的浏览器访问 Amazon 截图"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return None

        pw = None
        browser = None
        page = None
        try:
            pw = await async_playwright().__aenter__()
            launch_opts = {
                "headless": True,
                "args": ["--disable-gpu", "--disable-dev-shm-usage",
                         "--no-sandbox", "--disable-extensions"],
            }
            if self._proxy_url:
                launch_opts["proxy"] = self._parse_proxy(self._proxy_url)

            browser = await pw.chromium.launch(**launch_opts)
            page = await browser.new_page(
                viewport={"width": 1280, "height": 1300},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )

            url = f"https://www.amazon.com/dp/{asin}?th=1&psc=1"
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)

            try:
                await page.wait_for_selector("#productTitle", timeout=8000)
            except Exception:
                pass

            await page.wait_for_timeout(2000)

            screenshot = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": 1300}
            )
            return screenshot

        except Exception as e:
            logger.warning(f"在线截图失败 {asin}: {e}")
            return None
        finally:
            if page:
                try: await page.close()
                except: pass
            if browser:
                try: await browser.close()
                except: pass
            if pw:
                try: await pw.__aexit__(None, None, None)
                except: pass

    async def _upload_screenshot(self, batch_name: str, asin: str, png_bytes: bytes) -> bool:
        """上传单张截图到服务器"""
        fname = f"{asin}.png"
        for attempt in range(3):
            try:
                resp = await self._http_client.post(
                    f"{self.server_url}/api/tasks/screenshot",
                    files={"file": (fname, png_bytes, "image/png")},
                    data={"batch_name": batch_name, "asin": asin},
                )
                if resp.status_code == 200:
                    return True
                logger.warning(f"上传失败 {asin}: HTTP {resp.status_code} (尝试 {attempt + 1}/3)")
            except Exception as e:
                logger.error(f"上传异常 {asin}: {e} (尝试 {attempt + 1}/3)")
            if attempt < 2:
                await asyncio.sleep(1)
        return False

    def _check_batch_completion(self):
        """检查批次是否已完成（_scraping_done 存在 + 无剩余 HTML）→ 写 _uploaded 标记"""
        if not os.path.isdir(self.html_dir):
            return

        for batch_name in os.listdir(self.html_dir):
            batch_dir = os.path.join(self.html_dir, batch_name)
            if not os.path.isdir(batch_dir):
                continue
            # 已标记完成的跳过
            uploaded_marker = os.path.join(self.base_dir, f"_uploaded_{batch_name}")
            if os.path.exists(uploaded_marker):
                continue

            # 需要 _scraping_done 标记
            scraping_done = os.path.join(batch_dir, "_scraping_done")
            if not os.path.exists(scraping_done):
                continue

            # 检查是否还有未处理的 HTML
            remaining = [f for f in os.listdir(batch_dir)
                         if f.endswith(".html") and not f.startswith("_")]
            if remaining:
                continue

            # 全部完成：写 _uploaded 标记
            with open(uploaded_marker, "w") as f:
                f.write(str(time.time()))
            logger.info(f"批次完成标记已写入: {batch_name}")

            # 清理批次目录
            shutil.rmtree(batch_dir, ignore_errors=True)

    async def _render_screenshot(self, html_content: str, asin: str) -> Optional[bytes]:
        """Playwright 渲染截图"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("playwright 未安装，跳过截图渲染")
            return None

        page = None
        try:
            # 懒初始化浏览器池
            if not self._browser_slots:
                async with self._browser_lock:
                    if not self._browser_slots:
                        for i in range(self._browsers_count):
                            pw = await async_playwright().__aenter__()
                            launch_opts = {
                                "headless": True,
                                "args": ["--disable-gpu", "--disable-dev-shm-usage",
                                         "--no-sandbox", "--disable-extensions"],
                            }
                            # 离线渲染池不注入代理（本地渲染不需要网络）
                            browser = await pw.chromium.launch(**launch_opts)
                            self._browser_slots.append({"playwright": pw, "browser": browser})
                        logger.info(f"浏览器池启动（{self._browsers_count} 实例，离线渲染）")

            idx = self._browser_counter % len(self._browser_slots)
            self._browser_counter += 1
            browser = self._browser_slots[idx]["browser"]
            page = await browser.new_page(viewport={"width": 1280, "height": 1300})

            # 屏蔽无关资源
            async def block_resources(route):
                rt = route.request.resource_type
                url = route.request.url
                if rt in ("stylesheet", "image"):
                    await route.continue_()
                elif rt in ("script", "font", "media", "websocket",
                            "manifest", "other"):
                    await route.abort()
                elif any(x in url for x in ("analytics", "tracking", "beacon",
                                            "ads", "doubleclick", "facebook")):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", block_resources)

            # 注入 <base> 标签
            base_tag = '<base href="https://www.amazon.com/">'
            lower_head = html_content[:2000].lower()
            if "<base " not in lower_head:
                head_pos = lower_head.find("<head")
                if head_pos != -1:
                    close_pos = html_content.index(">", head_pos) + 1
                    html_content = html_content[:close_pos] + base_tag + html_content[close_pos:]
                else:
                    html_content = base_tag + html_content

            try:
                await page.set_content(
                    html_content,
                    wait_until="load",
                    timeout=10000,
                )
            except Exception:
                # load 超时也继续，domcontentloaded 后内容通常已经可见
                pass

            # 等待主图和样式加载（增加到 3 秒）
            await page.wait_for_timeout(3000)

            screenshot = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": 1300}
            )

            return screenshot
        except Exception as e:
            err_msg = str(e)
            if "browser has been closed" in err_msg or "Target closed" in err_msg:
                logger.error(f"浏览器崩溃，将重启: {asin}")
                await self._close_browsers()
            else:
                logger.warning(f"渲染失败 {asin}: {e}")
            return None
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    async def _close_browsers(self):
        """关闭所有浏览器"""
        async with self._browser_lock:
            for slot in self._browser_slots:
                try:
                    await slot["browser"].close()
                except Exception:
                    pass
                try:
                    await slot["playwright"].stop()
                except Exception:
                    pass
            self._browser_slots.clear()


def main():
    """入口：python screenshot_worker.py <server_url> [browsers_count] [pages_per_browser]"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [SCREENSHOT] %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 2:
        print("Usage: python screenshot_worker.py <server_url> [browsers_count] [pages_per_browser]")
        sys.exit(1)

    server_url = sys.argv[1].rstrip("/")
    browsers_count = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    pages_per_browser = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    proxy_url = sys.argv[4] if len(sys.argv) > 4 else None

    worker = ScreenshotWorker(
        server_url=server_url,
        browsers_count=browsers_count,
        pages_per_browser=pages_per_browser,
        proxy_url=proxy_url,
    )
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
