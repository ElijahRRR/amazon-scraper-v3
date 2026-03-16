"""
截图子进程 v3 — 参照测试脚本重写，简洁高效。

核心逻辑：
  1 个 Playwright 实例 + 1 个 Chromium 浏览器（常驻复用）
  并发 N 个 page 同时渲染（通过 Semaphore 控制）
  渲染完立即上传 Server，成功后删除 HTML

通信协议（基于文件系统）：
  screenshot_cache/html/{batch_name}/{asin}.html  — 采集 Worker 写入
  screenshot_cache/html/{batch_name}/_scraping_done — 采集完成标记
  screenshot_cache/_uploaded_{batch_name}           — 全部截图完成标记
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
                 browsers_count: int = 1, pages_per_browser: int = 5,
                 proxy_url: str = None):
        self.server_url = server_url
        self.base_dir = base_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "screenshot_cache"
        )
        self.html_dir = os.path.join(self.base_dir, "html")
        self._concurrency = pages_per_browser
        self._browser = None
        self._pw = None
        self._browser_lock = asyncio.Lock()
        self._render_count = 0
        self._restart_every = 500
        self._running = True
        self._http_client: Optional[httpx.AsyncClient] = None

    # ==================== 浏览器管理 ====================

    async def _ensure_browser(self):
        """懒初始化：加锁防止并发创建多个实例"""
        if self._browser:
            return
        async with self._browser_lock:
            if self._browser:
                return  # 双重检查
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().__aenter__()
            self._browser = await self._pw.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--no-sandbox", "--disable-software-rasterizer",
                      "--disable-dev-shm-usage", "--disable-extensions"],
            )
            logger.info(f"浏览器启动（并发 {self._concurrency}）")

    async def _close_browser(self):
        async with self._browser_lock:
            b, self._browser = self._browser, None
            p, self._pw = self._pw, None
        if b:
            try:
                await b.close()
            except Exception:
                pass
        if p:
            try:
                await p.stop()
            except Exception:
                pass
            # 等待 node 进程退出
            await asyncio.sleep(0.5)

    # ==================== 主循环 ====================

    async def start(self):
        os.makedirs(self.html_dir, exist_ok=True)
        self._http_client = httpx.AsyncClient(timeout=30)
        logger.info(f"截图进程启动（并发: {self._concurrency}, 监控: {self.html_dir}）")

        try:
            while self._running:
                pending = self._scan_pending()
                if not pending:
                    self._check_batch_completion()
                    await asyncio.sleep(1)
                    continue

                await self._process_batch(pending)
                self._check_batch_completion()
        except KeyboardInterrupt:
            pass
        finally:
            await self._close_browser()
            if self._http_client:
                await self._http_client.aclose()
            logger.info("截图进程退出")

    # ==================== 扫描 + 批处理 ====================

    def _scan_pending(self) -> list:
        pending = []
        if not os.path.isdir(self.html_dir):
            return pending
        for batch_name in os.listdir(self.html_dir):
            batch_dir = os.path.join(self.html_dir, batch_name)
            if not os.path.isdir(batch_dir):
                continue
            if os.path.exists(os.path.join(self.base_dir, f"_uploaded_{batch_name}")):
                continue
            for fname in os.listdir(batch_dir):
                if fname.endswith(".html") and not fname.startswith("_"):
                    asin = fname[:-5]
                    pending.append((batch_name, asin, os.path.join(batch_dir, fname)))
        return pending

    async def _process_batch(self, pending: list):
        logger.info(f"处理 {len(pending)} 张截图")
        await self._ensure_browser()

        sem = asyncio.Semaphore(self._concurrency)

        async def do_one(batch_name, asin, html_path):
            async with sem:
                await self._render_upload(batch_name, asin, html_path)

        tasks = [asyncio.create_task(do_one(b, a, p)) for b, a, p in pending]
        await asyncio.gather(*tasks, return_exceptions=True)

        # 定期重启浏览器回收内存
        self._render_count += len(pending)
        if self._render_count >= self._restart_every:
            logger.info(f"已渲染 {self._render_count} 张，重启浏览器")
            await self._close_browser()
            self._render_count = 0

    # ==================== 单张截图：渲染 → 上传 → 清理 ====================

    async def _render_upload(self, batch_name: str, asin: str, html_path: str):
        png_bytes = None
        page = None
        try:
            with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                html = f.read()

            # 注入 <base> 让相对路径的 CSS/图片能正确加载
            base_tag = '<base href="https://www.amazon.com/">'
            lower = html[:2000].lower()
            if "<base " not in lower:
                pos = lower.find("<head")
                if pos != -1:
                    close = html.index(">", pos) + 1
                    html = html[:close] + base_tag + html[close:]
                else:
                    html = base_tag + html

            page = await self._browser.new_page(viewport={"width": 1280, "height": 1300})
            try:
                await page.set_content(html, wait_until="domcontentloaded", timeout=8000)
            except Exception:
                pass

            # 等待主图加载（3秒）
            await page.wait_for_timeout(3000)

            png_bytes = await page.screenshot(
                type="png", clip={"x": 0, "y": 0, "width": 1280, "height": 1300}
            )

        except Exception as e:
            err = str(e)
            if "browser has been closed" in err or "Target closed" in err:
                logger.error(f"浏览器崩溃，将重启: {asin}")
                await self._close_browser()
            else:
                logger.warning(f"渲染失败 {asin}: {e}")
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

        # 上传
        if png_bytes and len(png_bytes) > 0:
            ok = await self._upload(batch_name, asin, png_bytes)
            if ok:
                logger.info(f"截图完成: {asin} ({len(png_bytes) // 1024}KB)")
                try:
                    os.remove(html_path)
                except OSError:
                    pass
            else:
                logger.warning(f"上传失败: {asin}")
        else:
            logger.warning(f"渲染失败: {asin}")
            try:
                await self._http_client.post(
                    f"{self.server_url}/api/tasks/screenshot/fail",
                    json={"asin": asin, "batch_name": batch_name, "error": "render_failed"},
                    timeout=5,
                )
            except Exception:
                pass
            try:
                os.remove(html_path)
            except OSError:
                pass

    # ==================== 上传 ====================

    async def _upload(self, batch_name: str, asin: str, png_bytes: bytes) -> bool:
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
                logger.warning(f"上传失败 {asin}: HTTP {resp.status_code} ({attempt + 1}/3)")
            except Exception as e:
                logger.error(f"上传异常 {asin}: {e} ({attempt + 1}/3)")
            if attempt < 2:
                await asyncio.sleep(1)
        return False

    # ==================== 批次完成检测 ====================

    def _check_batch_completion(self):
        if not os.path.isdir(self.html_dir):
            return
        for batch_name in os.listdir(self.html_dir):
            batch_dir = os.path.join(self.html_dir, batch_name)
            if not os.path.isdir(batch_dir):
                continue
            uploaded_marker = os.path.join(self.base_dir, f"_uploaded_{batch_name}")
            if os.path.exists(uploaded_marker):
                continue
            if not os.path.exists(os.path.join(batch_dir, "_scraping_done")):
                continue
            remaining = [f for f in os.listdir(batch_dir)
                         if f.endswith(".html") and not f.startswith("_")]
            if remaining:
                continue
            with open(uploaded_marker, "w") as f:
                f.write(str(time.time()))
            logger.info(f"批次完成: {batch_name}")
            shutil.rmtree(batch_dir, ignore_errors=True)


# ==================== 入口 ====================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [SCREENSHOT] %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 2:
        print("Usage: python screenshot.py <server_url> [browsers_count] [pages_per_browser] [proxy_url]")
        sys.exit(1)

    server_url = sys.argv[1].rstrip("/")
    browsers_count = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    pages_per_browser = int(sys.argv[3]) if len(sys.argv) > 3 else 5
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
