"""
截图子进程 v3 — 融合 v2 截图准确性 + 测试脚本防泄漏模式。

架构：1 个 Playwright + 1 个 Chromium（常驻复用），Semaphore 控制并发。
渲染：base 注入 + 资源拦截 + 智能等主图 + 空白检测重试。
防泄漏：signal 优雅停机 + finally page.close；父进程负责进程组级兜底清理。
"""
import asyncio
import logging
import os
import shutil
import signal
import sys
import time
from typing import Optional

import httpx

logger = logging.getLogger("screenshot_worker")


class ScreenshotWorker:
    def __init__(self, server_url: str, base_dir: str = None,
                 browsers_count: int = 1, pages_per_browser: int = 5,
                 proxy_url: str = None, api_key: str = None):
        default_base_dir = os.environ.get("SCREENSHOT_BASE_DIR") or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "screenshot_cache"
        )
        self.server_url = server_url
        self._api_key = api_key or os.environ.get("WORKER_API_KEY", "")
        self.base_dir = base_dir or default_base_dir
        self.html_dir = os.path.join(self.base_dir, "html")
        self._concurrency = pages_per_browser
        self._pw = None
        self._browser = None
        self._browser_lock = asyncio.Lock()
        self._render_count = 0
        self._restart_every = 500
        self._running = True
        self._http_client: Optional[httpx.AsyncClient] = None

    # ==================== 浏览器生命周期 ====================

    def request_stop(self, reason: str = "signal"):
        """请求优雅退出，让 finally 负责收尾资源。"""
        if not self._running:
            return
        self._running = False
        logger.info(f"收到停止请求，准备退出当前截图循环 ({reason})")

    async def _ensure_browser(self):
        if self._browser:
            return
        async with self._browser_lock:
            if self._browser:
                return
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
                      "--disable-extensions", "--disable-software-rasterizer"],
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

    # ==================== 主循环 ====================

    async def start(self):
        os.makedirs(self.html_dir, exist_ok=True)
        server_headers = {}
        if self._api_key:
            server_headers["X-Worker-Api-Key"] = self._api_key
        self._http_client = httpx.AsyncClient(timeout=30, headers=server_headers)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda *_args, _sig=sig: self.request_stop(_sig.name))
        logger.info(f"截图进程启动（并发: {self._concurrency}, 监控: {self.html_dir}）")

        try:
            while self._running:
                pending = self._scan_pending()
                if not pending:
                    await self._check_batch_completion()
                    await asyncio.sleep(1)
                    continue
                await self._process_batch(pending)
                await self._check_batch_completion()
        except KeyboardInterrupt:
            pass
        finally:
            await self._close_browser()
            if self._http_client:
                await self._http_client.aclose()
            logger.info("截图进程退出")

    # ==================== 扫描 ====================

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
            self._recover_inflight_files(batch_dir)
            for fname in os.listdir(batch_dir):
                if fname.endswith(".html") and not fname.startswith("_"):
                    pending.append((batch_name, fname[:-5], os.path.join(batch_dir, fname)))
        return pending

    # ==================== 批处理 ====================

    async def _process_batch(self, pending: list):
        logger.info(f"处理 {len(pending)} 张截图")
        await self._ensure_browser()
        sem = asyncio.Semaphore(self._concurrency)

        async def do_one(batch_name, asin, html_path):
            async with sem:
                await self._render_upload(batch_name, asin, html_path)

        tasks = [asyncio.create_task(do_one(b, a, p)) for b, a, p in pending]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"截图任务未捕获异常: {type(result).__name__}: {result}")

        self._render_count += len(pending)
        if self._render_count >= self._restart_every:
            logger.info(f"已渲染 {self._render_count} 张，重启浏览器")
            await self._close_browser()
            self._render_count = 0

    # ==================== 单张截图 ====================

    async def _render_upload(self, batch_name: str, asin: str, html_path: str):
        logger.info(f"开始截图: {asin}")
        processing_path = self._claim_html_file(html_path)
        if not processing_path:
            logger.warning(f"截图源文件在占用前消失: {asin}")
            return
        logger.info(f"占用截图源文件: {asin} -> {os.path.basename(processing_path)}")

        # 读取 HTML
        try:
            with open(processing_path, "r", encoding="utf-8", errors="replace") as f:
                html = f.read()
        except FileNotFoundError:
            logger.warning(f"截图源文件在占用后消失: {asin}")
            return
        except Exception as e:
            logger.error(f"读取截图 HTML 失败 {asin}: {e}")
            self._restore_inflight_file(processing_path)
            return

        if not html or len(html) < 500:
            logger.warning(f"HTML 过短: {asin} ({len(html)}B)")
            await self._mark_terminal_failure(batch_name, asin, processing_path, "html_too_short")
            return

        # 注入 <base>
        lower = html[:2000].lower()
        if "<base " not in lower:
            pos = lower.find("<head")
            if pos != -1:
                close = html.index(">", pos) + 1
                html = html[:close] + '<base href="https://www.amazon.com/">' + html[close:]

        # 渲染（含空白检测重试，最多 3 次）
        png_bytes = None
        for attempt in range(3):
            try:
                png_bytes, has_content = await asyncio.wait_for(
                    self._render_one(html, asin), timeout=45
                )
            except asyncio.TimeoutError:
                logger.error(f"截图超时: {asin} (attempt={attempt+1}/3)")
                png_bytes, has_content = None, False
                await self._close_browser()
                await self._ensure_browser()

            if png_bytes and (len(png_bytes) >= 10240 or has_content):
                break  # 正常

            if png_bytes and len(png_bytes) < 10240 and not has_content:
                logger.warning(f"空白截图 {asin} ({len(png_bytes)}B) 第{attempt+1}次，重试...")
                png_bytes = None
                await asyncio.sleep(1)
            else:
                break  # 渲染异常，不重试

        # 上传
        if png_bytes and len(png_bytes) > 0:
            ok = await self._upload(batch_name, asin, png_bytes)
            logger.info(f"截图上传结果: {asin} ok={ok}")
            if ok:
                logger.info(f"截图完成: {asin} ({len(png_bytes)//1024}KB)")
                try:
                    os.remove(processing_path)
                except OSError:
                    pass
            else:
                logger.warning(f"上传失败: {asin}")
                self._restore_inflight_file(processing_path)
        else:
            await self._mark_terminal_failure(batch_name, asin, processing_path, "render_failed")

    async def _mark_terminal_failure(self, batch_name: str, asin: str, html_path: str, error: str):
        logger.warning(f"截图最终失败: {asin} ({error})")
        # 重命名为 .failed 保留供排查，服务端直接记为 failed 终态。
        try:
            os.replace(html_path, self._failed_path(html_path))
        except OSError:
            pass
        try:
            await self._http_client.post(
                f"{self.server_url}/api/tasks/screenshot/fail",
                json={"asin": asin, "batch_name": batch_name, "error": error},
                timeout=5,
            )
        except Exception as e:
            logger.error(f"上报截图失败异常 {asin}: {e}")

    async def _render_one(self, html: str, asin: str) -> tuple:
        """渲染单张，返回 (png_bytes, has_content)。参照 v2 渲染逻辑。"""
        page = None
        try:
            page = await self._browser.new_page(viewport={"width": 1280, "height": 1300})

            # 资源拦截：放行 CSS/图片，屏蔽 JS/字体/广告
            async def block_resources(route):
                rt = route.request.resource_type
                url = route.request.url
                if rt in ("stylesheet", "image"):
                    await route.continue_()
                elif rt in ("script", "font", "media", "websocket", "manifest", "other"):
                    await route.abort()
                elif any(x in url for x in ("analytics", "tracking", "beacon",
                                            "ads", "doubleclick", "facebook")):
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_resources)

            try:
                await page.set_content(html, wait_until="domcontentloaded", timeout=5000)
            except Exception:
                pass

            # 智能等待主图加载（最多 7 秒）
            try:
                await page.evaluate("""() => new Promise((resolve) => {
                    const selectors = [
                        '#landingImage', '#imgBlkFront', '#main-image',
                        '#imgTagWrapperId img', '#imageBlock img[src*="images-amazon"]'
                    ];
                    let img = null;
                    for (const sel of selectors) {
                        img = document.querySelector(sel);
                        if (img) break;
                    }
                    if (!img) return resolve(false);
                    if (img.complete && img.naturalWidth > 0) return resolve(true);
                    img.addEventListener('load', () => resolve(true), {once: true});
                    img.addEventListener('error', () => resolve(false), {once: true});
                    setTimeout(() => resolve(false), 7000);
                })""")
            except Exception:
                pass

            # 空白检测
            has_content = await page.evaluate("""() => {
                if (!document.body) return false;
                const text = document.body.innerText || '';
                if (text.trim().length > 50) return true;
                const imgs = document.querySelectorAll('img[src]');
                if (imgs.length > 0) return true;
                return false;
            }""")

            png_bytes = await page.screenshot(
                type="png", clip={"x": 0, "y": 0, "width": 1280, "height": 1300}
            )
            return png_bytes, has_content

        except Exception as e:
            err = str(e)
            if "browser has been closed" in err or "Target closed" in err:
                logger.error(f"浏览器崩溃: {asin}")
                await self._close_browser()
            else:
                logger.warning(f"渲染异常 {asin}: {e}")
            return None, False
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
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
                logger.warning(f"上传失败 {asin}: HTTP {resp.status_code} ({attempt+1}/3)")
            except Exception as e:
                logger.error(f"上传异常 {asin}: {e} ({attempt+1}/3)")
            if attempt < 2:
                await asyncio.sleep(1)
        return False

    # ==================== 批次完成 ====================

    async def _check_batch_completion(self):
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
                         if (f.endswith(".html") or f.endswith(".processing")) and not f.startswith("_")]
            if remaining:
                continue
            progress = await self._get_screenshot_progress(batch_name)
            total = progress.get("total", 0)
            finished = progress.get("done", 0) + progress.get("failed", 0)
            if total <= 0 or finished < total:
                logger.warning(
                    f"批次 {batch_name} 本地无待处理 HTML，但服务端截图未完成: "
                    f"done={progress.get('done', 0)} failed={progress.get('failed', 0)} total={total}"
                )
                continue
            with open(uploaded_marker, "w") as f:
                f.write(str(time.time()))
            logger.info(
                f"批次完成: {batch_name} "
                f"(done={progress.get('done', 0)} failed={progress.get('failed', 0)} total={total})"
            )
            shutil.rmtree(batch_dir, ignore_errors=True)

    async def _get_screenshot_progress(self, batch_name: str) -> dict:
        try:
            resp = await self._http_client.get(
                f"{self.server_url}/api/batches/{batch_name}/screenshots/progress",
                timeout=5,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return {"pending": 0, "processing": 0, "done": 0, "failed": 0, "total": 0}

    def _processing_path(self, html_path: str) -> str:
        if html_path.endswith(".html"):
            return html_path[:-5] + ".processing"
        return html_path

    def _failed_path(self, inflight_path: str) -> str:
        if inflight_path.endswith(".html"):
            return inflight_path[:-5] + ".failed"
        if inflight_path.endswith(".processing"):
            return inflight_path[:-11] + ".failed"
        return inflight_path + ".failed"

    def _html_path(self, inflight_path: str) -> str:
        if inflight_path.endswith(".processing"):
            return inflight_path[:-11] + ".html"
        return inflight_path

    def _claim_html_file(self, html_path: str) -> Optional[str]:
        processing_path = self._processing_path(html_path)
        try:
            os.replace(html_path, processing_path)
            return processing_path
        except FileNotFoundError:
            return None
        except OSError as e:
            logger.warning(f"标记截图处理中失败 {os.path.basename(html_path)}: {e}")
            return None

    def _restore_inflight_file(self, inflight_path: str):
        html_path = self._html_path(inflight_path)
        if inflight_path == html_path or not os.path.exists(inflight_path):
            return
        try:
            os.replace(inflight_path, html_path)
        except OSError as e:
            logger.warning(f"恢复截图任务失败 {os.path.basename(inflight_path)}: {e}")

    def _recover_inflight_files(self, batch_dir: str):
        for fname in os.listdir(batch_dir):
            if not fname.endswith(".processing") or fname.startswith("_"):
                continue
            inflight_path = os.path.join(batch_dir, fname)
            html_path = self._html_path(inflight_path)
            if os.path.exists(html_path):
                continue
            try:
                os.replace(inflight_path, html_path)
                logger.warning(f"恢复中断截图任务: {os.path.basename(html_path)}")
            except OSError as e:
                logger.warning(f"恢复中断截图任务失败 {fname}: {e}")


# ==================== 入口 ====================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [SCREENSHOT] %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 2:
        print("Usage: python screenshot.py <server_url> [browsers_count] [pages_per_browser]")
        sys.exit(1)

    server_url = sys.argv[1].rstrip("/")
    browsers_count = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    pages_per_browser = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    worker = ScreenshotWorker(
        server_url=server_url,
        browsers_count=browsers_count,
        pages_per_browser=pages_per_browser,
    )

    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
