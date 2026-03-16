"""
截图技术栈探索测试脚本
对比不同方案的 CPU 占用、速度、成功率

测试 HTML 来源: worker/screenshot_cache/html/ss500/
输出目录: /tmp/ss_test_output/
"""
import asyncio
import os
import sys
import time
import statistics
import psutil

HTML_DIR = "worker/screenshot_cache/html/ss500"
OUTPUT_DIR = "/tmp/ss_test_output"


def get_cpu_percent():
    """获取当前系统 CPU 使用率"""
    return psutil.cpu_percent(interval=0.5)


class CpuMonitor:
    """后台监控 CPU 使用率"""
    def __init__(self):
        self.samples = []
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while self._running:
            self.samples.append(psutil.cpu_percent(interval=0))
            await asyncio.sleep(1)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def avg(self):
        return statistics.mean(self.samples) if self.samples else 0

    @property
    def peak(self):
        return max(self.samples) if self.samples else 0

# 收集所有 HTML 文件
def get_html_files(limit=20):
    files = []
    for f in sorted(os.listdir(HTML_DIR)):
        if f.endswith(".html") and not f.startswith("_"):
            files.append((f[:-5], os.path.join(HTML_DIR, f)))
            if len(files) >= limit:
                break
    return files


def inject_base_tag(html):
    base_tag = '<base href="https://www.amazon.com/">'
    lower = html[:2000].lower()
    if "<base " not in lower:
        pos = lower.find("<head")
        if pos != -1:
            close = html.index(">", pos) + 1
            return html[:close] + base_tag + html[close:]
        return base_tag + html
    return html


# ============================================================
# 方案 1: Playwright (v2 原方案)
# ============================================================
async def test_playwright(files, output_subdir):
    from playwright.async_api import async_playwright

    os.makedirs(output_subdir, exist_ok=True)
    results = {"ok": 0, "fail": 0, "times": [], "sizes": [], "cpu_avg": 0, "cpu_peak": 0}

    cpu = CpuMonitor()
    await cpu.start()

    pw = await async_playwright().__aenter__()
    browser = await pw.chromium.launch(
        headless=True,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
              "--disable-extensions", "--disable-software-rasterizer",
              "--disable-background-networking", "--js-flags=--max-old-space-size=128"],
    )
    concurrency = int(os.environ.get("PW_CONCURRENCY", "1"))
    print(f"  Playwright 浏览器已启动, 并发={concurrency}")

    sem = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()

    async def process_one(asin, html_path):
        async with sem:
            t0 = time.time()
            try:
                with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                    html = inject_base_tag(f.read())

                page = await browser.new_page(viewport={"width": 1280, "height": 1300})

                async def block(route):
                    rt = route.request.resource_type
                    if rt in ("stylesheet", "image"):
                        await route.continue_()
                    elif rt in ("script", "font", "media", "websocket", "manifest"):
                        await route.abort()
                    else:
                        await route.continue_()
                await page.route("**/*", block)

                await page.set_content(html, wait_until="load", timeout=10000)
                await page.wait_for_timeout(2000)

                png = await page.screenshot(type="png", clip={"x": 0, "y": 0, "width": 1280, "height": 1300})
                await page.close()

                elapsed = time.time() - t0
                out_path = os.path.join(output_subdir, f"{asin}.png")
                with open(out_path, "wb") as f:
                    f.write(png)

                async with lock:
                    results["ok"] += 1
                    results["times"].append(elapsed)
                    results["sizes"].append(len(png))
                print(f"    ✅ {asin}: {len(png)//1024}KB {elapsed:.1f}s")

            except Exception as e:
                async with lock:
                    results["fail"] += 1
                    results["times"].append(time.time() - t0)
                print(f"    ❌ {asin}: {e}")

    tasks = [asyncio.create_task(process_one(asin, path)) for asin, path in files]
    await asyncio.gather(*tasks)

    await browser.close()
    try:
        await pw.stop()
    except Exception:
        pass

    await cpu.stop()
    results["cpu_avg"] = cpu.avg
    results["cpu_peak"] = cpu.peak
    return results


# ============================================================
# 方案 2: wkhtmltoimage (如果已安装)
# ============================================================
async def test_wkhtmltoimage(files, output_subdir):
    import shutil
    wk = shutil.which("wkhtmltoimage")
    if not wk:
        print("  wkhtmltoimage 未安装，跳过")
        return None

    os.makedirs(output_subdir, exist_ok=True)
    results = {"ok": 0, "fail": 0, "times": [], "sizes": []}

    for asin, html_path in files:
        t0 = time.time()
        out_path = os.path.join(output_subdir, f"{asin}.png")
        try:
            proc = await asyncio.create_subprocess_exec(
                wk, "--width", "1280", "--height", "1300",
                "--quality", "80", "--disable-javascript",
                html_path, out_path,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)

            if proc.returncode == 0 and os.path.exists(out_path):
                size = os.path.getsize(out_path)
                elapsed = time.time() - t0
                results["ok"] += 1
                results["times"].append(elapsed)
                results["sizes"].append(size)
                print(f"    ✅ {asin}: {size//1024}KB {elapsed:.1f}s")
            else:
                results["fail"] += 1
                results["times"].append(time.time() - t0)
                print(f"    ❌ {asin}: exit={proc.returncode}")
        except Exception as e:
            results["fail"] += 1
            results["times"].append(time.time() - t0)
            print(f"    ❌ {asin}: {e}")

    return results


# ============================================================
# 方案 3: Pyppeteer (Puppeteer Python 版)
# ============================================================
async def test_pyppeteer(files, output_subdir):
    try:
        import pyppeteer
    except ImportError:
        print("  pyppeteer 未安装，跳过")
        return None

    os.makedirs(output_subdir, exist_ok=True)
    results = {"ok": 0, "fail": 0, "times": [], "sizes": []}

    browser = await pyppeteer.launch(
        headless=True,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
              "--disable-extensions", "--disable-software-rasterizer"]
    )

    for asin, html_path in files:
        t0 = time.time()
        try:
            with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                html = inject_base_tag(f.read())

            page = await browser.newPage()
            await page.setViewport({"width": 1280, "height": 1300})
            await page.setContent(html, {"waitUntil": "load", "timeout": 10000})
            await asyncio.sleep(2)

            png = await page.screenshot({"type": "png", "clip": {"x": 0, "y": 0, "width": 1280, "height": 1300}})
            await page.close()

            elapsed = time.time() - t0
            out_path = os.path.join(output_subdir, f"{asin}.png")
            with open(out_path, "wb") as f:
                f.write(png)

            results["ok"] += 1
            results["times"].append(elapsed)
            results["sizes"].append(len(png))
            print(f"    ✅ {asin}: {len(png)//1024}KB {elapsed:.1f}s")
        except Exception as e:
            results["fail"] += 1
            results["times"].append(time.time() - t0)
            print(f"    ❌ {asin}: {e}")

    await browser.close()
    return results


# ============================================================
# 方案 4: Chrome headless CLI (直接调用系统 Chrome，无 Playwright 开销)
# ============================================================
async def test_chrome_cli(files, output_subdir):
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if not os.path.exists(chrome):
        print("  Chrome 未安装，跳过")
        return None

    os.makedirs(output_subdir, exist_ok=True)
    results = {"ok": 0, "fail": 0, "times": [], "sizes": [], "cpu_avg": 0, "cpu_peak": 0}

    cpu = CpuMonitor()
    await cpu.start()

    for asin, html_path in files:
        t0 = time.time()
        out_path = os.path.join(output_subdir, f"{asin}.png")
        abs_html = os.path.abspath(html_path)
        try:
            proc = await asyncio.create_subprocess_exec(
                chrome,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-extensions",
                "--disable-software-rasterizer",
                "--hide-scrollbars",
                f"--window-size=1280,1300",
                f"--screenshot={out_path}",
                f"file://{abs_html}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)

            if os.path.exists(out_path):
                size = os.path.getsize(out_path)
                elapsed = time.time() - t0
                if size > 5000:
                    results["ok"] += 1
                    results["times"].append(elapsed)
                    results["sizes"].append(size)
                    print(f"    ✅ {asin}: {size//1024}KB {elapsed:.1f}s")
                else:
                    results["fail"] += 1
                    results["times"].append(elapsed)
                    print(f"    ❌ {asin}: 太小 {size}B")
            else:
                results["fail"] += 1
                results["times"].append(time.time() - t0)
                print(f"    ❌ {asin}: 无输出文件")
        except asyncio.TimeoutError:
            results["fail"] += 1
            results["times"].append(time.time() - t0)
            print(f"    ❌ {asin}: 超时")
        except Exception as e:
            results["fail"] += 1
            results["times"].append(time.time() - t0)
            print(f"    ❌ {asin}: {e}")

    await cpu.stop()
    results["cpu_avg"] = cpu.avg
    results["cpu_peak"] = cpu.peak
    return results


# ============================================================
# 方案 5: imgkit (wkhtmltoimage 的 Python 封装)
# ============================================================
async def test_imgkit(files, output_subdir):
    try:
        import imgkit
    except ImportError:
        print("  imgkit 未安装，跳过")
        return None

    os.makedirs(output_subdir, exist_ok=True)
    results = {"ok": 0, "fail": 0, "times": [], "sizes": []}

    options = {
        "width": 1280,
        "height": 1300,
        "quality": 80,
        "disable-javascript": "",
        "no-stop-slow-scripts": "",
    }

    for asin, html_path in files:
        t0 = time.time()
        out_path = os.path.join(output_subdir, f"{asin}.png")
        try:
            imgkit.from_file(html_path, out_path, options=options)
            size = os.path.getsize(out_path)
            elapsed = time.time() - t0
            results["ok"] += 1
            results["times"].append(elapsed)
            results["sizes"].append(size)
            print(f"    ✅ {asin}: {size//1024}KB {elapsed:.1f}s")
        except Exception as e:
            results["fail"] += 1
            results["times"].append(time.time() - t0)
            print(f"    ❌ {asin}: {e}")

    return results


# ============================================================
# 汇总
# ============================================================
def print_summary(name, results):
    if results is None:
        print(f"  {name}: 跳过（未安装）")
        return
    total = results["ok"] + results["fail"]
    rate = results["ok"] / total * 100 if total else 0
    avg_time = statistics.mean(results["times"]) if results["times"] else 0
    avg_size = statistics.mean(results["sizes"]) if results["sizes"] else 0
    print(f"  {name}:")
    print(f"    成功率: {results['ok']}/{total} ({rate:.0f}%)")
    print(f"    平均耗时: {avg_time:.2f}s")
    print(f"    平均大小: {avg_size/1024:.0f}KB")
    if results["times"]:
        print(f"    总耗时: {sum(results['times']):.1f}s")
    print(f"    CPU 平均: {results.get('cpu_avg', 0):.1f}%")
    print(f"    CPU 峰值: {results.get('cpu_peak', 0):.1f}%")


async def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    files = get_html_files(limit)
    print(f"测试 {len(files)} 个 HTML 文件\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_results = {}

    # 方案 1: Playwright
    print("=" * 50)
    print("方案 1: Playwright (Chromium headless)")
    print("=" * 50)
    all_results["Playwright"] = await test_playwright(files, os.path.join(OUTPUT_DIR, "playwright"))

    # 方案 2: wkhtmltoimage
    print("\n" + "=" * 50)
    print("方案 2: wkhtmltoimage")
    print("=" * 50)
    all_results["wkhtmltoimage"] = await test_wkhtmltoimage(files, os.path.join(OUTPUT_DIR, "wkhtml"))

    # 方案 3: Pyppeteer
    print("\n" + "=" * 50)
    print("方案 3: Pyppeteer (Puppeteer Python)")
    print("=" * 50)
    all_results["Pyppeteer"] = await test_pyppeteer(files, os.path.join(OUTPUT_DIR, "pyppeteer"))

    # 方案 4: Chrome CLI
    print("\n" + "=" * 50)
    print("方案 4: Chrome headless CLI (直接调用)")
    print("=" * 50)
    all_results["Chrome CLI"] = await test_chrome_cli(files, os.path.join(OUTPUT_DIR, "chrome_cli"))

    # 方案 5: imgkit
    print("\n" + "=" * 50)
    print("方案 5: imgkit")
    print("=" * 50)
    all_results["imgkit"] = await test_imgkit(files, os.path.join(OUTPUT_DIR, "imgkit"))

    # 汇总对比
    print("\n" + "=" * 50)
    print("汇总对比")
    print("=" * 50)
    for name, results in all_results.items():
        print_summary(name, results)

    print(f"\n输出目录: {OUTPUT_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
