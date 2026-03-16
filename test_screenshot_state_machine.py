import os
import tempfile
import unittest

from worker.screenshot import ScreenshotWorker


class FakeScreenshotWorker(ScreenshotWorker):
    def __init__(self, base_dir: str, upload_result: bool = True, progress=None):
        super().__init__("http://127.0.0.1:0", base_dir=base_dir, pages_per_browser=1)
        self.upload_result = upload_result
        self.progress = progress or {"pending": 0, "processing": 0, "done": 1, "failed": 0, "total": 1}

    async def _render_one(self, html: str, asin: str) -> tuple:
        return (b"x" * 20000, True)

    async def _upload(self, batch_name: str, asin: str, png_bytes: bytes) -> bool:
        return self.upload_result

    async def _get_screenshot_progress(self, batch_name: str) -> dict:
        return dict(self.progress)


class ScreenshotStateMachineTest(unittest.IsolatedAsyncioTestCase):
    def _write_html(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("<html><head></head><body>" + ("x" * 1000) + "</body></html>")

    async def test_render_upload_requeues_html_when_upload_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            worker = FakeScreenshotWorker(tmpdir, upload_result=False)
            html_path = os.path.join(tmpdir, "html", "batch-a", "ATEST000001.html")
            self._write_html(html_path)

            await worker._render_upload("batch-a", "ATEST000001", html_path)

            self.assertTrue(os.path.exists(html_path))
            self.assertFalse(os.path.exists(html_path.replace(".html", ".processing")))
            self.assertFalse(os.path.exists(html_path.replace(".html", ".failed")))

    async def test_scan_pending_recovers_processing_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            worker = FakeScreenshotWorker(tmpdir)
            batch_dir = os.path.join(tmpdir, "html", "batch-b")
            os.makedirs(batch_dir, exist_ok=True)
            processing_path = os.path.join(batch_dir, "ATEST000002.processing")
            self._write_html(processing_path)

            pending = worker._scan_pending()

            self.assertEqual(
                pending,
                [("batch-b", "ATEST000002", os.path.join(batch_dir, "ATEST000002.html"))],
            )
            self.assertTrue(os.path.exists(os.path.join(batch_dir, "ATEST000002.html")))
            self.assertFalse(os.path.exists(processing_path))

    async def test_check_batch_completion_waits_for_processing_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            worker = FakeScreenshotWorker(tmpdir, progress={"pending": 0, "processing": 0, "done": 1, "failed": 0, "total": 1})
            batch_dir = os.path.join(tmpdir, "html", "batch-c")
            os.makedirs(batch_dir, exist_ok=True)
            with open(os.path.join(batch_dir, "_scraping_done"), "w", encoding="utf-8") as f:
                f.write("1")
            self._write_html(os.path.join(batch_dir, "ATEST000003.processing"))

            await worker._check_batch_completion()

            self.assertTrue(os.path.isdir(batch_dir))
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "_uploaded_batch-c")))


if __name__ == "__main__":
    unittest.main()
