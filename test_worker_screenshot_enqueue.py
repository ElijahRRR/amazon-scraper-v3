import os
import tempfile
import unittest

from worker.engine import Worker


class WorkerScreenshotEnqueueTest(unittest.IsolatedAsyncioTestCase):
    async def test_missing_product_enqueue_writes_placeholder_html(self):
        worker = Worker(server_url="http://127.0.0.1:8899", worker_id="test-worker", enable_screenshot=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            worker._screenshot_base_dir = tmpdir
            worker._screenshot_html_dir = os.path.join(tmpdir, "html")

            started = {"called": False}

            async def fake_ensure():
                started["called"] = True

            worker._ensure_screenshot_process = fake_ensure

            html = worker._build_missing_product_html("BTEST404404")
            self.assertGreater(len(html), 500)

            await worker._enqueue_screenshot_html(
                asin="BTEST404404",
                batch_name="batch-404",
                batch_id=12,
                html_content=html,
            )

            html_path = os.path.join(worker._screenshot_html_dir, "batch-404", "BTEST404404.html")
            self.assertTrue(os.path.exists(html_path))
            self.assertIn("batch-404", worker._screenshot_pending_batches)
            self.assertEqual(worker._screenshot_batch_ids["batch-404"], 12)
            self.assertTrue(started["called"])


if __name__ == "__main__":
    unittest.main()
