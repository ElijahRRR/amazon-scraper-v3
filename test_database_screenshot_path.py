import os
import tempfile
import unittest

from common.database import Database


class ScreenshotPathRegressionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = Database(self.db_path)
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    async def test_save_result_keeps_done_screenshot_when_upload_happens_first(self):
        batch_id = await self.db.create_batch("race-batch", needs_screenshot=True)
        await self.db.create_tasks(batch_id, ["BTEST123456"], needs_screenshot=True)
        expected_path = "/static/screenshots/race-batch/BTEST123456.png"

        await self.db.update_screenshot_status(
            "BTEST123456",
            batch_id,
            "done",
            file_path=expected_path,
        )
        await self.db.save_result(
            {
                "asin": "BTEST123456",
                "title": "Race Product",
                "brand": "Brand",
                "current_price": "$9.99",
                "buybox_price": "$9.99",
                "stock_status": "In Stock",
            },
            batch_id=batch_id,
        )

        row = await self.db.get_result_by_asin("BTEST123456")
        self.assertEqual(row["screenshot_path"], expected_path)

    async def test_get_results_falls_back_when_main_table_has_string_none(self):
        batch_id = await self.db.create_batch("fallback-batch", needs_screenshot=True)
        await self.db.create_tasks(batch_id, ["BTEST654321"], needs_screenshot=True)
        expected_path = "/static/screenshots/fallback-batch/BTEST654321.png"

        await self.db.save_result(
            {
                "asin": "BTEST654321",
                "title": "Fallback Product",
                "brand": "Brand",
                "current_price": "$19.99",
                "buybox_price": "$19.99",
                "stock_status": "In Stock",
            },
            batch_id=batch_id,
        )
        await self.db.update_screenshot_status(
            "BTEST654321",
            batch_id,
            "done",
            file_path=expected_path,
        )
        await self.db._db.execute(
            "UPDATE asin_data SET screenshot_path = 'None' WHERE asin = ?",
            ("BTEST654321",),
        )

        results = await self.db.get_results(
            batch_id=batch_id,
            limit=10,
            search="BTEST654321",
        )

        self.assertEqual(results["items"][0]["screenshot_path"], expected_path)


if __name__ == "__main__":
    unittest.main()
