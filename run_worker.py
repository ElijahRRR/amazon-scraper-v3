"""启动 worker"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import asyncio
from worker.engine import Worker


def main():
    parser = argparse.ArgumentParser(description="Amazon Scraper v3 Worker")
    parser.add_argument("--server", required=True, help="Server URL (e.g., http://x.x.x.x:8899)")
    parser.add_argument("--worker-id", default=None, help="Worker ID (auto-generated if not set)")
    parser.add_argument("--concurrency", type=int, default=None, help="Initial concurrency")
    parser.add_argument("--zip-code", default=None, help="Delivery zip code")
    parser.add_argument("--no-screenshot", action="store_true", help="Disable screenshot")
    parser.add_argument("--api-key", default=None, help="ERP Server Worker API Key (or set WORKER_API_KEY env)")
    args = parser.parse_args()

    worker = Worker(
        server_url=args.server,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        zip_code=args.zip_code,
        enable_screenshot=not args.no_screenshot,
        api_key=args.api_key,
    )
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
