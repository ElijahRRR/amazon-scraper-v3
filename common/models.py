"""
Amazon ASIN 采集系统 v3 - 数据模型
"""
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime


@dataclass
class Task:
    id: int = 0
    batch_id: int = 0
    asin: str = ""
    zip_code: str = "10001"
    status: str = "pending"  # pending / processing / done / failed
    priority: int = 0
    needs_screenshot: bool = False
    worker_id: str = ""
    retry_count: int = 0
    error_type: str = ""
    error_detail: str = ""
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AsinData:
    """当前 ASIN 数据（主表，每个 ASIN 一行）"""
    id: int = 0
    asin: str = ""
    title: str = ""
    brand: str = ""
    product_type: str = ""
    manufacturer: str = ""
    model_number: str = ""
    part_number: str = ""
    country_of_origin: str = ""
    is_customized: str = ""
    best_sellers_rank: str = ""
    original_price: str = ""
    current_price: str = ""
    buybox_price: str = ""
    buybox_shipping: str = ""
    is_fba: str = ""
    stock_count: str = ""
    stock_status: str = ""
    delivery_date: str = ""
    delivery_time: str = ""
    image_urls: str = ""
    bullet_points: str = ""
    long_description: str = ""
    upc_list: str = ""
    ean_list: str = ""
    parent_asin: str = ""
    variation_asins: str = ""
    root_category_id: str = ""
    category_ids: str = ""
    category_tree: str = ""
    first_available_date: str = ""
    package_dimensions: str = ""
    package_weight: str = ""
    item_dimensions: str = ""
    item_weight: str = ""
    product_url: str = ""
    site: str = "amazon.com"
    zip_code: str = "10001"
    crawl_time: str = ""
    screenshot_path: str = ""
    content_hash: str = ""
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Batch:
    id: int = 0
    name: str = ""
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    needs_screenshot: bool = False
    created_at: str = ""
    updated_at: str = ""


# 变动类型枚举
CHANGE_TYPE_PRICE_STOCK = "price_stock"
CHANGE_TYPE_TITLE_BULLETS = "title_bullets"
CHANGE_TYPE_NEW = "new"

# 导出可选字段（排除内部字段，加虚拟字段 total_price）
_INTERNAL_FIELDS = {"id", "content_hash", "title_bullets_hash", "created_at", "updated_at", "screenshot_path"}
EXPORTABLE_FIELDS = [f.name for f in __import__("dataclasses").fields(AsinData) if f.name not in _INTERNAL_FIELDS] + ["total_price"]
