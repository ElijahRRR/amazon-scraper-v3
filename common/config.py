"""
Amazon ASIN 采集系统 v3 - 配置
server 和 worker 共用的配置项
"""
import os

# 加载 .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    _env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.isfile(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _key, _, _val = _line.partition("=")
                    os.environ.setdefault(_key.strip(), _val.strip())

# ============================================================
# 基础路径
# ============================================================
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_DIR, "data", "scraper.db")
EXPORT_DIR = os.path.join(PROJECT_DIR, "data", "exports")
SCREENSHOT_DIR = os.path.join(PROJECT_DIR, "server", "static", "screenshots")
TEMPLATE_DIR = os.path.join(PROJECT_DIR, "server", "templates")
STATIC_DIR = os.path.join(PROJECT_DIR, "server", "static")

# ============================================================
# 服务器配置
# ============================================================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8899"))

# ============================================================
# 采集配置
# ============================================================
DEFAULT_ZIP_CODE = os.environ.get("DEFAULT_ZIP_CODE", "10001")
MAX_CLIENTS = 32
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
TASK_TIMEOUT_MINUTES = 5
SESSION_ROTATE_EVERY = 1000

# 令牌桶限流
TOKEN_BUCKET_RATE = 64.0

# ============================================================
# 自适应并发控制
# ============================================================
INITIAL_CONCURRENCY = 16
MIN_CONCURRENCY = 16
MAX_CONCURRENCY = 32

PROXY_BANDWIDTH_MBPS = 0
ADJUST_INTERVAL_S = 3
TARGET_LATENCY_S = 5.0
MAX_LATENCY_S = 8.0
TARGET_SUCCESS_RATE = 0.95
MIN_SUCCESS_RATE = 0.85
BLOCK_RATE_THRESHOLD = 0.05
BANDWIDTH_SOFT_CAP = 0.80
GLOBAL_MAX_CONCURRENCY = 1000
GLOBAL_MAX_QPS = 1000.0

TASK_QUEUE_SIZE = 100
TASK_PREFETCH_THRESHOLD = 0.5

# ============================================================
# 代理配置
# ============================================================
# TPS 模式（每次请求自动换 IP）
PROXY_URL = os.environ.get("PROXY_URL", "")

# ============================================================
# 浏览器指纹
# ============================================================
BROWSER_PROFILES = [
    {
        "impersonate": "chrome120",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome123",
        "sec_ch_ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome124",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome131",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome136",
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        ],
    },
]

USER_AGENTS = []
for _p in BROWSER_PROFILES:
    USER_AGENTS.extend(_p["user_agents"])

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# ============================================================
# 导出配置
# ============================================================
HEADER_MAP = {
    "crawl_time": "商品采集时间",
    "zip_code": "配送邮编",
    "product_url": "产品链接",
    "asin": "ASIN (商品ID)",
    "title": "商品标题",
    "original_price": "商品原价",
    "current_price": "当前价格",
    "buybox_price": "BuyBox 价格",
    "buybox_shipping": "BuyBox 运费",
    "is_fba": "是否 FBA 发货",
    "stock_count": "库存数量",
    "stock_status": "库存状态",
    "delivery_date": "配送到达时间",
    "delivery_time": "配送时长",
    "brand": "品牌",
    "model_number": "产品型号",
    "country_of_origin": "原产国",
    "is_customized": "是否为定制产品",
    "best_sellers_rank": "畅销排名",
    "upc_list": "UPC 列表",
    "ean_list": "EAN 列表",
    "package_dimensions": "包装尺寸",
    "package_weight": "包装重量",
    "item_dimensions": "商品本体尺寸",
    "item_weight": "商品本体重量",
    "parent_asin": "父体 ASIN",
    "variation_asins": "变体 ASIN 列表",
    "root_category_id": "根类目 ID",
    "category_ids": "类目 ID 链",
    "category_tree": "类目路径树",
    "bullet_points": "五点描述",
    "image_urls": "商品图片链接",
    "site": "站点",
    "manufacturer": "制造商",
    "part_number": "部件编号",
    "first_available_date": "上架时间",
    "long_description": "长描述",
    "product_type": "商品类型",
    "total_price": "总价",
}

EXPORT_COLUMN_ORDER = [
    "商品采集时间", "配送邮编", "产品链接", "ASIN (商品ID)", "商品标题",
    "商品原价", "当前价格", "BuyBox 价格", "BuyBox 运费", "总价", "是否 FBA 发货",
    "库存数量", "库存状态", "配送到达时间", "配送时长", "品牌", "产品型号",
    "原产国", "是否为定制产品", "畅销排名", "UPC 列表", "EAN 列表",
    "包装尺寸", "包装重量", "商品本体尺寸", "商品本体重量", "父体 ASIN",
    "变体 ASIN 列表", "根类目 ID", "类目路径树", "五点描述", "商品图片链接",
    "站点", "制造商", "部件编号", "上架时间", "长描述", "商品类型", "类目 ID 链",
]

# ============================================================
# 启动校验
# ============================================================
assert MIN_CONCURRENCY <= INITIAL_CONCURRENCY <= MAX_CONCURRENCY
assert TARGET_LATENCY_S < MAX_LATENCY_S
