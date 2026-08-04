from pathlib import Path
import os

APP_NAME = "Koju Dhan Algo"
DATA_DIR = Path("data")
STATE_FILE = DATA_DIR / "state.json"
PREMARKET_FILE = DATA_DIR / "premarket_cache.json"
PREMARKET_REPORT_FILE = DATA_DIR / "premarket_cache_report.json"
SCRIP_MASTER_FILE = DATA_DIR / "dhan_scrip_master.csv"

DHAN_API_BASE_URL = "https://api.dhan.co/v2"
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DHAN_FEED_URL = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
DHAN_ORDER_UPDATE_URL = "wss://api-order-update.dhan.co"

IST_TIMEZONE = "Asia/Kolkata"
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:30"

FETCH_REQUESTS_PER_SECOND = int(os.getenv("DHAN_FETCH_RPS", "1"))
ORDER_REQUESTS_PER_SECOND = 8
FETCH_RETRY_ATTEMPTS = int(os.getenv("DHAN_FETCH_RETRY_ATTEMPTS", "6"))
FETCH_RETRY_BASE_SECONDS = float(os.getenv("DHAN_FETCH_RETRY_BASE_SECONDS", "5"))
FETCH_RETRY_MAX_SECONDS = float(os.getenv("DHAN_FETCH_RETRY_MAX_SECONDS", "60"))
MAX_MARKET_FEED_CONNECTIONS = 5
MAX_INSTRUMENTS_PER_CONNECTION = 5000
MAX_SUBSCRIBE_BATCH = 100
