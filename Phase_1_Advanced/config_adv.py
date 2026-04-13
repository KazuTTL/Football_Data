import os
import logging
from dotenv import load_dotenv

def _find_dotenv():
    """Tự động tìm .env: ưu tiên Phase_1_Advanced, sau đó Phase_1, rồi Football."""
    current = os.path.dirname(os.path.abspath(__file__))  # Phase_1_Advanced/
    parent = os.path.dirname(current)                      # Football/
    sibling = os.path.join(parent, "Phase_1")              # Football/Phase_1/

    for directory in [current, sibling, parent]:
        candidate = os.path.join(directory, '.env')
        if os.path.exists(candidate):
            return candidate
    return None

load_dotenv(_find_dotenv())

# Cấu hình hệ thống Logging chuẩn (Thay thế cho các hàm print)
# Hiển thị timestamp, mức độ (INFO/ERROR) và thông điệp.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("DE_Pipeline")

# Hằng số API
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = "sofascore.p.rapidapi.com"

# Tiêu đề chung cho mọi request
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": RAPIDAPI_HOST
}

# AWS Credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = "us-east-1"

# Danh mục các giải đấu mục tiêu cần thu thập (ID tĩnh)
TARGET_LEAGUES = {
    "Premier League": 17,
    "La Liga": 8,
    "Serie A": 23,
    "Bundesliga": 35,
    "Ligue 1": 34
}

CHAMPIONS_LEAGUE_ID = 7

# === CẤU HÌNH TRANSFERMARKT ===
TM_BASE_URL = "https://www.transfermarkt.com"
TM_SEARCH_URL = "https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche"
TM_HEADERS = {
    # Bắt buộc phải có User-Agent để không bị Cloudflare / Server chặn
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}
