import os
import duckdb
import logging
from dotenv import load_dotenv

logger = logging.getLogger("db_connection")

def get_motherduck_connection(db_name="football_data"):
    """
    Thiết lập kết nối an toàn đến MotherDuck Data Warehouse.
    - Đọc token từ file .env ở Phase_1.
    - Kết nối vào db_name tương ứng.
    """
    _THIS_FILE = os.path.abspath(__file__)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(_THIS_FILE)))
    env_path = os.path.join(base_dir, "Phase_1_Advanced", ".env")
    
    load_dotenv(dotenv_path=env_path)
    
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        logger.error("MOTHERDUCK_TOKEN không tồn tại. Vui lòng kiểm tra file .env!")
        raise ValueError("Thiếu MOTHERDUCK_TOKEN")
        
    logger.info(f"Đang kết nối tới MotherDuck (Database: {db_name})...")
    conn = duckdb.connect(f"md:?motherduck_token={token}")
    conn.execute(f"USE {db_name}")
    
    logger.info("Kết nối thành công!")
    return conn
