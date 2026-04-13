import os
import sys
import shutil
from datetime import datetime

# Fix import để tìm module ở thư mục cha
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import s3_utils_stream
from config_adv import logger

def ingest_transfermarkt_csv():
    """
    Hàm đẩy file CSV Transfermarkt lên S3/Bronze Zone.
    Nếu chưa cài S3, tự động sao lưu vào thư mục local_data_chunks theo chuẩn partitioning.
    """
    local_csv_path = r"C:\FastAPI\Football\data\cauthu.csv"
    
    # 1. Kiểm tra file tồn tại
    if not os.path.exists(local_csv_path):
        logger.error(f"Không tìm thấy file CSV tại: {local_csv_path}")
        return

    # 2. Tạo thông tin phân vùng (Partitioning)
    now = datetime.now()
    partition_date = now.strftime("%Y-%m-%d")
    s3_file_name = f"market_value_bulk_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    logger.info("Bắt đầu quá trình Bulk Ingestion (CSV -> Bronze Layer)...")
    
    # 3. Thử đẩy lên S3
    success = s3_utils_stream.upload_file_to_s3(
        local_file_path=local_csv_path,
        s3_file_name=s3_file_name,
        source_name="transfermarkt",
        partition_date=partition_date
    )
    
    if success:
        logger.info(f"[S3] Thanh cong! source=transfermarkt/dt={partition_date}/{s3_file_name}")
    else:
        # 4. Fallback: Nếu không có S3, copy file vào local_data_chunks theo chuẩn dt=...
        local_dest_dir = os.path.join(ROOT_DIR, "local_data_chunks", "transfermarkt", f"dt={partition_date}")
        os.makedirs(local_dest_dir, exist_ok=True)
        local_dest_path = os.path.join(local_dest_dir, s3_file_name)
        
        shutil.copy2(local_csv_path, local_dest_path)
        logger.info(f"[Local Fallback] Da sao luu CSV vao: {local_dest_path}")
        logger.info("Goi y: Dien AWS credentials vao file .env de dung S3 thay vi luu local.")

if __name__ == "__main__":
    ingest_transfermarkt_csv()
