import os
import sys
import shutil
from datetime import datetime
import subprocess

# Fix import để tìm module ở thư mục cha (Phải được gọi TRƯỚC khi import s3_utils_stream và config_adv)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import s3_utils_stream
from config_adv import logger

def download_from_kaggle(file_name, download_dir):
    """Sử dụng Kaggle CLI để tự động tải và giải nén (Cần KAGGLE_USERNAME, KAGGLE_KEY trong .env)"""
    logger.info(f"Đang tải {file_name} từ dataset Kaggle...")    
    # Tìm file thực thi kaggle.exe nằm trong thư mục Scripts của Python
    kaggle_exe = os.path.join(os.path.dirname(sys.executable), "Scripts", "kaggle.exe")
    if not os.path.exists(kaggle_exe):
        kaggle_exe = "kaggle" # Fallback nếu gọi qua CMD thường
        
    cmd = [
        kaggle_exe, "datasets", "download", 
        "-d", "davidcariboo/player-scores", 
        "-f", file_name, 
        "--unzip", "--force", 
        "-p", download_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"Lỗi khi tải {file_name} từ Kaggle: {result.stderr}")
        return False
    return True

def ingest_transfermarkt_csv():
    """
    Hàm tự động tải dữ liệu từ Kaggle và đẩy Transfermarkt lên S3/Bronze Zone.
    """
    download_dir = os.path.join(ROOT_DIR, "data")
    os.makedirs(download_dir, exist_ok=True)
    
    target_files = ['clubs.csv', 'players.csv', 'player_valuations.csv']
    
    # Tạo thông tin phân vùng (Partitioning) chung cho mẻ dữ liệu này
    now = datetime.now()
    partition_date = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime('%Y%m%d_%H%M')
    
    logger.info("Bắt đầu quá trình Mới: Kaggle -> Bulk Ingestion -> Bronze Layer...")
    
    for file_name in target_files:
        # Bước 1: Download tự động thay vì dùng file cứng
        if not download_from_kaggle(file_name, download_dir):
            continue
            
        local_csv_path = os.path.join(download_dir, file_name)
        if not os.path.exists(local_csv_path):
            logger.error(f"Tải về nhưng không tìm thấy file tại: {local_csv_path}")
            continue

        # Bước 2: Chuẩn bị định dạng đẩy S3
        base_name, ext = os.path.splitext(file_name)
        s3_file_name = f"{base_name}_{timestamp_str}{ext}"
        
        # Bước 3: Đẩy lên S3
        success = s3_utils_stream.upload_file_to_s3(
            local_file_path=local_csv_path,
            s3_file_name=s3_file_name,
            source_name="transfermarkt",
            partition_date=partition_date
        )
        
        if success:
            logger.info(f"[S3] Thành công! source=transfermarkt/dt={partition_date}/{s3_file_name}")
        else:
            # Fallback nếu không có S3
            local_dest_dir = os.path.join(ROOT_DIR, "local_data_chunks", "transfermarkt", f"dt={partition_date}")
            os.makedirs(local_dest_dir, exist_ok=True)
            local_dest_path = os.path.join(local_dest_dir, s3_file_name)
            
            shutil.copy2(local_csv_path, local_dest_path)
            logger.info(f"[Local Fallback] Đã sao lưu CSV vào: {local_dest_path}")

    logger.info("Hoàn tất Ingest dữ liệu Transfermarkt từ Kaggle!")

if __name__ == "__main__":
    ingest_transfermarkt_csv()
