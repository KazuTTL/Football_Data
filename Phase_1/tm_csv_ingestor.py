import os
import boto3
from datetime import datetime
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_REGION, S3_BUCKET_NAME

def ingest_csv_simple():
    local_csv_path = r"C:\FastAPI\Football\data\cauthu.csv"
    
    if not os.path.exists(local_csv_path):
        print(f"Lỗi: Không tìm thấy file tại {local_csv_path}")
        return

    # Tên file trên S3
    s3_file_name = f"bronze_zone/market_value_bulk_simple_{datetime.now().strftime('%Y%m%d')}.csv"

    print(f"Đang đẩy file lên S3: {s3_file_name}...")

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=S3_REGION
        )
        
        # Dùng upload_file thay vì put_object để xử lý file lớn tốt hơn
        s3_client.upload_file(local_csv_path, S3_BUCKET_NAME, s3_file_name)
        
        print(" THÀNH CÔNG! Dữ liệu đã lên S3 (Thư mục phẳng).")
        
    except Exception as e:
        print(f" LỖI KHI ĐẨY FILE: {e}")

if __name__ == "__main__":
    ingest_csv_simple()
