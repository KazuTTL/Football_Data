import json
from s3_utils import upload_to_s3

def retry_failed_upload():
    fallback_file_path = r"C:\FastAPI\Football\Phase_1\emergency_backup\FALLBACK_sofascore_big5_clean_20260406_0009.json"
    print("1. Đang mở khóa và đọc dữ liệu từ bãi đáp khẩn cấp...")
    # Nạp dữ liệu từ file JSON vào RAM (biến recovered_data lúc này chính là data_dict)
    with open(fallback_file_path, 'r', encoding='utf-8') as f:
        recovered_data = json.load(f)
        
    print("2. Đang gọi người vận chuyển S3...")
    
    upload_to_s3(recovered_data, "sofascore_big5_recovered.json")
    print(" Quy trình phục hồi hoàn tất!")

if __name__ == "__main__":
    retry_failed_upload()
    