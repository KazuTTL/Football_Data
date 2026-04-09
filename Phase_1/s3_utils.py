import boto3
import json
import os
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_REGION, S3_BUCKET_NAME

def upload_to_s3(data_dict, file_name):
    print(f"\n Bắt đầu vận chuyển file '{file_name}' lên AWS S3...")
    
    # Chuẩn bị sẵn dữ liệu JSON
    json_data = json.dumps(data_dict, ensure_ascii=False, indent=4)
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=S3_REGION
        )
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"bronze_zone/{file_name}",
            Body=json_data,
            ContentType='application/json'
        )
        print(f" THÀNH CÔNG! Dữ liệu đã hạ cánh tại S3.")
        
    except Exception as e:
        # NẾU S3 LỖI
        print(f" THẤT BẠI KHI KẾT NỐI S3: {e}")
        print(" Kích hoạt cơ chế Fallback: Đang lưu khẩn cấp xuống máy tính...")
        
        # Tự động tạo thư mục emergency_backup nếu chưa có
        os.makedirs('emergency_backup', exist_ok=True)
        fallback_path = os.path.join('emergency_backup', f"FALLBACK_{file_name}")
        
        # Lưu thẳng file JSON xuống ổ cứng
        with open(fallback_path, 'w', encoding='utf-8') as f:
            f.write(json_data)
            
        print(f" Đã lưu an toàn tại: {fallback_path}. Vui lòng kiểm tra lại cấu hình AWS sau!")