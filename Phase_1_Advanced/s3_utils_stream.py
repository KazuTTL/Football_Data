import boto3
import json
import os
import aiofiles
from config_adv import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_REGION, S3_BUCKET_NAME, logger

# Lưu bất đồng bộ xuống ổ cứng để không bị nghẽn
async def save_chunk_locally(data_dict, file_name, source_name, partition_date):
    # Tổ chức thư mục local theo chuẩn: local_data_chunks/source/dt=YYYY-MM-DD/
    base_dir = os.path.join('local_data_chunks', source_name, f"dt={partition_date}")
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, file_name)
    
    json_data = json.dumps(data_dict, ensure_ascii=False, indent=2)
    async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
        await f.write(json_data)
        
    logger.info(f"Đã xả nhánh ổ cứng thành công: {file_path}")
    return file_path

# Đẩy file từ ổ cứng lên S3 với kiến trúc Phân vùng (Partitioning)
def upload_file_to_s3(local_file_path, s3_file_name, source_name, partition_date):
    if not AWS_ACCESS_KEY or not S3_BUCKET_NAME:
        logger.warning(f"Chưa có thiết lập Credentials S3. File JSON lưu tại: {local_file_path}")
        return False
        
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=S3_REGION
        )
        
        # Cấu trúc Key chuẩn Hive: bronze_zone/source=xxx/dt=YYYY-MM-DD/filename
        s3_key = f"bronze_zone/source={source_name}/dt={partition_date}/{s3_file_name}"
        
        s3_client.upload_file(
            Filename=local_file_path,
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        logger.info(f"Đẩy S3 Thành công -> {s3_key}")
        return True
    except Exception as e:
        logger.error(f"Lỗi đẩy file lên S3: {str(e)}")
        return False
