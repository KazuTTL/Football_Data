# Phase 1: Bronze Layer (Data Ingestion)

## 1. Vai trò
Đây là điểm chạm đầu tiên của hệ thống với thế giới bên ngoài. Vai trò chính là thu thập dữ liệu từ các nguồn khác nhau (Heterogeneous Sources) và lưu trữ chúng ở dạng thô (Raw Data) mà không làm thay đổi nội dung.

## 2. Quy trình vận hành
1.  **API Extraction:** Gửi các yêu cầu không đồng bộ (Asynchronous) tới Sofascore API để lấy dữ liệu cầu thủ và giải đấu.
2.  **Bulk Ingestion:** Tự động tải các file dataset lớn từ Kaggle (Transfermarkt) về máy.
3.  **Local/Cloud Storage:** Lưu trữ dữ liệu dưới dạng JSON (từ API) và CSV (từ Kaggle) vào thư mục `local_data_chunks` hoặc đẩy lên AWS S3.

## 3. Công nghệ sử dụng
- **Python (httpx/aiohttp):** Để gọi API không đồng bộ, tối ưu thời gian chờ.
- **Kaggle API:** Để tải dữ liệu tự động.
- **Boto3:** Thư viện làm việc với AWS S3.
- **Dotenv:** Quản lý các thông tin nhạy cảm (API Keys, Token).

## 4. Kiến thức cần thiết
- Hiểu về cấu trúc REST API và giao thức HTTP.
- Lập trình bất đồng bộ (Async/Await) trong Python.
- Cách quản lý file và thư mục trên hệ điều hành.

## 5. Cách chạy

Phase 1 được chia làm 2 nhánh độc lập để lấy dữ liệu từ 2 nguồn:

### Nhánh 1: Sofascore API (Dữ liệu thời gian thực)
Đây là nhánh chính, sử dụng lập trình bất đồng bộ để cào dữ liệu chi tiết:
```bash
python Phase_1_Advanced/api_extraction/main_pipeline_advanced.py
```

### Nhánh 2: Transfermarkt CSV (Dữ liệu thị trường)
Sử dụng Kaggle API để tải các bộ dataset lớn về máy:
```bash
python Phase_1_Advanced/bulk_ingestion/tm_csv_ingestor.py
```
