# Phase 2: Silver Layer (Data Processing & Validation)

## 1. Vai trò
Đây là giai đoạn quan trọng nhất của Pipeline. Tầng Silver có nhiệm vụ biến dữ liệu thô (Raw) thành dữ liệu tin cậy (Trusted Data). Tại đây, các lỗi về dữ liệu được xử lý, danh tính cầu thủ được đồng nhất và lịch sử thay đổi được ghi lại.

## 2. Quy trình vận hành
1.  **Normalization:** Chuẩn hóa văn bản (xóa dấu, viết thường), ép kiểu dữ liệu về dạng chuẩn (Date, Float).
2.  **Entity Resolution (Fuzzy Matching):** So sánh tên cầu thủ từ Sofascore và Transfermarkt để gán cho chúng một ID nội bộ duy nhất (`internal_player_id`).
3.  **Data Contract Validation:** Kiểm tra dữ liệu qua bộ quy tắc `Pandera`. Nếu dữ liệu không đạt chuẩn (ví dụ: thiếu ID, sai kiểu dữ liệu), hệ thống sẽ dừng lại để bảo vệ tầng sau.
4.  **SCD Type 2 (Slowly Changing Dimension):** Logic theo dõi lịch sử. Nếu giá trị cầu thủ thay đổi, hệ thống sẽ đóng bản ghi cũ và tạo bản ghi mới với mốc thời gian `valid_from`, `valid_to`.

## 3. Công nghệ sử dụng
- **Pandas:** Xử lý và biến đổi bảng dữ liệu.
- **RapidFuzz:** Thuật toán so sánh chuỗi mờ để khớp nối tên người.
- **Pandera:** Kiểm soát chất lượng dữ liệu (Data Quality).
- **DuckDB:** Xử lý file Parquet hiệu năng cao và kết nối Cloud.

## 4. Kiến thức cần thiết
- Kỹ thuật xử lý dữ liệu với thư viện Pandas.
- Tư duy về Data Integrity (Toàn vẹn dữ liệu).
- Khái niệm về SCD Type 2 trong Data Warehouse.

## 5. Cách chạy
```bash
# Thực hiện chuẩn hóa
python Phase_2/bronze_to_normalized.py

# Khớp nối danh tính cầu thủ
python Phase_2/entity_resolution.py

# Xử lý SCD Type 2 và lưu vào Silver Zone
python Phase_2/silver_scd2_loader.py

# Đẩy dữ liệu sạch lên MotherDuck
python Phase_2/silver_to_motherduck.py
```
