# Phase 2 Implementation Plan: Silver Zone & Identity Resolution

Dự án sẽ xây dựng một Data Pipeline chuẩn Enterprise, chuyển hóa dữ liệu thô (Bronze) sang dữ liệu có thể phân tích (Silver) bằng cách áp dụng **Data Quality**, **Entity Resolution** và theo dõi lịch sử với **SCD Type 2**.

## Hướng Tiếp Cận Mới
Dựa trên những thảo luận về Data Inconsistency và Schema Monitoring, cấu trúc Phase 2 sẽ bao gồm:
1. **Data Lineage (Truy vết):** Gắn `updated_at` riêng cho từng nguồn và dùng `suffixes=('_sfs', '_tm')` để truy vết cột sau khi merge.
2. **Entity Resolution (Định danh):** Xây dựng `master_player_mapping.json` lưu giữ kết nối ID. Ưu tiên tra ID trước khi dùng Tên & Ngày sinh.
3. **Data Quality Gate (Kiểm duyệt):** Dùng `Pandera` khóa schema trước khi Load.

---

## Proposed Changes

### [NEW] Thư mục: `Phase_2_Standardization/`

#### [NEW] `01_bronze_to_normalized.py` (Làm sạch & Chuẩn hóa)
- Đọc file JSON (Sofascore) và CSV (Transfermarkt).
- Dùng `unidecode` để chuẩn hóa Tên (làm phẳng chữ Tây Ban Nha/Đức).
- Chuẩn hóa format Ngày Sinh sang chuẩn ISO chung: `YYYY-MM-DD`.
- Đóng dấu thời gian (`sofascore_updated_at`, `tm_updated_at`) để thể hiện nguồn chân lý.

#### [NEW] `02_entity_resolution.py` (Lõi Định Danh Mở Rộng)
- Đọc file `metadata/master_player_mapping.json` (Từ điển ID cục bộ).
- Khớp Cấp 1: Tra ID từ điển.
- Khớp Cấp 2 (Mới): Dùng Composite Key (Ngày Sinh + Đội Bóng) cho cầu thủ không có trong từ điển.
- Cập nhật thêm vào từ điển nếu tìm thấy cầu thủ mới.
- Dùng `pd.merge()` gộp dữ liệu thành bảng phẳng.

#### [NEW] `data_contract.py` (Kiểm duyệt Schema)
- Khai báo cấu trúc Pandera `DataFrameSchema` cho dữ liệu đầu ra của bảng ghép.
- Bắt lỗi nếu API bất ngờ thay tên cột.

#### [NEW] `03_silver_scd2_loader.py` (Theo dõi thời gian - SCD2)
- Triển khai thuật toán Slowly Changing Dimension Type 2 bằng Pandas:
  - So sánh bảng mới nhận với file Parquet của lịch sử (`silver_zone/players_scd2.parquet`).
  - Đóng hiệu lực dòng cũ (`is_current=False`, `valid_to=today`).
  - Mở hiệu lực dòng mới (`is_current=True`, `valid_from=today`).
- Lưu trữ thành Data Warehouse tables.

## User Review Required

> [!IMPORTANT]  
> Các thư viện cần thiết bổ sung: `pip install pandas pyarrow pandera unidecode rapidfuzz`. Việc cài đặt này sẽ được tôi tự động chạy trong lần tới.

> [!CAUTION]
> Để Entity Resolution chạy đúng, tôi sẽ cần xử lý file CSV của The Transfermarkt một chút (Đổi cột Ngày tháng năm sinh trên CSV nếu bị lệch chuẩn). Bạn đồng ý cho tôi thao tác thẳng lên File CSV mẫu chứ?

## Verification Plan
1. **Xác thực Schema:** Chủ động đổi tên một cột đầu vào để xem hệ thống Pandera báo màn hình lỗi Đỏ có dễ hiểu không.
2. **Xác thực Truy vết Lineage:** Mở bảng sau khi chạy, kiểm tra xem có cột `goals_sfs` và `market_value_tm` hay không.
3. **Mô phỏng Thời gian:** Build thủ công file `master_player_mapping.json` coi như đã có 10 cầu thủ, và chạy thử file 02 xem nó tra ID nhanh như thế nào.
