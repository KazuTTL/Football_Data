# Football Data Pipeline Project ⚽📊

Dự án xây dựng hệ thống xử lý dữ liệu bóng đá chuyên nghiệp theo kiến trúc **Medallion (Bronze -> Silver -> Gold)**, kết hợp dữ liệu từ Sofascore API và Transfermarkt (Kaggle) để xây dựng kho dữ liệu (Data Warehouse) trên Cloud MotherDuck.

## 🌟 Tính năng nổi bật
- **Multi-Source Ingestion:** Tự động thu thập dữ liệu từ cả API (Sofascore) và file CSV lớn (Transfermarkt).
- **Entity Resolution:** Sử dụng Fuzzy Matching (`rapidfuzz`) để khớp nối danh tính cầu thủ giữa các nguồn dữ liệu khác nhau.
- **SCD Type 2:** Theo dõi lịch sử thay đổi của cầu thủ (giá trị chuyển nhượng, chỉ số...) theo thời gian.
- **Data Contract:** Kiểm soát chất lượng dữ liệu đầu vào bằng `Pandera`.
- **Cloud Integration:** Đồng bộ dữ liệu sạch lên MotherDuck Cloud Data Warehouse.
- **Modern Tech Stack:** Python, Pandas, DuckDB, MotherDuck, Pandera.

---

## 📂 Cấu trúc thư mục

```text
Football/
├── Phase_1_Advanced/        # Ingestion Layer (Bronze Zone)
│   ├── api_extraction/      # Script gọi API Sofascore
│   ├── bulk_ingestion/      # Script tải dữ liệu Transfermarkt từ Kaggle
│   └── data/                # Lưu trữ file thô (CSV/JSON)
│
├── Phase_2/                 # Processing Layer (Silver Zone)
│   ├── intermediate/        # Dữ liệu tạm thời sau khi chuẩn hóa
│   ├── silver_zone/         # Dữ liệu sạch, đã áp dụng SCD Type 2 (.parquet)
│   ├── metadata/            # Lưu trữ file ánh xạ ID cầu thủ (mapping)
│   ├── logs/                # Nhật ký vận hành hệ thống
│   ├── bronze_readers.py    # Đọc và Join dữ liệu thô
│   ├── bronze_to_normalized.py # Làm sạch và chuẩn hóa văn bản
│   ├── entity_resolution.py # Khớp nối danh tính cầu thủ (Fuzzy Match)
│   ├── data_contract.py     # Định nghĩa Schema và kiểm tra chất lượng
│   ├── silver_scd2_loader.py # Xử lý logic thay đổi dữ liệu (SCD2)
│   ├── silver_to_motherduck.py # Đồng bộ dữ liệu lên Cloud
│   └── logger_config.py     # Cấu hình hệ thống Log tập trung
│
├── Phase_3_Gold/            # Analytics Layer (Gold Zone - Đang phát triển)
│   └── build_star_schema.py # Xây dựng mô hình Star Schema trên Cloud
│
└── requirements.txt         # Danh sách thư viện cần thiết
```

---

## 🛠 Yêu cầu hệ thống
- Python 3.10+
- MotherDuck Account & Token
- Thư viện: `pandas`, `duckdb`, `pandera`, `rapidfuzz`, `python-dotenv`, `pyarrow`

Cài đặt nhanh:
```bash
pip install -r requirements.txt
```

---

## 🚀 Cách vận hành

Dự án được chạy tuần tự theo các bước của Pipeline:

### Bước 1: Thu thập dữ liệu (Phase 1)
Chạy các script trong `Phase_1_Advanced` để tải dữ liệu về thư mục `data/`.

### Bước 2: Xử lý và Làm sạch (Phase 2)
Bạn có thể chạy toàn bộ quy trình Silver Zone bằng lệnh:
```bash
python Phase_2/bronze_to_normalized.py
python Phase_2/entity_resolution.py
python Phase_2/silver_scd2_loader.py
```

### Bước 3: Đồng bộ lên Cloud
```bash
python Phase_2/silver_to_motherduck.py
```

---

## 📈 Lộ trình phát triển (Roadmap)
- [x] Xây dựng tầng Bronze & Silver.
- [x] Triển khai SCD Type 2 và Entity Resolution.
- [x] Đồng bộ MotherDuck Cloud.
- [ ] **Phase 3:** Hoàn thiện Star Schema (Fact/Dim tables).
- [ ] **Phase 4:** Tự động hóa toàn bộ bằng Airflow & Docker.
- [ ] **Phase 5:** Xây dựng Dashboard đánh giá cầu thủ dựa trên điểm số (Rating Engine).

---
*Dự án được thực hiện bởi: T.Lộc*
