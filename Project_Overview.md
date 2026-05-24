# Tài liệu Tổng quan Dự án: Football Data Pipeline (Medallion Architecture)

Dự án này là hệ thống xử lý dữ liệu bóng đá chuyên nghiệp từ các nguồn dữ liệu thô (Sofascore API và Transfermarkt CSV), xử lý làm sạch, chuẩn hóa, định danh thực thể (Entity Resolution), quản lý lịch sử biến động (SCD Type 2), lưu trữ dưới dạng Star Schema trên kho dữ liệu đám mây MotherDuck Cloud DWH, và hiển thị kết quả phân tích trinh sát cầu thủ (Moneyball) thông qua Streamlit Dashboard.

---

## 1. Kiến trúc luồng dữ liệu (Medallion Architecture)

Hệ thống hoạt động theo mô hình ELT (Extract - Load - Transform) chia làm 3 phân vùng dữ liệu:

### 1.1. Bronze Zone (Dữ liệu Thô)
*   **Sofascore (API Extraction)**: Sử dụng thư viện lập trình bất đồng bộ (`aiohttp`, `asyncio`) kết hợp với cơ chế kiểm soát rate limit (`Semaphore`) và retry tự động để cào profile chi tiết của cầu thủ thuộc 5 giải đấu hàng đầu châu Âu cùng với cúp C1 (Champions League).
*   **Transfermarkt (Bulk Ingestion)**: Thu thập các file CSV thô về thông tin chuyển nhượng, ngày sinh, câu lạc bộ thông qua Kaggle API.
*   **Phân vùng**: Dữ liệu thô được lưu trữ cục bộ tại thư mục `local_data_chunks/` hoặc đẩy lên AWS S3 theo định dạng phân vùng Hive `dt=YYYY-MM-DD`.

### 1.2. Silver Zone (Dữ liệu Chuẩn hóa & Hợp nhất)
*   **Chuẩn hóa dữ liệu (`bronze_to_normalized.py`)**:
    *   Làm sạch tên cầu thủ (loại bỏ dấu bằng `unidecode`, viết thường, loại bỏ khoảng trắng thừa).
    *   Chuẩn hóa ngày sinh sang dạng `YYYY-MM-DD`.
    *   *Dynamic Mapping*: Ánh xạ tự động toàn bộ hơn 60 chỉ số chuyên môn từ JSON lồng nhau sang các cột phẳng định dạng `snake_case` (có hậu tố `_sfs` hoặc `_tm`).
*   **Định danh cầu thủ (`entity_resolution.py`)**:
    *   Sử dụng giải pháp so khớp 3 cấp độ: Tra từ điển mapping ID có sẵn (`master_player_mapping.json`) -> Tìm kiếm mờ (Fuzzy Match với `RapidFuzz` tỉ lệ trùng khớp >= 85) kết hợp khoá phụ (Ngày sinh + Đội bóng) -> Tạo mã giữ chỗ **Placeholder ID (`PLR_xxxxx`)** cho các cầu thủ trẻ không có dữ liệu khớp trên Transfermarkt để bảo toàn 100% dữ liệu thống kê từ Sofascore.
*   **Kiểm duyệt chất lượng (`data_contract.py`)**:
    *   Định nghĩa Data Contract chặt chẽ bằng thư viện `Pandera` nhằm ngăn chặn dữ liệu lỗi hoặc thay đổi schema đột ngột từ nguồn đi sâu vào kho dữ liệu.
*   **Quản lý lịch sử thay đổi (`silver_scd2_loader.py`)**:
    *   Triển khai Slowly Changing Dimension Type 2 (SCD Type 2) trên file Parquet để theo dõi lịch sử biến động chỉ số (`goals`, `assists`, `market_value`...) qua các trường `is_current`, `valid_from`, `valid_to`.
    *   *Same-Day Upsert*: Khi chạy pipeline nhiều lần trong ngày, hệ thống sẽ tự động ghi đè bản ghi có cùng ngày hiệu lực để tránh phình to phiên bản trong cùng một ngày.
*   **Đồng bộ Cloud DWH (`silver_to_motherduck.py`)**:
    *   Đồng bộ toàn bộ bảng phẳng Silver từ local Parquet lên Cloud MotherDuck dưới tên bảng `silver_players`.

### 1.3. Gold Zone (Star Schema & Rating Engine)
*   **Mô hình hoá dữ liệu (Star Schema)**:
    *   Dữ liệu từ bảng phẳng Silver được chuẩn hóa và phân tách thành mô hình hình sao (Star Schema) gồm 5 bảng Dimension (`dim_player`, `dim_team`, `dim_tournament`, `dim_season`, `dim_position`) và 1 bảng Fact (`fact_player_season_stats`).
    *   Hệ thống sử dụng các Surrogate Key dạng số nguyên tự tăng để tối ưu hóa hiệu năng JOIN truy vấn.
*   **Chấm điểm tuyển trạch (`rating_engine.py`)**:
    *   Áp dụng mô hình Moneyball để tính điểm **Scout Score (0-100)** cho từng cầu thủ qua 4 bước:
        1.  *Lọc ngưỡng*: Chỉ chấm điểm cầu thủ có trên 900 phút thi đấu thực tế.
        2.  *Standardization (Per 90)*: Quy đổi toàn bộ chỉ số thô về hiệu suất trung bình mỗi 90 phút (P90) để đảm bảo tính công bằng.
        3.  *Min-Max Scaling (Per-League)*: Chuẩn hóa thang điểm 0-100 riêng biệt theo từng giải đấu để loại bỏ sự chênh lệch về mặt bằng chung giữa các giải.
        4.  *Weighted Score & Penalties & Underdog Bonus*: Chấm điểm theo trọng số vị trí chuyên môn cụ thể (8 vị trí thi đấu). Trừ điểm cho các lỗi nghiêm trọng và nhân hệ số gánh team **Underdog Bonus** (tối đa x1.285) cho những cầu thủ thuộc đội bóng yếu hơn.

### 1.4. Presentation Layer (Giao diện hiển thị)
*   Ứng dụng Streamlit Dashboard kết nối trực tiếp đến Cloud MotherDuck hiển thị qua 3 Tab:
    *   **Tab 1 - Tổng quan DWH**: Metric card tổng thể, biểu đồ phân bổ CLB/vị trí, bảng xếp hạng các đội bóng và biểu đồ Line Chart lịch sử biến động giá cầu thủ (SCD2).
    *   **Tab 2 - Bảng xếp hạng Scout**: Bộ lọc động chuyên sâu và bảng xếp hạng sắp xếp ưu tiên thông minh (ví dụ: bằng bàn thắng thì ưu tiên người đá ít penalty hơn lên trước).
    *   **Tab 3 - So sánh đối chiếu**: Biểu đồ Radar so sánh 5 góc cạnh hiệu năng và bảng đối chiếu chỉ số trực quan tô màu xanh lá (vượt trội) hoặc màu đỏ (kém hơn).

---

## 2. Cấu trúc thư mục dự án

```text
Football/
│
├── data/                                # Thư mục chứa dữ liệu thô Transfermarkt (CSV)
│   ├── clubs.csv
│   ├── valuations.csv
│   └── players.csv
│
├── local_data_chunks/                   # Thư mục chứa dữ liệu thô Sofascore (JSON phân vùng dt=...)
│   └── sofascore/
│       └── dt=YYYY-MM-DD/
│           └── raw_data_{League}_{Timestamp}.json
│
├── Phase_1_Advanced/                    # Quy trình thu thập dữ liệu (Bronze Zone)
│   ├── api_extraction/
│   │   ├── api_client_async.py          # Client gọi API bất đồng bộ Sofascore
│   │   └── main_pipeline_advanced.py    # Pipeline chính chạy cào dữ liệu thô
│   ├── config_adv.py                    # Cấu hình hằng số, API Keys, AWS S3
│   └── s3_utils_stream.py               # Tiện ích lưu trữ dữ liệu local và đẩy lên S3
│
├── Phase_2/                             # Quy trình chuẩn hóa & làm sạch (Silver Zone)
│   ├── intermediate/                    # Parquet trung gian (sofascore, transfermarkt, merged)
│   ├── logs/                            # File log hoạt động của pipeline tầng 2
│   ├── silver_zone/
│   │   └── players_history.parquet      # Cơ sở dữ liệu Silver local (lịch sử SCD2)
│   ├── bronze_to_normalized.py          # Chuẩn hóa văn bản & dynamic mapping
│   ├── data_contract.py                 # Hợp đồng dữ liệu Pandera
│   ├── entity_resolution.py             # So khớp và định danh cầu thủ (RapidFuzz + Placeholder ID)
│   ├── silver_scd2_loader.py            # Quản lý lịch sử biến động SCD Type 2 & Same-Day Upsert
│   └── silver_to_motherduck.py          # Đồng bộ bảng phẳng lên Cloud MotherDuck
│
├── Phase_3_Gold/                        # Quy trình chấm điểm & DWH (Gold Zone)
│   ├── output/
│   │   └── data/
│   │       └── gold_player_rating.parquet # File chứa điểm Scout Score local
│   ├── rating_engine/
│   │   ├── config/
│   │   │   └── position_weights.py      # Trọng số và cấu hình chấm điểm theo vị trí
│   │   ├── normalizer.py                # Thuật toán P90, Min-max, Underdog bonus
│   │   ├── rating_engine.py             # Lớp xử lý trung tâm của Rating Engine
│   │   └── run_rating_on_silver.py      # Chạy chấm điểm trên Silver và đẩy lên Cloud
│   └── star_schema/
│       ├── db_connection.py             # Quản lý kết nối DuckDB kết nối MotherDuck
│       ├── dim_*.py                     # Các file tạo bảng Dimension tương ứng
│       ├── fact_performance.py          # File tạo bảng Fact hiệu suất
│       ├── run_all.py                   # Chạy tạo toàn bộ Star Schema ở local
│       └── push_star_schema_to_motherduck.py # Đồng bộ Star Schema lên Cloud MotherDuck
│
├── Phase_4/                             # Ứng dụng Streamlit Dashboard
│   └── dashboard_app.py                 # File chạy chính của dashboard Streamlit
│
├── Implement/                           # Thư mục chứa các tài liệu phân tích thiết kế chi tiết
│   ├── Current_State_Report.md
│   ├── Data_Warehouse_Schema.md
│   ├── Galaxy_Schema.md
│   └── ratingEngine.md
│
├── Project_Overview.md                  # File tài liệu tổng quan này
├── README.md                            # README giới thiệu dự án ở root
└── .env                                 # Lưu trữ các biến môi trường (Keys, Tokens...)
```

---

## 3. Công nghệ cốt lõi
*   **Ngôn ngữ chính**: Python 3.x
*   **Thu thập dữ liệu**: `asyncio`, `aiohttp`, `tenacity` (retry logic)
*   **Xử lý dữ liệu bảng**: `Pandas`
*   **Database & DWH**: `DuckDB` (xử lý local nhanh) và `MotherDuck` (Cloud Data Warehouse)
*   **Kiểm tra chất lượng**: `Pandera`
*   **So khớp chuỗi mờ**: `RapidFuzz` (thuật toán C++ hiệu năng cao)
*   **Giao diện**: `Streamlit`, `Plotly` (Express & Graph Objects)

---

## 4. Các lệnh vận hành quan trọng

### Bước 1: Thu thập dữ liệu thô (Bronze)
```bash
python Phase_1_Advanced/api_extraction/main_pipeline_advanced.py
```

### Bước 2: Chuẩn hóa, định danh và nạp lịch sử SCD2 (Silver)
```bash
# Chuẩn hóa dữ liệu thô
python Phase_2/bronze_to_normalized.py

# Định danh và khớp nối cầu thủ
python Phase_2/entity_resolution.py

# Nạp dữ liệu vào bảng lịch sử SCD2 local
python Phase_2/silver_scd2_loader.py

# Đồng bộ bảng phẳng lên Cloud MotherDuck
python Phase_2/silver_to_motherduck.py
```

### Bước 3: Chấm điểm và Mô hình hoá DWH (Gold)
```bash
# Chạy chấm điểm Rating Engine và ghi lên Cloud
python Phase_3_Gold/rating_engine/run_rating_on_silver.py

# Tạo Star Schema local
python Phase_3_Gold/star_schema/run_all.py

# Đồng bộ Star Schema lên Cloud MotherDuck
python Phase_3_Gold/star_schema/push_star_schema_to_motherduck.py
```

### Bước 4: Khởi chạy giao diện phân tích
```bash
streamlit run Phase_4/dashboard_app.py
```
