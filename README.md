# Football Data Pipeline Project ⚽📊

Dự án xây dựng hệ thống xử lý dữ liệu bóng đá chuyên nghiệp dựa trên kiến trúc **Medallion (Bronze ➔ Silver ➔ Gold)**. Hệ thống thu thập dữ liệu thô từ hai nguồn (Sofascore API & Transfermarkt CSV), thực hiện chuẩn hóa danh tính, theo dõi biến động lịch sử (SCD Type 2), và đồng bộ trực tiếp với Cloud Data Warehouse **MotherDuck** để xây dựng mô hình **Star Schema** phục vụ cho **Rating Engine** và **Streamlit Moneyball Scout Dashboard**.

---

## 🏛️ 1. Kiến Trúc Dữ Liệu Hệ Thống (Medallion Architecture)

Hệ thống được thiết kế theo mô hình luồng dữ liệu ELT/ETL khép kín từ Bronze tới Gold:

```mermaid
graph TD
    %% Bronze Zone
    subgraph Bronze Zone (Raw Ingestion)
        A1[Sofascore API - Raw JSON] -->|cào tự động| B1[(Bronze Local /data/)]
        A2[Transfermarkt - Bulk CSV] -->|tải từ Kaggle| B2[(Bronze Local /data/)]
    end

    %% Processing Zone
    subgraph Silver Zone (Data Processing & DWH Ingestion)
        B1 & B2 --> C[bronze_to_normalized.py]
        C -->|Chuẩn hóa & Lọc| D1[sofascore_normalized.parquet]
        C -->|Chuẩn hóa & Lọc| D2[transfermarkt_normalized.parquet]
        
        D1 & D2 --> E[entity_resolution.py]
        E -->|Fuzzy Match & Entity Resolution| F[merged_players.parquet]
        
        F --> G[silver_scd2_loader.py]
        G -->|Xử lý SCD Type 2 & Enrich| H[(Silver players_history.parquet)]
        
        H -->|silver_to_motherduck.py| I[(MotherDuck Cloud: silver_players)]
    end

    %% Gold Zone
    subgraph Gold Zone (Analytics & Star Schema)
        I --> J[run_rating_on_silver.py]
        J -->|Rating Engine 4 bước| K[(gold_player_rating.parquet / Cloud Table)]
        
        I & K --> L[star_schema/run_all.py]
        L -->|Dynamic Dimensions & Fact Lookups| M[(Local Star Schema Output)]
        
        M -->|push_star_schema_to_motherduck.py| N[(MotherDuck Cloud DWH Star Schema)]
    end

    %% Visualization
    subgraph Visualization (Presentation Layer)
        N --> O[Streamlit Dashboard App]
    end

    %% Styles
    classDef bronze fill:#f9d5e5,stroke:#333,stroke-width:2px;
    classDef silver fill:#eeeeee,stroke:#333,stroke-width:2px;
    classDef gold fill:#ffeb3b,stroke:#333,stroke-width:2px;
    classDef visual fill:#80deea,stroke:#333,stroke-width:2px;
    
    class B1,B2 bronze;
    class D1,D2,F,H,I silver;
    class K,M,N gold;
    class O visual;
```

---

## 🛠️ 2. Công Nghệ Sử Dụng (Technologies Used)

Hệ thống được xây dựng 100% bằng ngôn ngữ **Python** và tích hợp các thư viện chuyên biệt:

*   **DuckDB & MotherDuck Cloud**: Đóng vai trò làm Database Engine tốc độ cao xử lý phân tích và làm Cloud Data Warehouse lưu trữ tập trung.
*   **Pandas (PyArrow backend)**: Thư viện xử lý và biến đổi ma trận dữ liệu chính trong pipeline.
*   **Pandera**: Thiết lập **Data Contract** cực kỳ nghiêm ngặt nhằm đảm bảo tính toàn vẹn và chất lượng dữ liệu ở các tầng xử lý.
*   **RapidFuzz (Fuzzy Matching)**: Thuật toán so khớp chuỗi thông minh thực hiện liên kết danh tính cầu thủ giữa 2 nguồn dữ liệu không cùng ID chung.
*   **Streamlit**: Framework xây dựng ứng dụng web phân tích hiệu suất và chấm điểm Moneyball của các cầu thủ theo thời gian thực.
*   **Python-dotenv**: Quản lý an toàn các thông tin bảo mật và mã Token kết nối Cloud.

---

## 🚀 3. Hướng Dẫn Vận Hành Hệ Thống (How to Run)

Toàn bộ Pipeline được chạy tuần tự theo các bước cực kỳ rõ ràng sau đây:

### ⚙️ Bước 0: Chuẩn bị Môi trường
Cài đặt toàn bộ thư viện cần thiết:
```bash
pip install -r requirements.txt
```
Tạo file `.env` tại thư mục gốc hoặc trong thư mục `Phase_1_Advanced` chứa Token MotherDuck:
```env
MOTHERDUCK_TOKEN=your_motherduck_cloud_token_here
```

---

### 📥 Bước 1: Thu Thập Dữ Liệu Thô (Bronze Ingestion)
Cào dữ liệu JSON thực tế từ API Sofascore và dữ liệu CSV từ Transfermarkt.
```bash
# Cào dữ liệu API từ Sofascore
python Phase_1_Advanced/api_extraction/main_pipeline_advanced.py
```
*   **Đầu ra (Output)**: 
    *   **AWS S3 Cloud Storage (Chính thức)**: Hệ thống tự động đóng gói dữ liệu và đẩy trực tiếp lên AWS S3 Bucket theo cấu trúc phân vùng tối ưu: `bronze_zone/source={sofascore|transfermarkt}/dt={partition_date}/{filename}`.
    *   **Thư mục Local (Bộ đệm & Dự phòng offline)**: 
        *   Các file JSON lưu tại: `Phase_1_Advanced/local_data_chunks/sofascore/` (VD: `raw_data_PremierLeague_20260413_1431.json`) chứa đầy đủ thống kê, số phút thi đấu, thứ hạng đội bóng, và điểm thô.
        *   Các file CSV Transfermarkt lưu tại: `Phase_1_Advanced/bulk_ingestion/data/` (gồm `players.csv`, `valuations.csv`, `clubs.csv`).
        *   *Lưu ý*: Nếu thiếu credentials cấu hình AWS trong `.env`, hệ thống sẽ kích hoạt chế độ **Fallback Local**, sử dụng các file đệm cục bộ này để tiếp tục xử lý mà không làm gián đoạn pipeline.

---

### 🧹 Bước 2: Chuẩn Hóa và Làm Sạch (Bronze to Normalized)
Lọc các cột cần thiết, chuẩn hóa văn bản tên cầu thủ (bỏ dấu, chuyển chữ thường) và suy ra tên mùa giải thực tế từ thời gian cào.
```bash
python Phase_2/bronze_to_normalized.py
```
*   **Đầu ra (Output)**:
    *   `Phase_2/intermediate/sofascore_normalized.parquet` (Dữ liệu Sofascore đã làm sạch, có thêm cột `league_sfs`, `season_sfs`, `minutes_played_sfs`, `base_rating_sfs`, `team_rank_sfs`).
    *   `Phase_2/intermediate/transfermarkt_normalized.parquet` (Dữ liệu Transfermarkt đã chuẩn hóa thông tin câu lạc bộ và giá trị chuyển nhượng cầu thủ).

---

### 🔗 Bước 3: So Khớp Danh Tính Cầu Thủ (Entity Resolution)
Sử dụng thuật toán Fuzzy Match so sánh tên cầu thủ giữa hai nguồn và ghi nhớ ánh xạ thông qua file từ điển ánh xạ ID cục bộ.
```bash
python Phase_2/entity_resolution.py
```
*   **Đầu ra (Output)**:
    *   `Phase_2/intermediate/merged_players.parquet` (Danh sách 50 cầu thủ đã liên kết thông tin 2 nguồn thành công).
    *   `Phase_2/metadata/player_mapping.json` (Từ điển lưu ánh xạ ID cố định giữa Sofascore và Transfermarkt phục vụ cho các phiên chạy sau chạy nhanh hơn).

---

### ⏳ Bước 4: Xử Lý Lịch Sử Biến Động (Silver SCD Type 2)
Xử lý SCD Type 2 nhằm theo dõi biến động lịch sử của các cầu thủ (như sự thay đổi về giá trị chuyển nhượng, số áo, câu lạc bộ). Đồng thời áp dụng cơ chế làm giàu dữ liệu phi SCD2 để gán các cột metadata mới mà không làm tăng phiên bản lịch sử thừa.
```bash
python Phase_2/silver_scd2_loader.py
```
*   **Đầu ra (Output)**:
    *   `Phase_2/silver_zone/players_history.parquet` (Dữ liệu SCD2 hoàn chỉnh với các cột hiệu dụng `is_current`, `valid_from`, `valid_to`).

---

### ☁️ Bước 5: Đồng Bộ Lên Cloud DWH
Tải toàn bộ dữ liệu Silver Zone sạch sẽ lên Cloud Warehouse MotherDuck.
```bash
python Phase_2/silver_to_motherduck.py
```
*   **Đầu ra (Output)**:
    *   Bảng **`silver_players`** được tạo mới hoặc cập nhật trực tiếp trên Cloud database **`football_data`** của MotherDuck.

---

### 🧮 Bước 6: Chạy Thuật Toán Chấm Điểm (Gold Rating Engine)
Tính toán điểm số hiệu năng Scout Score của 8 nhóm vị trí dựa trên 4 bước: (1) Lọc số phút tối thiểu ➔ (2) Chuẩn hóa chỉ số P90 ➔ (3) Min-Max Scaling theo League ➔ (4) Áp dụng trọng số vị trí, điểm phạt lỗi, và nhân hệ số đội bóng underdog.
```bash
python Phase_3_Gold/rating_engine/run_rating_on_silver.py
```
*   **Đầu ra (Output)**:
    *   `Phase_3_Gold/output/data/gold_player_rating.parquet` (Local backup).
    *   Đồng bộ trực tiếp lên bảng **`gold_player_rating`** trên Cloud MotherDuck.

---

###  Bước 7: Xây Dựng và Đồng Bộ Mô Hình Star Schema
Thiết lập và tạo mô hình Star Schema chuẩn phân tích gồm các bảng Dimensions (`dim_player`, `dim_team`, `dim_position`, `dim_tournament`, `dim_season`) và bảng Fact (`fact_player_season_stats`) một cách **động hoàn toàn** dựa trên dữ liệu thật trên Cloud.
```bash
# Tạo các file Star Schema cục bộ
python Phase_3_Gold/star_schema/run_all.py

# Đồng bộ toàn bộ mô hình lên MotherDuck Cloud DWH
python Phase_3_Gold/star_schema/push_star_schema_to_motherduck.py
```
*   **Đầu ra (Output)**:
    *   Toàn bộ 6 bảng của mô hình Star Schema hoàn tất hiện diện trực tiếp trên MotherDuck Cloud DWH, tất cả các surrogate key đều được JOIN động chuẩn xác 100%.

---

###  Bước 8: Trải Nghiệm Streamlit Scout Dashboard
Chạy giao diện trực quan hóa thông tin chiêu mộ Moneyball:
```bash
streamlit run Phase_4/dashboard_app.py
```
*   **Đầu ra (Output)**: Giao diện web local tại `http://localhost:8501` hiển thị đầy đủ bộ lọc cho 5 giải đấu châu Âu, biểu đồ phân tán Moneyball (Hiệu năng vs Giá trị thị trường) tự nhiên chân thực, danh sách top ngọc thô và tính năng soi chiếu chi tiết chỉ số từng cầu thủ!

---
*Dự án được triển khai và phát triển bởi: Tien Loc*
