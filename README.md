# Football Data Pipeline Project ⚽📊

Dự án xây dựng hệ thống xử lý dữ liệu bóng đá chuyên nghiệp dựa trên kiến trúc **Medallion (Bronze ➔ Silver ➔ Gold)**. Hệ thống thu thập dữ liệu thô từ hai nguồn (Sofascore API & Transfermarkt CSV), thực hiện chuẩn hóa danh tính, theo dõi biến động lịch sử (SCD Type 2), và đồng bộ trực tiếp với Cloud Data Warehouse **MotherDuck** để xây dựng mô hình **Star Schema** phục vụ cho **Rating Engine** và **Streamlit Moneyball Scout Dashboard**.

---

## 🏛️ 1. Kiến Trúc Dữ Liệu Hệ Thống (Medallion Architecture)

Hệ thống được thiết kế theo mô hình luồng dữ liệu ELT/ETL khép kín từ Bronze tới Gold:

```mermaid
graph TD
    %% Bronze Zone
    subgraph Bronze Zone (Raw Ingestion)
        A1[Sofascore API - Raw Players JSON] -->|cào sâu| B1[(Bronze Local /data/)]
        A2[Transfermarkt - Bulk CSV] -->|tải từ Kaggle| B2[(Bronze Local /data/)]
        A3[Sofascore API - Raw Standings/Top JSON] -->|cào nhanh| B3[(Bronze Local /data_chunks/)]
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
        
        B3 --> C2[process_auxiliary_data.py]
        C2 -->|Lọc trùng & Làm sạch| H2[(Silver standings/top_players.parquet)]
        
        H & H2 -->|silver_to_motherduck.py| I[(MotherDuck Cloud DWH Tables)]
    end

    %% Gold Zone
    subgraph Gold Zone (Analytics & Star Schema)
        I --> J[run_rating_on_silver.py]
        J -->|Rating Engine 4 bước| K[(gold_player_rating / Cloud Table)]
        
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
    
    class B1,B2,B3 bronze;
    class D1,D2,F,H,H2,I silver;
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

## 3. Hướng Dẫn Vận Hành Hệ Thống (How to Run)

Toàn bộ Pipeline được chạy tuần tự theo các bước cụ thể sau đây:

### Bước 0: Chuẩn bị Môi trường & Thiết lập Cấu hình
*   **Lệnh chạy**:
    ```bash
    pip install -r requirements.txt
    ```
*   **Cấu hình biến môi trường**: Tạo tệp `.env` tại thư mục gốc của dự án chứa thông tin:
    ```env
    MOTHERDUCK_TOKEN=your_motherduck_cloud_token_here
    ```

---

### Bước 1: Thu Thập Dữ Liệu Thô (Bronze Ingestion)
Lấy dữ liệu thô nguyên bản từ hai nguồn dữ liệu độc lập: API (Sofascore) và CSV (Transfermarkt).

*   **Lệnh chạy**:
    1. Lấy dữ liệu từ Sofascore API (cào sâu danh sách & thông số cầu thủ):
       ```bash
       python Phase_1_Advanced/api_extraction/main_pipeline_advanced.py
       ```
    2. Lấy dữ liệu Transfermarkt (tải dữ liệu CSV từ Kaggle):
       ```bash
       python Phase_1_Advanced/bulk_ingestion/tm_csv_ingestor.py
       ```
    3. Cào nhanh dữ liệu phụ trợ (Bảng xếp hạng giải đấu & Top Players UCL):
       ```bash
       python Phase_1_Advanced/api_extraction/fetch_standings_only.py
       ```
*   **Đầu vào (Input)**: Gọi API Sofascore trực tiếp trên mạng và bộ tải Kaggle API.
*   **Đầu ra (Output)**:
    *   Tệp JSON thô lưu tại thư mục: `Phase_1_Advanced/local_data_chunks/` hoặc `local_data_chunks_standings/`.
    *   Các tệp CSV thô từ Transfermarkt được tải về: `Phase_1_Advanced/bulk_ingestion/data/`.

---

### Bước 2: Chuẩn Hóa Dữ Liệu Thô & Xử Lý Trùng Lặp (Bronze to Normalized)
Lọc các cột cần thiết, chuẩn hóa tên cầu thủ và trích xuất các trường chỉ số thực tế từ Bronze.

*   **Lệnh chạy**:
    1. Chuẩn hóa cầu thủ:
       ```bash
       python Phase_2/bronze_to_normalized.py
       ```
    2. Chuẩn hóa và lọc trùng dữ liệu phụ trợ (BXH & Top Players):
       ```bash
       python Phase_2/process_auxiliary_data.py
       ```
*   **Đầu vào (Input)**: Dữ liệu thô trong thư mục `local_data_chunks/`.
*   **Đầu ra (Output)**:
    *   `Phase_2/intermediate/sofascore_normalized.parquet` & `transfermarkt_normalized.parquet`
    *   `Phase_2/intermediate/silver_standings.parquet` & `silver_top_players.parquet` (Đã lọc trùng khớp dữ liệu tuyệt đối).

---

### Bước 3: So Khớp Danh Tính Cầu Thủ (Entity Resolution)
Sử dụng thuật toán so khớp chuỗi mờ (Fuzzy Match) để gộp thông tin của hai nguồn dữ liệu dựa trên một ID duy nhất.

*   **Lệnh chạy**:
    ```bash
    python Phase_2/entity_resolution.py
    ```
*   **Đầu vào (Input)**:
    *   Hai tệp Parquet trung gian đã chuẩn hóa từ Bước 2.
    *   Tệp từ điển ID cục bộ: `Phase_2/metadata/master_player_mapping.json`.
*   **Đầu ra (Output)**:
    *   `Phase_2/intermediate/merged_players.parquet` (Dữ liệu đã gộp chung bằng khóa ID duy nhất `internal_player_id`, tự động gán Placeholder ID `PLR_xxxxx` cho cầu thủ mới không có Transfermarkt).

---

### Bước 4: Quản Lý Lịch Sử Biến Động (Silver SCD Type 2)
Kiểm tra chất lượng dữ liệu qua Pandera Schema và áp dụng Slowly Changing Dimension (SCD Type 2) để quản lý lịch sử biến động.

*   **Lệnh chạy**:
    ```bash
    python Phase_2/silver_scd2_loader.py
    ```
*   **Đầu vào (Input)**:
    *   Tệp `merged_players.parquet` từ Bước 3.
    *   Tệp lịch sử hiện tại (nếu có): `Phase_2/silver_zone/players_history.parquet`.
*   **Đầu ra (Output)**:
    *   `Phase_2/silver_zone/players_history.parquet` (Dữ liệu SCD2 hoàn chỉnh với các cột hiệu dụng `is_current`, `valid_from`, `valid_to`).

---

### Bước 5: Đồng Bộ Lên Cloud DWH MotherDuck
Tải toàn bộ dữ liệu Silver Zone sạch sẽ lên Cloud Warehouse MotherDuck.

*   **Lệnh chạy**:
    ```bash
    python Phase_2/silver_to_motherduck.py
    ```
*   **Đầu vào (Input)**: Các tệp Parquet Silver Zone và Parquet phụ trợ.
*   **Đầu ra (Output)**: Đồng bộ 3 bảng trên MotherDuck Cloud DWH:
    *   `silver_players` (Dữ liệu lịch sử cầu thủ)
    *   `silver_standings` (Bảng xếp hạng giải đấu sạch)
    *   `silver_top_players` (Danh sách cầu thủ xuất sắc nhất giải đấu)

---

### Bước 6: Chạy Thuật Tính Chấm Điểm (Gold Rating Engine)
Tính toán điểm số hiệu năng Scout Score của cầu thủ theo công thức Moneyball 4 bước dựa trên dữ liệu thực tế trên Cloud.

*   **Lệnh chạy**:
    ```bash
    python Phase_3_Gold/rating_engine/run_rating_on_silver.py
    ```
*   **Đầu vào (Input)**: Dữ liệu bảng `silver_players` trên Cloud MotherDuck.
*   **Đầu ra (Output)**: Cập nhật trực tiếp lên bảng `gold_player_rating` trên Cloud MotherDuck DWH.

---

### Bước 7: Xây Dựng Mô Hình Star Schema Cục Bộ
Sinh ra cấu trúc các chiều thông tin (Dimension) và bảng sự kiện (Fact) động dựa trên dữ liệu thật từ Cloud.

*   **Lệnh chạy**:
    ```bash
    python Phase_3_Gold/star_schema/run_all.py
    ```
*   **Đầu vào (Input)**: Bảng `silver_players` và `gold_player_rating` từ Cloud DWH MotherDuck.
*   **Đầu ra (Output)**: Các tệp Parquet của mô hình Star Schema tại thư mục `Phase_3_Gold/star_schema/output/`.

---

### Bước 8: Đồng Bộ Mô Hình Star Schema Lên Cloud DWH
Tải toàn bộ các bảng của mô hình Star Schema từ local lên Cloud MotherDuck DWH.

*   **Lệnh chạy**:
    ```bash
    python Phase_3_Gold/star_schema/push_star_schema_to_motherduck.py
    ```
*   **Đầu vào (Input)**: Các tệp Parquet Star Schema tạo ra từ Bước 7.
*   **Đầu ra (Output)**: Cập nhật 6 bảng phân tích hình sao trực tiếp trên MotherDuck: `fact_player_season_stats`, `dim_player`, `dim_team`, `dim_position`, `dim_tournament`, `dim_season`.

---

### Bước 9: Trải Nghiệm Streamlit Scout Dashboard
Khởi chạy ứng dụng Web để trinh sát và so sánh trực quan hiệu quả Moneyball của các cầu thủ.

*   **Lệnh chạy**:
    ```bash
    cd Phase_4
    streamlit run app.py
    ```
*   **Đầu vào (Input)**: Truy vấn dữ liệu phân tích trực tiếp từ các bảng Star Schema, `silver_standings` và `silver_top_players` trên MotherDuck Cloud DWH.
*   **Đầu ra (Output)**: Giao diện web được mở tại địa chỉ local `http://localhost:8501`.

---

*Dự án được triển khai và phát triển bởi: Tien Loc*
