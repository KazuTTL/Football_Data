# Báo cáo Tóm tắt Dự án: Football Data Pipeline (Trạng thái Hiện tại)

Ngày cập nhật: 29/05/2026

---

## 1. Cấu trúc Dự án Hiện tại

Hệ thống hiện đã hoàn thành 100% tất cả các giai đoạn phát triển từ thu thập (Bronze), làm sạch và đồng bộ (Silver), chấm điểm và lập mô hình kho dữ liệu (Gold), cho tới hiển thị giao diện (Presentation) và lập lịch chạy tự động hóa (Containerization & Orchestration).

```text
Football/
├── Phase_1_Advanced/        # BRONZE: Thu thập dữ liệu API & S3
├── Phase_2/                 # SILVER: Làm sạch, Entity Resolution, SCD2
├── Phase_3_Gold/            # GOLD: Chấm điểm Scout & dựng Star Schema
├── Phase_4/                 # PRESENTATION: Streamlit Dashboard cao cấp
├── dags/                    # ORCHESTRATION: Kịch bản lập lịch Apache Airflow
├── DE_Tutorial/             # TUTORIAL: Tài liệu tự học Docker & Airflow
├── Dockerfile               # Cấu hình container Streamlit
├── Dockerfile.airflow       # Cấu hình container Airflow Webserver/Scheduler
└── docker-compose.yml       # Điều phối toàn bộ các container dịch vụ
```

---

## 2. Các cột mốc công nghệ đã đạt được

### 2.1. Chuẩn hóa và Định danh Cầu thủ (Silver Zone)
*   Áp dụng thuật toán so khớp chuỗi mờ (Fuzzy matching) của `RapidFuzz` kết hợp so khớp khóa phụ (ngày sinh, đội bóng) giúp ánh xạ tự động dữ liệu Sofascore sang Transfermarkt.
*   Cơ chế **Placeholder ID (`PLR_xxxxx`)** giúp bảo toàn dữ liệu thống kê từ Sofascore đối với những cầu thủ trẻ chưa được lập chỉ số trên Transfermarkt.
*   Cấu hình cơ chế lưu trữ lịch sử **SCD Type 2** dạng file Parquet cục bộ và đồng bộ tự động lên Cloud MotherDuck.

### 2.2. Công cụ chấm điểm Scout Score (Gold Zone)
*   Lập trình công thức đánh giá tài năng bóng đá (Scout Score) dựa trên triết lý Moneyball: chuẩn hóa dữ liệu theo hiệu suất mỗi 90 phút (Per 90), loại bỏ lệch giải đấu (Min-max per league), chấm điểm theo trọng số vị trí (8 vị trí thi đấu khác nhau) và cộng điểm gánh đội (Underdog Bonus).
*   Xây dựng mô hình hình sao (Star Schema) phân tách bảng phẳng thành 5 bảng Dimension và 1 bảng Fact, kết nối đồng bộ trực tiếp lên Cloud MotherDuck DWH.

### 2.3. Giao diện Streamlit Premium UI/UX (Presentation Layer)
*   Sử dụng Custom CSS Injection thiết lập cấu hình Card bo tròn góc, viền siêu mảnh, đổ bóng mịn màng và các hiệu ứng hover chuyển động mượt mà.
*   Cấu hình hiển thị theo 3 Tab chuyên sâu: Tab 1 (Tổng quan DWH & Biểu đồ góc phần tư Moneyball), Tab 2 (Bảng xếp hạng Scout thông minh), Tab 3 (So sánh đối đầu Smart Radar Chart tự động phát hiện vị trí thi đấu). Hỗ trợ chuyển đổi độc lập thông số giải quốc nội hoặc Champions League (UCL).

### 2.4. Tự động hóa và Ảo hóa (Docker & Airflow Orchestration)
*   **Dockerization**: Container hóa hoàn toàn dự án. Sử dụng Docker Volume Mount hỗ trợ Hot-Reload cho Dashboard Streamlit.
*   **Airflow Integration**: Tạo tệp kịch bản [football_pipeline_dag.py](file:///c:/FastAPI/Football/dags/football_pipeline_dag.py) chạy tự động luồng ETL Medallion 5 bước vào lúc **07:00 sáng Thứ Hai & Thứ Sáu hàng tuần**.
*   **Giải quyết lỗi khóa đĩa SQLite**: Di chuyển tệp tin `airflow.db` sang một **Docker Named Volume (`airflow-db`)** độc lập chạy trên phân vùng Linux ảo của container, giải quyết triệt để lỗi xung đột ghi đĩa `database is locked` trên hệ điều hành Windows Host.
*   **Hệ thống Telegram Alert**: Kết nối thành công bot cảnh báo lỗi gửi tin nhắn trực tiếp về điện thoại người dùng mỗi khi có task bị lỗi trong quá trình lập lịch.

---

## 3. Tình trạng chạy thực tế hiện tại
*   Cơ sở dữ liệu đám mây trên MotherDuck hoạt động ổn định, lưu giữ dữ liệu của hơn 60 cầu thủ.
*   Hệ thống container Docker đang hoạt động tốt. Streamlit Dashboard hoạt động bình thường trên cổng 8501.
*   Trình quản lý Airflow hoạt động bình thường trên cổng 8080.
*   DAG `football_etl_pipeline` hiện đang ở trạng thái **Paused** (Tạm dừng lập lịch tự động) để tránh hao phí tài nguyên máy tính cá nhân khi kết thúc phiên làm việc.

---

## 4. Các công việc phát triển trong tương lai (Next Steps)

*   **Tính năng Watchlist**: Bổ sung chức năng cho phép tuyển trạch viên thêm cầu thủ vào danh sách theo dõi riêng và lưu lại trạng thái lựa chọn.
*   **Xuất báo cáo PDF**: Xây dựng chức năng xuất file so sánh đối đầu giữa hai cầu thủ hoặc báo cáo bảng xếp hạng Scout thành tệp tin PDF tải trực tiếp từ giao diện Streamlit.
*   **Mở rộng giải đấu**: Cấu hình tăng cường cào thêm dữ liệu từ các giải bóng đá khác ngoài 5 giải đấu lớn của châu Âu.
