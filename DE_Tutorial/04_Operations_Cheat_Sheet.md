# Bảng Tra Cứu Lệnh Vận Hành Và Khắc Phục Sự Cố (Operations & Troubleshooting)

Tài liệu này cung cấp các câu lệnh thông dụng nhất để bạn làm chủ hệ thống Docker và Airflow trong công việc hằng ngày của một Data Engineer.

---

## 1. Các câu lệnh Docker Compose hay dùng nhất

### Khởi động tất cả các dịch vụ ở chế độ chạy ngầm
```bash
docker compose up -d dashboard airflow-webserver airflow-scheduler
```

### Dừng hoàn toàn hệ thống và giải phóng tài nguyên
```bash
docker compose down
```

### Xây dựng lại (Rebuild) Image sau khi sửa đổi tệp requirements.txt hoặc Dockerfile
```bash
docker compose build --no-cache
```

### Khởi động lại nhanh các dịch vụ của Airflow
```bash
docker compose restart airflow-webserver airflow-scheduler
```

### Theo dõi logs hoạt động thời gian thực của một container
```bash
docker logs -f football_airflow_scheduler
```

---

## 2. Các câu lệnh Airflow CLI chạy từ Container

Bạn có thể ra lệnh cho Airflow từ terminal của máy Windows bằng cách thực thi lệnh đó thông qua container đang chạy `football_airflow_webserver`.

### Bật (Unpause) hoặc Tắt (Pause) một kịch bản DAG
* **Bật DAG**:
  ```bash
  docker exec -it football_airflow_webserver airflow dags unpause football_etl_pipeline
  ```
* **Tắt DAG**:
  ```bash
  docker exec -it football_airflow_webserver airflow dags pause football_etl_pipeline
  ```

### Kích hoạt chạy thủ công một phiên chạy (Trigger DAG)
```bash
docker exec -it football_airflow_webserver airflow dags trigger football_etl_pipeline
```

### Xem danh sách lịch sử các phiên chạy của DAG
```bash
docker exec -it football_airflow_webserver airflow dags list-runs -d football_etl_pipeline
```

### Kiểm tra trạng thái của tất cả tác vụ trong một phiên chạy cụ thể
```bash
docker exec -it football_airflow_webserver airflow tasks states-for-dag-run football_etl_pipeline TÊN_PHIÊN_CHẠY
# Ví dụ:
docker exec -it football_airflow_webserver airflow tasks states-for-dag-run football_etl_pipeline manual__2026-05-28T16:20:52+00:00
```

---

## 3. Quy trình Debug (Khắc phục sự cố) khi có lỗi xảy ra

Khi có bất kỳ lỗi nào làm dừng tiến trình chạy dữ liệu, hãy bình tĩnh thực hiện theo các bước sau:

### Bước 1: Kiểm tra tin nhắn báo lỗi trên điện thoại
* Nếu bạn đã cấu hình Telegram Alert, bot sẽ tự động nhắn tin cho biết tên Task bị lỗi (ví dụ: `normalize_and_scd2`) kèm theo một đường dẫn trực tiếp đến log lỗi trên Web UI.

### Bước 2: Đọc tệp tin logs của Task bị lỗi
Bạn có thể xem log chi tiết của tác vụ đó trên giao diện Web UI bằng cách nhấp chọn vào ô màu đỏ của task đó, chọn **Log**.
Nếu muốn đọc trực tiếp từ terminal máy host:
* Tìm đường dẫn tệp log trong thư mục `logs/dag_id=football_etl_pipeline/...` trên máy tính của bạn.
* Hoặc chạy lệnh đọc log trực tiếp từ container (thay đổi tên phiên chạy và tên task cho phù hợp):
  ```bash
  docker exec -it football_airflow_webserver cat /opt/airflow/logs/dag_id=football_etl_pipeline/run_id=TÊN_PHIÊN_CHẠY/task_id=TÊN_TASK/attempt=1.log
  ```

### Bước 3: Đưa cơ sở dữ liệu Airflow về trạng thái sạch ban đầu (Reset Database)
Trong trường hợp phát sinh các lỗi rối loạn logic hoặc lỗi ghi dữ liệu đặc biệt mà bạn muốn làm sạch toàn bộ lịch sử chạy để làm lại từ đầu:
1. Dừng hoàn toàn hệ thống:
   ```bash
   docker compose down
   ```
2. Xóa Named Volume lưu trữ cơ sở dữ liệu SQLite của Airflow (tên volume sẽ có tiền tố là tên thư mục dự án):
   ```bash
   docker volume rm football_airflow-db
   ```
3. Khởi động lại hệ thống, Airflow sẽ tự khởi tạo một tệp tin cơ sở dữ liệu mới tinh và tự tạo lại tài khoản quản trị:
   ```bash
   docker compose up -d
   ```
