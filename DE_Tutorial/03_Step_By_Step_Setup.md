# Hướng Dẫn Xây Dựng Hệ Thống Từng Bước (Step-By-Step Setup)

Tài liệu này ghi lại từng bước thiết lập môi trường Docker, cấu hình cho Airflow, và cách giải quyết sự cố phát sinh trong quá trình phát triển hệ thống.

---

## Bước 1: Chuẩn bị danh sách thư viện (requirements.txt)

Trước hết, chúng ta cần liệt kê tất cả các thư viện Python cần dùng cho dự án trong tệp [requirements.txt](file:///c:/FastAPI/Football/requirements.txt):
* `streamlit` cho giao diện người dùng.
* `pandas` để xử lý và biến đổi dữ liệu.
* `duckdb` để truy vấn SQL và lưu trữ phân tích.
* `boto3` để làm việc với AWS S3.
* `python-dotenv` để đọc các tham số bảo mật trong tệp .env.
* *Lưu ý*: Phiên bản `duckdb` phải được giới hạn ở mức `duckdb<=1.5.2` để tương thích hoàn hảo với MotherDuck Cloud.

---

## Bước 2: Viết Dockerfile.airflow cho Airflow

Do phiên bản Airflow mặc định trên Docker Hub không có sẵn các thư viện nghiệp vụ của chúng ta, ta cần tự xây dựng một Image riêng bằng cách dùng [Dockerfile.airflow](file:///c:/FastAPI/Football/Dockerfile.airflow):

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/airflow/db && chown -R airflow:root /home/airflow/db

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### Tại sao cần cấp quyền truy cập mục?
* Mặc định, Airflow chạy dưới quyền user non-root có tên là `airflow`.
* Nếu chúng ta tạo thư mục chứa SQLite DB mà không chuyển quyền sở hữu, container sẽ không thể ghi dữ liệu vào tệp tin và sinh ra lỗi quyền truy cập. Do đó, lệnh `chown -R airflow:root` là bắt buộc phải thực hiện dưới quyền `root` trước khi trả lại quyền hạn cho user `airflow`.

---

## Bước 3: Cấu hình cụm dịch vụ trong docker-compose.yml

Chúng ta khai báo cụm dịch vụ Airflow bên cạnh dịch vụ dashboard sẵn có. Đặc biệt quan trọng là cách chúng ta giải quyết lỗi khóa file SQLite.

### Sự cố và Giải pháp cho lỗi khóa file SQLite (database is locked) trên Windows Host
Khi dùng Windows, hệ thống tệp tin NTFS của Windows khi gắn kết qua Linux (WSL2 hoặc Docker Desktop) không hỗ trợ cơ chế khóa tệp (file locking) đồng bộ của SQLite. Điều này khiến cho cả Webserver và Scheduler khi ghi dữ liệu vào tệp tin `airflow.db` trên ổ đĩa Windows sẽ bị lỗi ngay lập tức.

**Giải pháp**:
Chúng ta khai báo một Named Volume có tên là `airflow-db` nằm hoàn toàn trong phân vùng ổ đĩa Linux ảo của Docker:
```yaml
volumes:
  airflow-db:
    driver: local
```
Và gắn kết (mount) volume này vào thư mục `/home/airflow/db` trong container:
```yaml
services:
  airflow-webserver:
    ...
    volumes:
      - .:/opt/airflow
      - airflow-db:/home/airflow/db
    environment:
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/airflow/db/airflow.db
```
Nhờ vậy, cơ sở dữ liệu SQLite được đọc ghi trực tiếp rất nhanh trên ổ đĩa ảo Linux và không bao giờ bị lỗi khóa file từ phía Windows host.

---

## Bước 4: Thiết lập khởi động tuần tự cho Scheduler

Webserver cần thời gian để chạy lệnh khởi tạo cơ sở dữ liệu ban đầu:
```bash
airflow db init
airflow users create --username admin --password admin ...
```
Nếu Scheduler cũng chạy đồng thời lúc này, cả hai tiến trình sẽ cùng tranh chấp ghi tệp tin SQLite gây lỗi.
Để khắc phục, chúng ta thêm độ trễ cho Scheduler:
```yaml
  airflow-scheduler:
    ...
    entrypoint: bash -c "sleep 20 && airflow scheduler"
```
Độ trễ 20 giây này giúp Webserver luôn có đủ thời gian hoàn thành trước, sau đó Scheduler mới bắt đầu kết nối.

---

## Bước 5: Triển khai tệp tin DAG và kết nối Telegram Bot

1. Tạo bot trên Telegram qua `@BotFather` để lấy mã bảo mật `Token`.
2. Lấy số định danh tài khoản `Chat ID` cá nhân thông qua bot `@userinfobot`.
3. Khai báo hai giá trị này vào tệp tin cấu hình môi trường [.env](file:///c:/FastAPI/Football/Phase_1_Advanced/.env).
4. Viết hàm gửi yêu cầu POST lên Telegram API trong tệp `dags/football_pipeline_dag.py` và gán hàm này vào tham số `on_failure_callback`.
5. Khi hệ thống khởi động, truy cập vào giao diện Web UI để kích hoạt DAG (Unpause) và theo dõi luồng chạy tự động.
