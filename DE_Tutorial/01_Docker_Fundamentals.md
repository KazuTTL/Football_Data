# Hướng Dẫn Tự Học Docker Cho Data Engineer

Tài liệu này giúp bạn hiểu rõ các khái niệm cốt lõi về Docker, cách thiết lập Dockerfile và cách quản lý cụm dịch vụ thông qua Docker Compose trong dự án Football Analytics.

---

## 1. Docker là gì và tại sao Data Engineer cần dùng?

Trước đây, khi bạn muốn chạy mã nguồn Python ở các máy tính khác nhau, bạn phải:
* Cài đặt đúng phiên bản Python.
* Cài đặt đúng các thư viện phụ thuộc (requirements.txt).
* Cấu hình các biến môi trường hệ thống.

Điều này dẫn đến lỗi kinh điển: "Mã nguồn chạy tốt trên máy của tôi nhưng lỗi trên máy người khác".

Docker giải quyết vấn đề này bằng cách đóng gói toàn bộ mã nguồn, môi trường Python, và các thư viện hệ thống vào một đơn vị gọi là **Container** (bộ chứa). Container có thể chạy đồng nhất trên mọi hệ điều hành (Windows, Linux, macOS) mà không cần quan tâm máy host đang cài đặt những gì.

---

## 2. Phân biệt Image và Container

* **Image (Ảnh ảo)**: Giống như một bản thiết kế hoặc tệp tin cài đặt phần mềm. Nó là một tệp tin tĩnh lưu trữ toàn bộ cấu hình môi trường được dựng sẵn (hệ điều hành, biến môi trường, tệp tin thư viện). Bạn không thể thay đổi trực tiếp một Image đang hoạt động.
* **Container (Bộ chứa)**: Là một thể hiện đang chạy (an instance) của Image. Nó giống như việc bạn cài đặt thành công và đang khởi chạy ứng dụng từ tệp tin cài đặt. Bạn có thể bật, tắt, xóa hoặc thay đổi dữ liệu trong Container. Khi container bị xóa, mọi thay đổi bên trong nó cũng biến mất, trừ khi bạn sử dụng Volume để lưu trữ dữ liệu ra ngoài.

---

## 3. Giải thích chi tiết Dockerfile trong dự án

Dockerfile là tệp tin chỉ dẫn từng bước để Docker tự động xây dựng (build) một Image.

### Dockerfile (Dùng cho Streamlit Dashboard)
Tệp tin [Dockerfile](file:///c:/FastAPI/Football/Dockerfile) hiện tại trong dự án:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "Phase_4/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

* **FROM python:3.11-slim**: Khai báo Image nền móng là Linux Debian rút gọn đã cài đặt sẵn Python 3.11. Phiên bản `slim` giúp Image có kích thước nhỏ gọn.
* **WORKDIR /app**: Thiết lập thư mục làm việc mặc định bên trong container là `/app`. Các câu lệnh tiếp theo sẽ được thực thi tại thư mục này.
* **RUN apt-get...**: Cài đặt các công cụ cần thiết để biên dịch mã Python nếu có. Sau đó xóa bộ nhớ đệm hệ thống để tiết kiệm dung lượng.
* **COPY requirements.txt .**: Sao chép tệp danh sách thư viện từ máy host vào container.
* **RUN pip install...**: Thực thi lệnh cài đặt các thư viện Python bên trong môi trường container.
* **COPY . .**: Sao chép toàn bộ mã nguồn hiện tại từ máy host vào thư mục `/app` của container.
* **EXPOSE 8501**: Khai báo rằng container sẽ mở cổng `8501` khi hoạt động.
* **CMD [...]**: Câu lệnh mặc định sẽ tự động chạy khi container được bật lên (khởi động ứng dụng Streamlit).

---

## 4. Giải thích Docker Compose và Tệp docker-compose.yml

Docker Compose là công cụ giúp bạn quản lý nhiều container cùng một lúc chỉ bằng một tệp cấu hình duy nhất: `docker-compose.yml`. Thay vì phải gõ các lệnh `docker run` rất dài cho từng container, bạn chỉ cần gõ `docker compose up`.

Tệp tin [docker-compose.yml](file:///c:/FastAPI/Football/docker-compose.yml) gồm các khối chính:

```yaml
version: '3.8'

services:
  dashboard:
    build: .
    container_name: football_dashboard
    ports:
      - "8501:8501"
    volumes:
      - .:/app
    environment:
      - ENVIRONMENT=production
    restart: always
```

* **services**: Khai báo các container (dịch vụ) sẽ hoạt động.
* **dashboard**: Tên của dịch vụ đầu tiên.
* **build: .**: Chỉ ra vị trí chứa Dockerfile để tự động dựng Image. Dấu chấm (.) có nghĩa là dựng từ thư mục hiện tại.
* **ports ("8501:8501")**: Ánh xạ cổng `8501` của máy host vào cổng `8501` của container. Nhờ đó bạn có thể mở trình duyệt trên Windows gõ `http://localhost:8501` để truy cập.
* **volumes (".:/app")**: Gắn kết (mount) thư mục hiện tại trên Windows vào thư mục `/app` của container. Đây là tính năng quan trọng cho lập trình viên. Khi bạn sửa mã nguồn trên Windows, mã nguồn trong container cũng thay đổi ngay lập tức mà không cần dựng lại Image (Hot Reload).
* **environment**: Cấu hình các biến môi trường cần thiết cho container.
* **restart: always**: Tự động khởi động lại container nếu nó bị lỗi đột ngột.

---

## 5. Các lệnh Docker cơ bản bạn cần nằm lòng

* **Xây dựng image từ Dockerfile**:
  ```bash
  docker compose build
  ```
* **Khởi động toàn bộ các dịch vụ ở chế độ chạy ngầm (background)**:
  ```bash
  docker compose up -d
  ```
* **Dừng và xóa sạch các container đang chạy**:
  ```bash
  docker compose down
  ```
* **Kiểm tra các container nào đang hoạt động**:
  ```bash
  docker ps
  ```
* **Xem logs của một container cụ thể**:
  ```bash
  docker logs -f football_dashboard
  ```
* **Truy cập vào terminal bên trong container đang chạy để kiểm tra**:
  ```bash
  docker exec -it football_dashboard bash
  ```
