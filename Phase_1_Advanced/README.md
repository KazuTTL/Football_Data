# Football Data Engineering Pipeline - Phase 1 (Advanced Extraction)

 Mục tiêu của Phase 1 là xây dựng một lớp **Bronze Zone (Data Lake)** thu thập dữ liệu thô từ nhiều nguồn khác nhau với hiệu năng cao, ổn định, và có khả năng phục hồi lỗi.

##  Tính năng nổi bật & Kiến thức Data Engineering áp dụng

1. **Multi-source Extraction (Trích xuất đa nguồn)**
   - Kết hợp việc gọi API nội bộ (Sofascore) thông qua Reverse Engineering.
   - Kết hợp Web Scraping (Transfermarkt) để thu thập dữ liệu định giá (Market Value) và Squad.

2. **Asynchronous Processing (Xử lý bất đồng bộ cấu hình cao)**
   - Sử dụng `aiohttp` và `asyncio` để đẩy nhanh tốc độ cào dữ liệu gấp nhiều lần.
   - Triển khai **Concurrency Control** với `asyncio.Semaphore` để kiểm soát giới hạn Rate Limit (ví dụ: không bao giờ bung quá 5 requests/giây để tránh bị DDoS).

3. **Resilience & Fault Tolerance (Khả năng chịu lỗi)**
   - Triển khai Retry Mechanism bằng thư viện `tenacity`.
   - Nếu API Server từ chối kết nối, hệ thống tự động ngắt quãng (Backoff) và thử lại nhiều lần thay vì sập toàn bộ luồng.

4. **Data Partitioning (Phân vùng ngày tháng chuẩn Hive)**
   - Dữ liệu thô (RAW JSON) tạo ra không bao giờ bị ghi đè.
   - Được ánh xạ xuống hệ thống file theo chuẩn chia nhỏ thời gian `dt=YYYY-MM-DD`. Giúp việc truy vấn sau này trên Data Warehouse (Spark/Athena) cực kỳ tối ưu (Time Travel & Partition Discovery).

5. **S3 Cloud Storage Integration**
   - Viết tính năng đẩy trực tiếp các lô dữ liệu (Chunks) lên **AWS S3** qua thư viện `boto3`.
   - Có cơ chế **Fallback Local**: Nếu lỗi kết nối S3 hoặc chưa điền Key, hệ thống tự động lưu trữ tạm tại ổ cứng nội bộ `local_data_chunks` để không bị mất kết quả.

6. **Master Data Management (Lập bản đồ tự động)**
   - Một File Bootstrapper được viết riêng để thu thập tên hàng trăm đội bóng, rẽ qua Transfermarkt dò tìm URL tương ứng, và lưu trữ dưới dạng một "Quyển từ điển" (`team_mapping.json`). Các Node Web Scraping sau đó chỉ dựa vào bản đồ này để lấy dữ liệu.

## 🛠 Công nghệ sử dụng

- **Ngôn ngữ:** `Python 3.10+`
- **Thư viện Web/API:** `aiohttp` (Async HTTP), `requests`, `BeautifulSoup4` (HTML Parsing)
- **Thư viện Data Eng:** `boto3` (AWS S3), `tenacity` (Retry), `aiofiles` (Async File IO)
- **Bảo mật:** `python-dotenv` (Ẩn giấu API Keys)
- **Kiến trúc dữ liệu:** Medallion Architecture (Bronze Layer)

##  Tổ chức mã nguồn

| File | Vai trò (Bức tranh tổng thể) |
| :--- | :--- |
| `config_adv.py` | Trạm điều khiển trung tâm: Cấu hình Leagues, Constants, Logging, Variables. |
| `api_client_async.py` | Lớp Driver chuyên giao tiếp với API Sofascore. Xử lý Logic Retries. |
| `s3_utils_stream.py` | Lớp Dịch vụ Lưu trữ. Chịu trách nhiệm ghi Partition (chia ngày) và Up S3. |
| `main_pipeline_advanced.py` | **Node 1**: Orchestrator (Kẻ điều phối) luồng API Sofascore. |
| `bootstrap_team_mapping.py`| **Node 2**: Công cụ dò tìm URL tự động. |
| `tm_squad_scraper.py` | **Node 3**: Cỗ máy Web Scraping cho Transfermarkt. |
| `tm_csv_ingestor.py` | **Node 4**: Công cụ Bulk Ingestion cho dữ liệu CSV có sẵn. |

## 🕹 Cách thức chạy (How to run)

Trước tiên, setup môi trường:
```bash
pip install -r requirements.txt
```

*(Hãy chắc chắn bạn đã sao chép/đổi tên file `.env.example` thành `.env` để điền Keys nếu cần up S3).*

Chạy cỗ máy theo thứ tự các Node:

1. **Khởi chạy Hệ thống API (Sofascore)**
   ```bash
   python main_pipeline_advanced.py
   ```
   *(Kết quả sẽ rớt xuống thư mục `local_data_chunks/sofascore/dt=YYYY-MM-DD/`)*

2. **Khởi chạy Tạo Map Đội Bóng (Chỉ cần chạy 1 lần lúc đầu season)**
   ```bash
   python bootstrap_team_mapping.py
   ```
   *(Tạo ra/Cập nhật file `team_mapping.json`)*

3. **Khởi chạy Cỗ Máy Cào Web (Transfermarkt)**
   ```bash
   python tm_squad_scraper.py
   ```
   *(Kết quả rớt xuống `local_data_chunks/transfermarkt/dt=YYYY-MM-DD/`)*

> **Lưu ý Đặc biệt về Anti-Scraping:**
> Transfermarkt thường xuyên bảo vệ web bằng hệ thống sinh trắc học Cloudflare (Lỗi 405 Method Not Allowed hoặc yêu cầu xác minh rùa/human verification). Nếu gặp tình trạng này, một Data Engineer sẽ đối phó ở Phase tiếp theo bằng 2 cách:
> 1. Tích hợp proxy tĩnh giá nhỉnh (BrightData / ScraperAPI).
> 2. Chuyển từ BeautifulSoup qua cào qua Selenium / Playwright với Chrome Ẩn danh (Stealth mode).

##  Bước tiếp theo (Next Steps - Phase 2)
Sau khi kho đạn Bronze Zone đã đầy JSON, hệ thống sẽ được đưa qua Phase 2 (Data Processing Silver Zone) sử dụng **Pandas** hoặc **PySpark** để: Làm phẳng JSON, Merge Dữ liệu giữa API và HTML bằng kỹ thuật *Fuzzy Matching* và đẩy sang Data Warehouse.
