# Hướng Dẫn Tự Học Apache Airflow Cho Data Engineer

Tài liệu này giúp bạn làm quen với công cụ lập lịch và điều phối luồng dữ liệu (Workflow Orchestrator) phổ biến nhất hiện nay: Apache Airflow.

---

## 1. Apache Airflow là gì và tại sao phải dùng nó?

Khi dự án của bạn phát triển lớn hơn, bạn sẽ có rất nhiều tệp tin Python cần chạy theo thứ tự:
* Cào dữ liệu (Crawl) trước.
* Làm sạch và chuẩn hóa dữ liệu (Normalize) sau.
* Cuối cùng là đồng bộ và phân tích dữ liệu (DWH / Dashboard).

Nếu dùng công cụ lập lịch Cron Job truyền thống của hệ điều hành:
* Bạn rất khó quản lý sự phụ thuộc (Ví dụ: Nếu bước 1 bị lỗi thì bước 2 vẫn cứ chạy, dẫn đến làm hỏng dữ liệu).
* Khó theo dõi nhật ký hoạt động (logs) và bắt lỗi trên giao diện.
* Không có cơ chế tự động chạy lại (Retry) khi mất kết nối mạng hoặc lỗi API tạm thời.

Airflow ra đời để giải quyết các vấn đề trên bằng cách lập trình các luồng công việc dưới dạng mã nguồn Python (Workflow-as-Code).

---

## 2. Các khái niệm cốt lõi trong Airflow

* **DAG (Directed Acyclic Graph - Đồ thị có hướng không chu trình)**: Đây là khái niệm quan trọng nhất. DAG là tập hợp các công việc (Task) được sắp xếp theo một thứ tự hợp lý. Nó "có hướng" (đi từ tác vụ A -> B -> C) và "không chu trình" (không được phép lặp lại vòng tròn B -> A -> B để tránh vòng lặp vô tận).
* **Task (Tác vụ)**: Là một đơn vị công việc nhỏ nhất trong một DAG (ví dụ: chạy một tệp tin python, thực thi một câu lệnh sql, gửi tin nhắn cảnh báo qua telegram).
* **Operator**: Là định nghĩa về loại công việc mà Task sẽ thực hiện. Một số Operator phổ biến:
  * `BashOperator`: Dùng để thực thi các lệnh bash shell trong terminal.
  * `PythonOperator`: Dùng để gọi một hàm Python viết sẵn.
  * `SimpleHttpOperator`: Thực hiện yêu cầu gọi HTTP API.
* **Scheduler (Bộ lập lịch)**: Đây là trái tim của Airflow. Nó liên tục quét các tệp tin DAG, tính toán xem tác vụ nào đã đủ điều kiện để chạy (dựa trên thời gian lập lịch hoặc sự hoàn thành của các task trước đó) và đẩy vào hàng đợi thực thi.
* **Webserver**: Giao diện Web giúp người dùng theo dõi trực quan trạng thái luồng chạy, bật/tắt DAG, đọc log và khởi chạy lại các tác vụ bị lỗi.
* **Database (Metastore)**: Airflow cần một cơ sở dữ liệu (như SQLite, PostgreSQL) để lưu trữ toàn bộ trạng thái của các DAG, lịch sử chạy, log hoạt động và cấu hình người dùng.

---

## 3. Phân tích tệp tin DAG thực tế của dự án

Tệp tin DAG của chúng ta nằm tại: [football_pipeline_dag.py](file:///c:/FastAPI/Football/dags/football_pipeline_dag.py). Hãy cùng phân tích các khối mã nguồn quan trọng:

### 3.1. Cấu hình mặc định (Default Args)
```python
default_args = {
    'owner': 'tien_loc',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_telegram_alert,
}
```
* **depends_on_past**: Nếu là `False`, phiên chạy hôm nay lỗi thì phiên chạy tiếp theo vẫn được phép khởi động bình thường.
* **retries**: Nếu một task bị lỗi, nó sẽ tự động thử chạy lại tối đa 3 lần.
* **retry_delay**: Khoảng thời gian chờ giữa mỗi lần thử lại là 5 phút (giúp đợi API phục hồi hoặc hết bị giới hạn rate limit).
* **on_failure_callback**: Khi một task bị lỗi sau tất cả các lần thử lại, nó sẽ tự động gọi hàm `send_telegram_alert` để gửi thông báo ngay lập tức về điện thoại của bạn.

### 3.2. Định nghĩa các Task bằng BashOperator
```python
crawl_raw_data = BashOperator(
    task_id='crawl_raw_data',
    bash_command='python /opt/airflow/Phase_1_Advanced/api_extraction/main_pipeline_advanced.py && python /opt/airflow/Phase_1_Advanced/api_extraction/fetch_standings_only.py',
)
```
Ở đây, chúng ta sử dụng `BashOperator` để chạy trực tiếp lệnh terminal ngay trong môi trường container. Đường dẫn `/opt/airflow` chính là thư mục dự án đã được Docker Compose gắn kết sang.

### 3.3. Thiết lập thứ tự chạy (Task Dependency)
Cuối tệp tin DAG, chúng ta liên kết thứ tự thực thi của các tác vụ:
```python
crawl_raw_data >> normalize_and_scd2 >> sync_silver_to_dwh >> run_rating_engine >> rebuild_star_schema_to_dwh
```
Ký tự `>>` khai báo rằng: Tác vụ `crawl_raw_data` phải chạy thành công thì mới đến tác vụ `normalize_and_scd2`, và cứ tiếp tục như vậy cho đến bước cuối cùng. Nếu một bước ở giữa đường bị lỗi, các bước phía sau sẽ chuyển sang trạng thái `upstream_failed` (không chạy) để bảo vệ dữ liệu khỏi bị sai sót.

---

## 4. Giải thích Vòng đời và Trạng thái của Task

Một Task Instance trong Airflow sẽ trải qua các trạng thái chính sau:
* **None / Scheduled**: Task mới được tạo hoặc đã được xếp lịch, đang chờ đến lượt.
* **Queued**: Task đã được scheduler đẩy vào hàng đợi thông tin, chờ worker lấy đi thực thi.
* **Running**: Task đang hoạt động thực tế bên trong container.
* **Success**: Task hoàn thành công việc thành công mà không gặp lỗi (exit code 0).
* **Failed**: Task gặp lỗi nghiêm trọng (exit code khác 0 hoặc lỗi mã nguồn).
* **Up For Retry**: Task bị lỗi, nhưng vẫn còn lượt chạy lại. Nó sẽ tạm nghỉ và chờ hết thời gian delay để thực thi lại.
* **Skipped / Upstream Failed**: Task bị bỏ qua hoặc không thể chạy vì tác vụ trước đó đã bị lỗi.
