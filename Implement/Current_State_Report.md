# Báo cáo Tóm tắt Dự án: Football Data Pipeline (Checkpoint)

*Ngày cập nhật: 07/05/2026*

## 1. Cấu trúc Dự án Hiện tại
Hệ thống hiện đã hoàn thành 100% tầng **Bronze** (Thu thập) và **Silver** (Xử lý sạch), sẵn sàng để tiến lên tầng **Gold** (Phân tích).

```text
Football/
├── Phase_1_Advanced/        # BRONZE: Ingestion (API & Kaggle)
├── Phase_2/                 # SILVER: Processing & Validation
│   ├── bronze_readers.py    # Logic đọc/ghép dữ liệu thô
│   ├── bronze_to_normalized.py # Chuẩn hóa và làm sạch văn bản
│   ├── entity_resolution.py # Khớp nối danh tính (Fuzzy Match)
│   ├── data_contract.py     # Kiểm soát chất lượng (Pandera)
│   ├── silver_scd2_loader.py # Quản lý lịch sử (SCD Type 2)
│   ├── silver_to_motherduck.py # Đồng bộ Cloud
│   └── logger_config.py     # Hệ thống Log chuyên nghiệp
├── Implement/               # Tài liệu thuyết minh dự án
└── README.md                # Tổng quan dự án
```

---

## 2. Các đoạn Code Cốt lõi (Đã chạy thành công)

### 2.1. Logic Khớp nối danh tính & Bổ sung Vị trí (Phase 2)
Chúng ta đã thành công trong việc trích xuất thêm cột `position_tm` và `sub_position_tm` để phục vụ đánh giá cầu thủ sau này.

```python
# Trích xuất từ bronze_readers.py
keep_cols = [
    "player_id", "name", "date_of_birth",
    "position", "sub_position", # Thêm mới để phục vụ Rating Engine
    "club_name", "market_value_eur",
]
```

### 2.2. SCD Type 2 - Trái tim của sự tin cậy
Đoạn code này đảm bảo mỗi khi giá trị cầu thủ hay chỉ số thay đổi, hệ thống sẽ tự động lưu lại lịch sử thay đổi thay vì ghi đè.

```python
# Trích xuất từ silver_scd2_loader.py
def apply_scd2(df_new_records, df_changed_records, df_existing):
    today = str(date.today())
    # Đóng bản ghi cũ
    df_result.loc[mask_close, "is_current"] = False
    df_result.loc[mask_close, "valid_to"] = today
    # Thêm bản ghi mới
    df_inserts = add_scd2_cols(df_new_records, is_current=True, valid_from=today)
```

### 2.3. Data Contract - Chốt chặn chất lượng
Sử dụng Pandera để đảm bảo dữ liệu lên mây luôn sạch 100%.
```python
# Trích xuất từ data_contract.py
MERGED_SCHEMA = DataFrameSchema({
    "internal_player_id": Column(str, nullable=True),
    "market_value_tm": Column(float, nullable=True, coerce=True),
    "position_tm": Column(str, nullable=True),
})
```

---

## 3. Các công việc Sắp tới (To-do List)

### 🚀 Phase 3: Gold Layer (Trọng tâm tiếp theo)
- [ ] **Thiết kế Star Schema:** Triển khai 7 bảng (4 Dim, 1 Fact, 1 Rating, 1 Date).
- [ ] **Xây dựng Rating Engine:** Lập trình công thức tính điểm theo vị trí (FWD, MID, DEF, GK) kết hợp với hệ số BXH đội bóng.
- [ ] **Tạo Surrogate Keys:** Chuyển đổi các ID chuỗi sang ID số nguyên để tối ưu hiệu suất JOIN trên MotherDuck.

### 🤖 Phase 4: Automation & Orchestration
- [ ] **Containerization:** Đóng gói toàn bộ project vào Docker.
- [ ] **Airflow DAGs:** Viết kịch bản tự động chạy Pipeline hàng ngày/hàng tuần.
- [ ] **Monitoring:** Xây dựng Dashboard theo dõi sức khỏe của Pipeline.

---
**Ghi chú:** Dự án hiện đang có 48 cầu thủ được quản lý chặt chẽ trong Silver Zone với đầy đủ thông số vị trí và giá trị chuyển nhượng.
