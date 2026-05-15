# Phase 3: Gold Layer (Analytics & Star Schema)

## 1. Vai trò
Tầng Gold là tầng cuối cùng, nơi dữ liệu được tổ chức theo cấu trúc tối ưu cho việc truy vấn và báo cáo. Dữ liệu tại đây được mô hình hóa theo dạng **Star Schema** để các công cụ Dashboard (như PowerBI, Tableau, Streamlit) hoặc các nhà phân tích có thể lấy thông tin nhanh nhất.

## 2. Quy trình vận hành (ELT Pattern)
1.  **Modeling:** Chia bảng Silver phẳng thành các bảng Dimension (Player, Team, League, Date) và Fact (Performance).
2.  **Surrogate Key Generation:** Tạo các khóa thay thế (Integer keys) thay cho ID chuỗi để tăng tốc độ truy vấn JOIN.
3.  **Rating Engine (Đang triển khai):** Tính toán điểm số hiệu suất cầu thủ dựa trên các trọng số riêng biệt cho từng vị trí (Tiền đạo, Hậu vệ...).
4.  **View/Table Creation:** Tạo các bảng chính thức trên Cloud Data Warehouse.

## 3. Công nghệ sử dụng
- **MotherDuck (DuckDB Cloud):** Lưu trữ và tính toán dữ liệu trên đám mây.
- **SQL (DuckDB Dialect):** Sử dụng ngôn ngữ truy vấn để biến đổi dữ liệu trực tiếp trên Cloud (ELT).
- **Python:** Điều phối (Orchestration) các câu lệnh SQL.

## 4. Kiến thức cần thiết
- Kiến trúc dữ liệu Star Schema (Fact & Dimension Tables).
- Tối ưu hóa truy vấn SQL nâng cao.
- Tư duy về Business Metrics (KPIs bóng đá).

## 5. Cách chạy (Dự kiến)
```bash
# Xây dựng các bảng Star Schema trên mây
python Phase_3_Gold/build_star_schema.py

# Tính toán điểm số cầu thủ
python Phase_3_Gold/player_rating_engine.py
```
