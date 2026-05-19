import os
import sys
import logging

# Thiết lập đường dẫn động để tìm db_connection
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
possible_paths = [
    _THIS_DIR,
    os.path.join(_THIS_DIR, "star_schema"),
    os.path.join(os.path.dirname(_THIS_DIR), "star_schema")
]
for p in possible_paths:
    if os.path.exists(p) and p not in sys.path:
        sys.path.append(p)

from db_connection import get_motherduck_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("generate_erd")

def generate_erd_markdown():
    logger.info("=== BẮT ĐẦU KHỞI TẠO BẢN ĐỒ ERD TỪ DWH MOTHERDUCK ===")
    
    try:
        conn = get_motherduck_connection()
    except Exception as e:
        logger.error(f"Không thể kết nối tới MotherDuck: {e}")
        return

    tables = [
        "dim_player",
        "dim_team",
        "dim_position",
        "dim_tournament",
        "dim_season",
        "fact_player_season_stats"
    ]
    
    schema_info = {}
    
    try:
        # 1. Thu thập dữ liệu cấu trúc bảng
        for table in tables:
            logger.info(f"Đang đọc cấu trúc bảng '{table}'...")
            query = f"DESCRIBE {table};"
            df_cols = conn.execute(query).df()
            
            # Lưu cột: tên, kiểu dữ liệu
            columns = []
            for _, row in df_cols.iterrows():
                col_name = row["column_name"]
                col_type = row["column_type"]
                columns.append((col_name, col_type))
            
            schema_info[table] = columns
            
        conn.close()
        logger.info("Đã lấy thành công cấu trúc toàn bộ các bảng trong Star Schema.")
        
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn cấu trúc bảng: {e}")
        conn.close()
        return

    # 2. Xây dựng nội dung file ERD.md với Mermaid.js
    _THIS_FILE = os.path.abspath(__file__)
    star_schema_dir = os.path.dirname(_THIS_FILE)
    erd_md_path = os.path.join(star_schema_dir, "ERD.md")
    
    mermaid_code = "```mermaid\nerDiagram\n"
    
    # Định nghĩa các thực thể (Entities)
    for table, cols in schema_info.items():
        mermaid_code += f"    {table} {{\n"
        for col_name, col_type in cols:
            # Gắn nhãn PK/FK để trực quan
            is_pk = col_name.endswith("_key") and table.startswith("dim_")
            is_fk = col_name.endswith("_key") and table.startswith("fact_")
            
            label = ""
            if is_pk:
                label = "PK"
            elif is_fk:
                label = "FK"
                
            mermaid_code += f"        {col_type} {col_name} {label}\n"
        mermaid_code += "    }\n\n"
        
    # Định nghĩa các mối quan hệ (Relationships) của Star Schema
    mermaid_code += "    %% Star Schema Relationships\n"
    mermaid_code += "    fact_player_season_stats }|--|| dim_player : \"player_key\"\n"
    mermaid_code += "    fact_player_season_stats }|--|| dim_team : \"team_key\"\n"
    mermaid_code += "    fact_player_season_stats }|--|| dim_position : \"position_key\"\n"
    mermaid_code += "    fact_player_season_stats }|--|| dim_tournament : \"tournament_key\"\n"
    mermaid_code += "    fact_player_season_stats }|--|| dim_season : \"season_key\"\n"
    mermaid_code += "```"

    # Xây dựng toàn bộ file Markdown hướng dẫn
    markdown_content = f"""# Sơ đồ Quan hệ Thực thể (ERD) - Football Star Schema DWH

Sơ đồ này mô tả cấu trúc của mô hình **Star Schema** (Sơ đồ hình sao) được thiết lập trong kho dữ liệu **MotherDuck Cloud DWH** (Database: `football_data`).

> [!NOTE]  
> Tài liệu này được tạo tự động bởi công cụ `generate_erd.py` bằng cách trực tiếp truy vấn cấu trúc bảng (schema catalog) trên MotherDuck.

---

## 1. Bản vẽ Trực quan (Mermaid Diagram)

Bạn có thể xem trực tiếp bản vẽ này bằng cách sử dụng công cụ xem trước Markdown của VS Code (Phím tắt `Ctrl + Shift + V`) hoặc copy đoạn mã Mermaid dưới đây vào trang [Mermaid Live Editor](https://mermaid.live).

{mermaid_code}

---

## 2. Chi tiết Cấu trúc Các Bảng (Schema Details)

### 📊 Bảng Fact (Bảng Sự kiện chính)
#### `fact_player_season_stats`
Bảng lưu trữ thông tin hiệu suất tổng hợp của cầu thủ qua từng mùa giải, được liên kết trực tiếp với các chiều qua các khóa Surrogate Key (`*_key`).
* **Các khóa liên kết ngoại (Foreign Keys)**: `player_key`, `team_key`, `position_key`, `tournament_key`, `season_key`.
* **Các chỉ số đo lường (Metrics)**: `goals`, `assists`, `final_scout_score`.

### 🗂️ Các Bảng Dimension (Bảng Chiều thông tin)
1. **`dim_player`**: Chứa thông tin chi tiết về từng cầu thủ, hỗ trợ lịch sử thay đổi thông tin (SCD Type 2).
2. **`dim_team`**: Thông tin về các câu lạc bộ bóng đá.
3. **`dim_position`**: Nhóm vị trí và vị trí chi tiết của cầu thủ.
4. **`dim_tournament`**: Các giải đấu giải vô địch quốc gia.
5. **`dim_season`**: Các mùa giải bóng đá.

---

## 3. Cách Kết Nối DBeaver Hoặc Công Cụ SQL Khác Để Xem ERD Tự Động

Ngoài việc sử dụng sơ đồ Mermaid ở trên, bạn có thể sử dụng các SQL Client chuyên nghiệp như **DBeaver** để tự động kết xuất ERD tương tác cực đẹp:

### Bước 1: Khởi tạo kết nối trong DBeaver
1. Mở **DBeaver**, chọn **Database** -> **New Database Connection**.
2. Chọn Driver là **DuckDB** hoặc **MotherDuck** (nếu có sẵn).
3. Tại ô **Path / Host URL**, nhập:
   ```text
   md:football_data?motherduck_token=YOUR_MOTHERDUCK_TOKEN
   ```
   *(Thay thế `YOUR_MOTHERDUCK_TOKEN` bằng mã token của bạn trong file `.env`)*

### Bước 2: Xem ERD của Star Schema
1. Kết nối thành công, bạn mở rộng thư mục `football_data` -> `main` -> `Tables`.
2. Giữ phím `Ctrl` và click chọn đồng thời cả 6 bảng:
   * `fact_player_season_stats`
   * `dim_player`
   * `dim_team`
   * `dim_position`
   * `dim_tournament`
   * `dim_season`
3. Click chuột phải chọn **View Diagram** (hoặc nhấn phím tắt `F4`). DBeaver sẽ tự động vẽ một sơ đồ thực thể liên kết vô cùng chuyên nghiệp giúp bạn kéo thả trực quan!
"""

    with open(erd_md_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)
        
    logger.info(f"=== ĐÃ TẠO THÀNH CÔNG BẢN ERD MỚI TẠI: {erd_md_path} ===")

if __name__ == "__main__":
    generate_erd_markdown()
