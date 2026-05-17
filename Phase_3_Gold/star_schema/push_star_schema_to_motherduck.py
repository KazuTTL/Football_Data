import os
import sys
import logging
from db_connection import get_motherduck_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("push_to_dwh")

def push_star_schema():
    logger.info("=== BẮT ĐẦU ĐỒNG BỘ STAR SCHEMA LÊN MOTHERDUCK ===")
    
    _THIS_FILE = os.path.abspath(__file__)
    star_schema_dir = os.path.dirname(_THIS_FILE)
    output_dir = os.path.join(star_schema_dir, "output")
    
    if not os.path.exists(output_dir):
        logger.error(f"Thư mục output không tồn tại: {output_dir}")
        return

    # Danh sách các bảng cần đồng bộ
    tables_to_sync = [
        "dim_player",
        "dim_team",
        "dim_position",
        "dim_tournament",
        "dim_season",
        "fact_player_season_stats"
    ]
    
    try:
        conn = get_motherduck_connection()
    except Exception as e:
        logger.error(f"Không thể kết nối tới MotherDuck: {e}")
        return

    try:
        for table_name in tables_to_sync:
            parquet_file = os.path.join(output_dir, f"{table_name}.parquet")
            
            if not os.path.exists(parquet_file):
                logger.warning(f"Không tìm thấy file cho bảng {table_name}: {parquet_file}")
                continue
                
            logger.info(f"Đang đồng bộ bảng '{table_name}' lên DWH...")
            
            # Sử dụng đường dẫn an toàn cho DuckDB trên Windows (thay thế \ bằng /)
            safe_path = parquet_file.replace('\\', '/')
            
            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{safe_path}')
            """
            conn.execute(query)
            
            # Xác nhận đồng bộ
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"-> Đã đồng bộ thành công bảng '{table_name}' với {count} dòng.")
            
        logger.info("=== QUÁ TRÌNH ĐỒNG BỘ HOÀN TẤT THÀNH CÔNG ===")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình đồng bộ: {e}")
    finally:
        conn.close()
        logger.info("Đã đóng kết nối MotherDuck.")

if __name__ == "__main__":
    push_star_schema()
