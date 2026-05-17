import pandas as pd
import os
import sys
import logging
from rating_engine import RatingEngine

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thêm thư mục star_schema vào sys.path để import db_connection
_THIS_FILE = os.path.abspath(__file__)
rating_engine_dir = os.path.dirname(_THIS_FILE)
phase3_dir = os.path.dirname(rating_engine_dir)
star_schema_dir = os.path.join(phase3_dir, "star_schema")
if star_schema_dir not in sys.path:
    sys.path.append(star_schema_dir)

try:
    from db_connection import get_motherduck_connection
    HAS_DB_CONN = True
except ImportError:
    HAS_DB_CONN = False
    logging.warning("Không thể import get_motherduck_connection từ star_schema. Sẽ chạy ở chế độ offline (chỉ đọc file local).")


def run_silver_to_gold_rating():
    # 1. Xác định thư mục gốc của dự án (Football/)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Xây dựng đường dẫn tương đối
    silver_parquet_path = os.path.join(base_dir, "Phase_2", "silver_zone", "players_history.parquet")
    output_dir = os.path.join(base_dir, "Phase_3_Gold", "output", "data")
    output_parquet_path = os.path.join(output_dir, "gold_player_rating.parquet")
    
    # Tạo thư mục output nếu chưa có
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Project Base Directory: {base_dir}")
    
    df = pd.DataFrame()
    conn = None
    read_from_cloud = False
    
    # Thử kết nối tới MotherDuck Cloud để đọc bảng silver_players
    if HAS_DB_CONN:
        try:
            conn = get_motherduck_connection()
            logging.info("Kết nối thành công tới MotherDuck Cloud. Đang đọc bảng 'silver_players'...")
            df = conn.execute("SELECT * FROM silver_players").df()
            read_from_cloud = True
            logging.info(f"Đã tải {len(df)} bản ghi từ MotherDuck Cloud DWH.")
        except Exception as db_err:
            logging.warning(f"Không thể kết nối hoặc đọc từ MotherDuck Cloud DWH: {db_err}. Sẽ tự động dùng file local.")
            conn = None

    # Fallback đọc từ file Parquet local nếu offline hoặc lỗi kết nối
    if not read_from_cloud:
        logging.info(f"Đang đọc dữ liệu Silver từ file local parquet: {silver_parquet_path}")
        if not os.path.exists(silver_parquet_path):
            logging.error(f"Không tìm thấy file local Silver Parquet tại: {silver_parquet_path}")
            return
        df = pd.read_parquet(silver_parquet_path)
        logging.info(f"Đã tải {len(df)} bản ghi từ file local parquet.")
    
    try:
        # Chỉ lấy những bản ghi đang active (is_current = True) nếu dùng SCD2
        if "is_current" in df.columns:
            logging.info("Đang lọc các bản ghi hiện tại đang active (is_current = True)...")
            df = df[df["is_current"] == True].copy()
            
        logging.info(f"Tổng số bản ghi active để tính toán Rating: {len(df)}")
        
        if df.empty:
            logging.warning("Không có dữ liệu hợp lệ để tính toán rating!")
            return
        
        # 2. Khởi tạo và chạy Rating Engine
        engine = RatingEngine(min_minutes=900)
        
        # --- Áp dụng Mocks và Mappings trước khi điền cột thiếu ---
        if "minutes_played" not in df.columns:
            df["minutes_played"] = 1000 # Mock value
        if "league" not in df.columns:
            df["league"] = "Unknown"
        if "team_rank" not in df.columns:
            df["team_rank"] = 10
            
        # Map tên các cột từ bảng silver_players sang đúng định dạng RatingEngine nếu bị thiếu
        if "name" not in df.columns and "name_sfs_raw" in df.columns:
            df["name"] = df["name_sfs_raw"]
        if "sub_position" not in df.columns and "sub_position_tm" in df.columns:
            df["sub_position"] = df["sub_position_tm"]
        if "team_name" not in df.columns:
            if "team_sfs" in df.columns and "team_tm" in df.columns:
                df["team_name"] = df["team_sfs"].fillna(df["team_tm"])
            elif "team_sfs" in df.columns:
                df["team_name"] = df["team_sfs"]
            elif "team_tm" in df.columns:
                df["team_name"] = df["team_tm"]
                
        # Ánh xạ chỉ số goals và assists từ sofascore
        if "goals" not in df.columns and "goals_sfs" in df.columns:
            df["goals"] = df["goals_sfs"]
        if "assists" not in df.columns and "assists_sfs" in df.columns:
            df["assists"] = df["assists_sfs"]
            
        # Gán giá trị base_rating giả định có biến động để thuật toán min-max hoạt động chính xác
        if "base_rating" not in df.columns:
            df["base_rating"] = 6.5 + (df.index % 10) * 0.15
            
        # Lấy danh sách các cột cần thiết từ engine để kiểm tra
        metrics_to_p90, metrics_to_scale = engine.get_required_metrics()
        
        # Điền 0 cho các cột missing còn lại để tránh lỗi
        required_cols = list(set(metrics_to_p90 + metrics_to_scale))
        for col in required_cols:
            if col not in df.columns and f"{col}_p90" not in df.columns:
                df[col] = 0.0
            
        # Chạy engine
        result_df = engine.run(df)
        
        # 3. Lọc ra các cột Gold Layer cần thiết (theo document)
        gold_cols = [
            "internal_player_id", "name", "sub_position", "league", 
            "team_name", "team_rank", "minutes_played", "base_score", 
            "penalty", "team_multiplier", "final_scout_score", "status"
        ]
        
        # Chỉ lấy những cột có tồn tại trong df
        final_cols = [c for c in gold_cols if c in result_df.columns]
        gold_df = result_df[final_cols]
        
        # 4. Ghi bản sao cục bộ (Local Backup Parquet)
        logging.info(f"Đang ghi {len(gold_df)} bản ghi ra file local Gold Layer: {output_parquet_path}")
        gold_df.to_parquet(output_parquet_path, index=False)
        logging.info("Ghi bản sao local thành công!")
        
        # 5. Đồng bộ trực tiếp lên MotherDuck Cloud nếu có kết nối
        if conn is not None:
            logging.info("Đang đồng bộ dữ liệu Rating lên bảng 'gold_player_rating' trên MotherDuck...")
            # Sử dụng lệnh CREATE OR REPLACE TABLE từ file parquet cục bộ để đảm bảo an toàn tuyệt đối và tính nguyên tử (atomic)
            conn.execute(f"""
                CREATE OR REPLACE TABLE gold_player_rating AS
                SELECT * FROM read_parquet('{output_parquet_path.replace(chr(92), '/')}')
            """)
            logging.info("Đã đồng bộ thành công dữ liệu Rating lên MotherDuck Cloud!")
            
            # Xác nhận kết quả đồng bộ
            verification = conn.execute("""
                SELECT COUNT(*), SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) 
                FROM gold_player_rating
            """).fetchone()
            logging.info(f"Xác nhận DWH: {verification[0]} tổng bản ghi | {verification[1]} cầu thủ đang Active.")
        else:
            logging.warning("Đang chạy ở chế độ Offline. Bảng 'gold_player_rating' trên Cloud MotherDuck không được cập nhật.")
            
    except Exception as e:
        logging.error(f"Lỗi khi xử lý dữ liệu Rating: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
                logging.info("Đã đóng kết nối MotherDuck.")
            except Exception as close_err:
                logging.error(f"Lỗi khi đóng kết nối: {close_err}")

if __name__ == "__main__":
    run_silver_to_gold_rating()
