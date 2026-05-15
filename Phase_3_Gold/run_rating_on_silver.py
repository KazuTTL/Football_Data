import pandas as pd
import os
import logging
from rating_engine import RatingEngine

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_silver_to_gold_rating():
    # 1. Xác định thư mục gốc của dự án (Football/)
    # __file__ là đường dẫn của file script này, dirname lấy thư mục chứa nó (Phase_3_Gold)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Xây dựng đường dẫn tương đối
    silver_parquet_path = os.path.join(base_dir, "Phase_2", "silver_zone", "players_history.parquet")
    output_dir = os.path.join(base_dir, "Phase_3_Gold", "output", "data")
    output_parquet_path = os.path.join(output_dir, "gold_player_rating.parquet")
    
    # Tạo thư mục output nếu chưa có
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Project Base Directory: {base_dir}")
    logging.info(f"Reading Silver data from: {silver_parquet_path}")
    
    try:
        # Đọc dữ liệu từ Silver
        df = pd.read_parquet(silver_parquet_path)
        
        # Chỉ lấy những bản ghi đang active (is_current = True) nếu dùng SCD2
        if "is_current" in df.columns:
            logging.info("Filtering current records (SCD2)...")
            df = df[df["is_current"] == True].copy()
            
        logging.info(f"Loaded {len(df)} current records.")
        
        # Tiền xử lý các cột để khớp với RatingEngine
        # Giả định: nếu thiếu cột số, ta gán = 0 để engine có thể chạy mà không bị crash
        # (Trong thực tế, bạn sẽ map các cột từ Sofascore/Transfermarkt về đúng tên cột)
        
        # 2. Khởi tạo và chạy Rating Engine
        engine = RatingEngine(min_minutes=900)
        
        # Lấy danh sách các cột cần thiết từ engine để kiểm tra
        metrics_to_p90, metrics_to_scale = engine.get_required_metrics()
        
        # Tạm thời điền 0 cho các cột missing để tránh lỗi
        required_cols = list(set(metrics_to_p90 + metrics_to_scale))
        for col in required_cols:
            if col not in df.columns and f"{col}_p90" not in df.columns:
                # Nếu thiếu cả bản gốc lẫn bản p90, khởi tạo = 0
                df[col] = 0.0
                
        # Cột mặc định nếu thiếu
        if "minutes_played" not in df.columns:
            df["minutes_played"] = 1000 # Mock value
        if "league" not in df.columns:
            df["league"] = "Unknown"
        if "team_rank" not in df.columns:
            df["team_rank"] = 10
            
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
        
        # 4. Lưu kết quả
        logging.info(f"Saving {len(gold_df)} records to Gold Layer...")
        gold_df.to_parquet(output_parquet_path, index=False)
        logging.info(f"Success! File saved to: {output_parquet_path}")
        
    except Exception as e:
        logging.error(f"Error processing data: {e}")

if __name__ == "__main__":
    run_silver_to_gold_rating()
