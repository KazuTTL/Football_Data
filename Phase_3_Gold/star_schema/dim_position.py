import os
import logging
import pandas as pd
from db_connection import get_motherduck_connection

logger = logging.getLogger("dim_position")

def build_dim_position(output_dir):
    """
    Truy vấn bảng silver_players từ DWH MotherDuck,
    trích xuất danh sách các vị trí thi đấu (sub_position_tm), phân nhóm và sinh khoá Surrogate Key.
    """
    conn = get_motherduck_connection()
    logger.info("Đang truy xuất thông tin vị trí thi đấu từ DWH MotherDuck...")
    
    query = """
        SELECT DISTINCT sub_position_tm AS name 
        FROM silver_players 
        WHERE sub_position_tm IS NOT NULL
    """
    
    df_position = conn.execute(query).df()
    conn.close()
    
    unique_positions = df_position["name"].unique()
    logger.info(f"Đã tìm thấy {len(unique_positions)} vị trí duy nhất.")
    
    # Hàm map general position đơn giản
    def map_general(pos):
        pos_lower = str(pos).lower()
        if "forward" in pos_lower or "wing" in pos_lower or "striker" in pos_lower:
            return "Attack"
        elif "midfield" in pos_lower:
            return "Midfield"
        elif "back" in pos_lower or "defender" in pos_lower:
            return "Defender"
        elif "goalkeeper" in pos_lower:
            return "Goalkeeper"
        return "Unknown"

    dim_position = pd.DataFrame({
        "position_key": range(1, len(unique_positions) + 1),
        "name": unique_positions,
        "general_position": [map_general(p) for p in unique_positions],
        "role_weights": "{}" # Nơi chứa JSON cấu hình weights sau này
    })
    
    out_path = os.path.join(output_dir, "dim_position.parquet")
    dim_position.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_position và lưu tại: {out_path}")
    
    return dim_position
