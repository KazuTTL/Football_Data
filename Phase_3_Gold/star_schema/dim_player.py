import os
import logging
import pandas as pd
from db_connection import get_motherduck_connection

logger = logging.getLogger("dim_player")

def build_dim_player(output_dir):
    """
    Truy vấn bảng silver_players từ DWH MotherDuck,
    rút trích các thuộc tính Dimension của cầu thủ và sinh khoá Surrogate Key (player_key).
    Chỉ lưu lại file local (Dev/Sandbox strategy).
    """
    conn = get_motherduck_connection()
    logger.info("Đang truy xuất thông tin cầu thủ từ DWH MotherDuck...")
    
    # Truy vấn lấy các thuộc tính định danh cá nhân
    query = """
        SELECT 
            internal_player_id, 
            name_sfs_raw AS name, 
            dob_tm AS dob, 
            COALESCE(
                sub_position_tm, 
                position_tm, 
                CASE position_sfs 
                    WHEN 'G' THEN 'Goalkeeper'
                    WHEN 'D' THEN 'Defender'
                    WHEN 'M' THEN 'Midfielder'
                    WHEN 'F' THEN 'Forward'
                    ELSE 'Unknown'
                END
            ) AS sub_position, 
            market_value_tm AS current_market_value,
            is_current,
            valid_from,
            valid_to
        FROM silver_players
        ORDER BY internal_player_id, valid_from
    """
    
    df_player = conn.execute(query).df()
    conn.close()
    
    logger.info(f"Đã tải {len(df_player)} dòng từ bảng silver_players.")
    
    # Tạo Surrogate Key 'player_key'
    # Index trong df tự động tăng, ta lấy index + 1 làm khóa
    df_player["player_key"] = range(1, len(df_player) + 1)
    
    # Sắp xếp lại cột để khóa chính đứng đầu
    cols = ["player_key", "internal_player_id", "name", "dob", "sub_position", "current_market_value", "is_current", "valid_from", "valid_to"]
    df_player = df_player[cols]
    
    # Ghi ra file
    out_path = os.path.join(output_dir, "dim_player.parquet")
    df_player.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_player và lưu tại: {out_path}")
    
    return df_player
