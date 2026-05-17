import os
import logging
import pandas as pd
from db_connection import get_motherduck_connection

logger = logging.getLogger("dim_team")

def build_dim_team(output_dir):
    """
    Truy vấn bảng silver_players từ DWH MotherDuck,
    trích xuất danh sách các đội bóng (team_sfs và team_tm), sinh khoá Surrogate Key.
    """
    conn = get_motherduck_connection()
    logger.info("Đang truy xuất thông tin đội bóng từ DWH MotherDuck...")
    
    query = """
        SELECT DISTINCT team_sfs AS team_name FROM silver_players WHERE team_sfs IS NOT NULL
        UNION
        SELECT DISTINCT team_tm AS team_name FROM silver_players WHERE team_tm IS NOT NULL
    """
    
    df_team = conn.execute(query).df()
    conn.close()
    
    # Lọc bỏ giá trị rỗng nếu có
    df_team = df_team.dropna(subset=["team_name"])
    unique_teams = df_team["team_name"].unique()
    
    logger.info(f"Đã tìm thấy {len(unique_teams)} đội bóng duy nhất.")
    
    dim_team = pd.DataFrame({
        "team_key": range(1, len(unique_teams) + 1),
        "name": unique_teams,
        "short_name": [str(t)[:3].upper() for t in unique_teams], # Giả lập tên ngắn
        "primary_color": "#000000", # Mặc định
        "secondary_color": "#FFFFFF"
    })
    
    out_path = os.path.join(output_dir, "dim_team.parquet")
    dim_team.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_team và lưu tại: {out_path}")
    
    return dim_team
