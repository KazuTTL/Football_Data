import os
import logging
import pandas as pd
from datetime import date

logger = logging.getLogger("dim_tournament")

def build_dim_tournament(output_dir):
    """
    Tạo bảng Dimension cho các giải đấu. 
    Trong tương lai có thể query từ DWH nếu thu thập nhiều giải đấu.
    Hiện tại mock tĩnh: Premier League.
    """
    logger.info("Đang tạo dim_tournament...")
    
    dim_tournament = pd.DataFrame({
        "tournament_key": [1],
        "name": ["Premier League"],
        "country": ["England"]
    })
    
    out_path = os.path.join(output_dir, "dim_tournament.parquet")
    dim_tournament.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_tournament và lưu tại: {out_path}")
    
    return dim_tournament
