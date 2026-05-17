import os
import logging
import pandas as pd
from datetime import date

logger = logging.getLogger("dim_season")

def build_dim_season(output_dir):
    """
    Tạo bảng Dimension cho các mùa giải.
    Hiện tại mock tĩnh cho mùa giải 2025-2026.
    """
    logger.info("Đang tạo dim_season...")
    
    dim_season = pd.DataFrame({
        "season_key": [1],
        "name": ["2025-2026"],
        "start_date": [date(2025, 8, 1)],
        "end_date": [date(2026, 5, 30)]
    })
    
    out_path = os.path.join(output_dir, "dim_season.parquet")
    dim_season.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_season và lưu tại: {out_path}")
    
    return dim_season
