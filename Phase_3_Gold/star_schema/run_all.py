import os
import logging
import sys

# Import các builder
from dim_player import build_dim_player
from dim_team import build_dim_team
from dim_position import build_dim_position
from dim_tournament import build_dim_tournament
from dim_season import build_dim_season
from fact_performance import build_fact_performance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_all")

def main():
    logger.info("=== BẮT ĐẦU QUÁ TRÌNH TẠO STAR SCHEMA TỪ MOTHERDUCK ===")
    
    # Thiết lập thư mục output
    _THIS_FILE = os.path.abspath(__file__)
    output_dir = os.path.join(os.path.dirname(_THIS_FILE), "output")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Thư mục lưu trữ: {output_dir}")
    
    try:
        # Bước 1: Build các bảng Dimension độc lập
        dim_tournament = build_dim_tournament(output_dir)
        dim_season = build_dim_season(output_dir)
        
        # Bước 2: Build các Dimension phụ thuộc DWH
        dim_position = build_dim_position(output_dir)
        dim_team = build_dim_team(output_dir)
        dim_player = build_dim_player(output_dir)
        
        # Bước 3: Build bảng Fact phụ thuộc Dimension & DWH
        fact_performance = build_fact_performance(output_dir, dim_player, dim_team, dim_position)
        
        logger.info("=== QUÁ TRÌNH HOÀN TẤT THÀNH CÔNG ===")
        
    except Exception as e:
        logger.error(f"Lỗi hệ thống: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
