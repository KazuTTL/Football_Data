import sys
import logging
import pandas as pd
from db_connection import get_motherduck_connection

# Thiết lập utf-8 cho Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("query_dwh")

def query_full_star_schema():
    logger.info("Đang kết nối tới MotherDuck để truy vấn toàn bộ Star Schema...")
    
    try:
        conn = get_motherduck_connection()
    except Exception as e:
        logger.error(f"Không thể kết nối tới MotherDuck: {e}")
        return

    query = """
        SELECT 
            p.name AS "Cầu thủ",
            p.sub_position AS "Vị trí cụ thể",
            t.name AS "Đội bóng",
            tour.name AS "Giải đấu",
            s.name AS "Mùa giải",
            f.goals AS "Bàn thắng",
            f.assists AS "Kiến tạo",
            ROUND(f.final_scout_score, 2) AS "Điểm Scout Score"
        FROM fact_player_season_stats f
        LEFT JOIN dim_player p ON f.player_key = p.player_key
        LEFT JOIN dim_team t ON f.team_key = t.team_key
        LEFT JOIN dim_tournament tour ON f.tournament_key = tour.tournament_key
        LEFT JOIN dim_season s ON f.season_key = s.season_key
        ORDER BY f.final_scout_score DESC
        LIMIT 15;
    """
    
    try:
        df = conn.execute(query).df()
        
        print("\n" + "="*80)
        print("TOP 15 CAU THU CO DIEM SCOUT SCORE CAO NHAT (DU LIEU TU STAR SCHEMA DWH)")
        print("="*80)
        # Sử dụng tùy chọn hiển thị rộng của Pandas
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df.to_string(index=False))
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Lỗi khi thực hiện truy vấn DWH: {e}")
    finally:
        conn.close()
        logger.info("Đã đóng kết nối MotherDuck.")

if __name__ == "__main__":
    query_full_star_schema()
