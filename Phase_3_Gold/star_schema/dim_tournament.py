import os
import logging
import pandas as pd
from db_connection import get_motherduck_connection

logger = logging.getLogger("dim_tournament")

# Bảng tra cứu quốc gia cho từng giải đấu (dùng khi suy ra country từ tên giải)
LEAGUE_COUNTRY_MAP = {
    "Premier League":  "England",
    "La Liga":         "Spain",
    "Serie A":         "Italy",
    "Bundesliga":      "Germany",
    "Ligue 1":         "France",
    "Champions League": "Europe",
    "Europa League":   "Europe",
}

# Danh sách fallback nếu cột league_sfs chưa tồn tại trên Silver Cloud
FALLBACK_LEAGUES = [
    "Premier League",
    "La Liga",
    "Serie A",
    "Bundesliga",
    "Ligue 1",
]


def build_dim_tournament(output_dir):
    """
    Tạo bảng Dimension cho các giải đấu.
    - Truy vấn động DISTINCT league_sfs từ bảng silver_players trên MotherDuck.
    - Fallback an toàn: Nếu cột league_sfs chưa tồn tại (Silver cũ),
      sử dụng danh sách 5 giải chuẩn từ FALLBACK_LEAGUES.
    - Suy ra trường 'country' từ bảng tra cứu LEAGUE_COUNTRY_MAP.
    """
    logger.info("Đang tạo dim_tournament (dynamic)...")

    leagues = []
    conn = None
    try:
        conn = get_motherduck_connection()

        # Kiểm tra cột league_sfs có tồn tại trên bảng silver_players không
        columns_result = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'silver_players' AND column_name = 'league_sfs'"
        ).fetchall()

        if columns_result:
            # Truy vấn động: Lấy tất cả giải đấu duy nhất từ Silver Data
            df_leagues = conn.execute(
                "SELECT DISTINCT league_sfs AS name "
                "FROM silver_players "
                "WHERE league_sfs IS NOT NULL AND is_current = True "
                "ORDER BY league_sfs"
            ).df()
            leagues = df_leagues["name"].dropna().unique().tolist()
            logger.info(f"Đọc được {len(leagues)} giải đấu từ MotherDuck: {leagues}")
        else:
            logger.warning(
                "Cột 'league_sfs' chưa tồn tại trong silver_players (Silver cũ). "
                "Sử dụng fallback danh sách 5 giải chuẩn."
            )

    except Exception as e:
        logger.warning(f"Không thể truy vấn MotherDuck: {e}. Dùng fallback.")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Fallback: Dùng danh sách cứng nếu query rỗng hoặc lỗi
    if not leagues:
        leagues = FALLBACK_LEAGUES
        logger.info(f"Đang dùng fallback: {leagues}")

    # Sinh Surrogate Key và suy ra country
    dim_tournament = pd.DataFrame({
        "tournament_key": range(1, len(leagues) + 1),
        "name":           leagues,
        "country":        [LEAGUE_COUNTRY_MAP.get(lg, "Unknown") for lg in leagues],
    })

    out_path = os.path.join(output_dir, "dim_tournament.parquet")
    dim_tournament.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_tournament ({len(dim_tournament)} dòng) và lưu tại: {out_path}")

    return dim_tournament
