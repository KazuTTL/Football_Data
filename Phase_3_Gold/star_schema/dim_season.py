import os
import logging
import pandas as pd
from datetime import date
from db_connection import get_motherduck_connection

logger = logging.getLogger("dim_season")

# Fallback nếu cột season_sfs chưa tồn tại trên Silver Cloud
FALLBACK_SEASONS = ["2025-2026"]


def _parse_season_dates(season_name: str):
    """
    Suy ra start_date và end_date từ tên mùa giải (VD: '2025-2026').
    - Mùa giải bắt đầu ngày 01/08 của năm đầu.
    - Mùa giải kết thúc ngày 31/05 của năm sau.
    Trả về tuple (start_date, end_date) dạng date object.
    """
    try:
        parts = season_name.split("-")
        if len(parts) == 2:
            start_year = int(parts[0])
            end_year   = int(parts[1])
            return date(start_year, 8, 1), date(end_year, 5, 31)
    except (ValueError, AttributeError):
        pass
    # Fallback: Dùng mùa hiện tại
    current_year = date.today().year
    return date(current_year, 8, 1), date(current_year + 1, 5, 31)


def build_dim_season(output_dir):
    """
    Tạo bảng Dimension cho các mùa giải.
    - Truy vấn động DISTINCT season_sfs từ bảng silver_players trên MotherDuck.
    - Fallback an toàn: Nếu cột season_sfs chưa tồn tại (Silver cũ),
      sử dụng mùa giải mặc định '2025-2026'.
    - Tự động suy ra start_date / end_date từ tên mùa.
    """
    logger.info("Đang tạo dim_season (dynamic)...")

    seasons = []
    conn = None
    try:
        conn = get_motherduck_connection()

        # Kiểm tra cột season_sfs có tồn tại trên bảng silver_players không
        columns_result = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'silver_players' AND column_name = 'season_sfs'"
        ).fetchall()

        if columns_result:
            # Truy vấn động: Lấy tất cả mùa giải duy nhất từ Silver Data
            df_seasons = conn.execute(
                "SELECT DISTINCT season_sfs AS name "
                "FROM silver_players "
                "WHERE season_sfs IS NOT NULL AND is_current = True "
                "ORDER BY season_sfs"
            ).df()
            seasons = df_seasons["name"].dropna().unique().tolist()
            logger.info(f"Đọc được {len(seasons)} mùa giải từ MotherDuck: {seasons}")
        else:
            logger.warning(
                "Cột 'season_sfs' chưa tồn tại trong silver_players (Silver cũ). "
                "Sử dụng fallback mùa giải mặc định."
            )

    except Exception as e:
        logger.warning(f"Không thể truy vấn MotherDuck: {e}. Dùng fallback.")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Fallback: Dùng mùa giải cứng nếu query rỗng hoặc lỗi
    if not seasons:
        seasons = FALLBACK_SEASONS
        logger.info(f"Đang dùng fallback: {seasons}")

    # Sinh Surrogate Key và suy ra start_date / end_date
    rows = []
    for i, season_name in enumerate(seasons, start=1):
        start_dt, end_dt = _parse_season_dates(season_name)
        rows.append({
            "season_key":  i,
            "name":        season_name,
            "start_date":  start_dt,
            "end_date":    end_dt,
        })

    dim_season = pd.DataFrame(rows)

    out_path = os.path.join(output_dir, "dim_season.parquet")
    dim_season.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo dim_season ({len(dim_season)} dòng) và lưu tại: {out_path}")

    return dim_season
