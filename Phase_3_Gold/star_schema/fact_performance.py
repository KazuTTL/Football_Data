import os
import logging
import pandas as pd
from db_connection import get_motherduck_connection

logger = logging.getLogger("fact_performance")

def build_fact_performance(output_dir, dim_player, dim_team, dim_position, dim_tournament, dim_season):
    """
    Tạo bảng Fact:
    - Truy vấn metrics + league_sfs + season_sfs từ bảng silver_players trên DWH.
    - Ghép khóa ngoại (surrogate keys) từ các DataFrame dimension.
    - JOIN động tournament_key và season_key dựa trên league_sfs / season_sfs thực tế.
    - Lấy điểm final_scout_score từ local file gold_player_rating.
    """
    conn = get_motherduck_connection()
    logger.info("Đang truy xuất metrics từ DWH MotherDuck...")

    # Kiểm tra xem các cột mới đã tồn tại trên Silver chưa
    columns_result = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'silver_players' AND column_name IN ('league_sfs', 'season_sfs')"
    ).fetchall()
    existing_new_cols = {row[0] for row in columns_result}

    has_league = "league_sfs" in existing_new_cols
    has_season = "season_sfs" in existing_new_cols

    # Xây dựng danh sách cột cần SELECT động (tương thích cả Silver cũ và mới)
    extra_cols = ""
    if has_league:
        extra_cols += ",\n            league_sfs"
    if has_season:
        extra_cols += ",\n            season_sfs"

    query = f"""
        SELECT
            internal_player_id,
            team_sfs,
            team_tm,
            sub_position_tm,
            goals_sfs,
            assists_sfs,
            is_current{extra_cols}
        FROM silver_players
        WHERE is_current = True
    """

    df_silver = conn.execute(query).df()
    conn.close()
    logger.info(f"Đã tải {len(df_silver)} dòng (is_current=True) từ silver_players.")
    if has_league:
        leagues_found = df_silver["league_sfs"].dropna().unique().tolist()
        logger.info(f"Các giải đấu trong Silver: {leagues_found}")

    # Đọc file Gold Rating từ local
    _THIS_FILE = os.path.abspath(__file__)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(_THIS_FILE)))
    rating_path = os.path.join(base_dir, "Phase_3_Gold", "output", "data", "gold_player_rating.parquet")

    df_rating = pd.DataFrame()
    if os.path.exists(rating_path):
        logger.info(f"Đang đọc dữ liệu Rating từ: {rating_path}")
        df_rating = pd.read_parquet(rating_path)
    else:
        logger.warning(f"Không tìm thấy file Rating tại {rating_path}. Điểm số sẽ được set thành 0.")

    logger.info("Đang ghép (JOIN) Surrogate Keys để tạo bảng Fact...")
    df_fact = df_silver.copy()

    # 1. Join player_key
    df_fact = pd.merge(
        df_fact,
        dim_player[dim_player["is_current"] == True][["internal_player_id", "player_key"]],
        on="internal_player_id", how="left"
    )

    # 2. Join team_key
    df_fact["team_col"] = df_fact["team_sfs"].fillna(df_fact["team_tm"])
    df_fact = pd.merge(df_fact, dim_team, left_on="team_col", right_on="name", how="left")

    # 3. Join position_key
    if "sub_position_tm" in df_fact.columns:
        df_fact = pd.merge(
            df_fact, dim_position,
            left_on="sub_position_tm", right_on="name",
            how="left", suffixes=("", "_pos")
        )
    else:
        df_fact["position_key"] = 1

    # 4. JOIN tournament_key — Động dựa trên league_sfs
    if has_league and not dim_tournament.empty:
        # Tạo lookup: tên giải đấu -> tournament_key
        tour_lookup = dim_tournament.set_index("name")["tournament_key"].to_dict()
        df_fact["tournament_key"] = df_fact["league_sfs"].map(tour_lookup)
        matched = df_fact["tournament_key"].notna().sum()
        unmatched = df_fact["tournament_key"].isna().sum()
        logger.info(f"tournament_key: {matched} khớp / {unmatched} không khớp (fallback = 1)")
        # Fallback: Nếu không khớp (dữ liệu bất thường), dùng key = 1 (giải đầu tiên)
        df_fact["tournament_key"] = df_fact["tournament_key"].fillna(1).astype(int)
    else:
        # Backward-compat: Silver cũ chưa có league_sfs
        logger.warning("Không có dữ liệu league_sfs. Fallback tournament_key = 1.")
        df_fact["tournament_key"] = 1

    # 5. JOIN season_key — Động dựa trên season_sfs
    if has_season and not dim_season.empty:
        # Tạo lookup: tên mùa giải -> season_key
        season_lookup = dim_season.set_index("name")["season_key"].to_dict()
        df_fact["season_key"] = df_fact["season_sfs"].map(season_lookup)
        matched = df_fact["season_key"].notna().sum()
        unmatched = df_fact["season_key"].isna().sum()
        logger.info(f"season_key: {matched} khớp / {unmatched} không khớp (fallback = 1)")
        # Fallback: Nếu không khớp, dùng key = 1 (mùa đầu tiên)
        df_fact["season_key"] = df_fact["season_key"].fillna(1).astype(int)
    else:
        # Backward-compat: Silver cũ chưa có season_sfs
        logger.warning("Không có dữ liệu season_sfs. Fallback season_key = 1.")
        df_fact["season_key"] = 1

    # 6. Join Gold Rating
    if not df_rating.empty and "internal_player_id" in df_rating.columns and "final_scout_score" in df_rating.columns:
        df_fact = pd.merge(
            df_fact,
            df_rating[["internal_player_id", "final_scout_score"]],
            on="internal_player_id", how="left"
        )
    else:
        df_fact["final_scout_score"] = 0.0

    # 7. Lọc các cột chuẩn cho bảng fact
    fact_cols = ["player_key", "team_key", "tournament_key", "season_key", "position_key"]

    # Xử lý các metrics (đổi tên cột _sfs về tên gốc)
    for c in ["goals_sfs", "assists_sfs"]:
        if c in df_fact.columns:
            new_col = c.replace("_sfs", "")
            df_fact[new_col] = df_fact[c]
            fact_cols.append(new_col)

    fact_cols.append("final_scout_score")

    # Loại bỏ các dòng null keys (nếu có lỗi data)
    df_fact = df_fact.dropna(subset=["player_key"])

    # Chọn cột và cast type
    df_fact = df_fact[[c for c in fact_cols if c in df_fact.columns]]

    out_path = os.path.join(output_dir, "fact_player_season_stats.parquet")
    df_fact.to_parquet(out_path, index=False)
    logger.info(f"Đã tạo bảng Fact ({len(df_fact)} dòng) và lưu tại: {out_path}")

    return df_fact
