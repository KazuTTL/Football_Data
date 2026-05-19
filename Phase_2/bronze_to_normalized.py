import os
import pandas as pd
from datetime import datetime
from unidecode import unidecode
from logger_config import setup_logger

logger = setup_logger("bronze_to_normalized")

# Import Reader tu cung thu muc
from bronze_readers import get_sofascore_raw, get_transfermarkt_raw

# =============================================================
# CAU HINH DUONG DAN DONG (Docker-Compatible)
# =============================================================
_THIS_FILE = os.path.abspath(__file__)                # .../Phase_2/bronze_to_normalized.py
_PHASE2_DIR = os.path.dirname(_THIS_FILE)             # .../Phase_2/
BASE_DIR   = os.getenv("PROJECT_ROOT", os.path.dirname(_PHASE2_DIR))  # .../Football/
OUTPUT_DIR = os.path.join(_PHASE2_DIR, "intermediate")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================================================
# HAM CHUAN HOA DUNG CHUNG
# =========================================================

"""
    Lop loc 1 - Text Normalization:
    - Xoa dau tieng Latin / Tay Ban Nha / Viet (unidecode): Odegaard -> odegaard
    - Chuyen chu thuong (lowercase): Harry Kane -> harry kane
    - Xoa khoang trang thua (strip)
"""
def normalize_text(text):
    if pd.isna(text) or str(text).strip() == "":
        return ""
    return unidecode(str(text)).lower().strip()


# =========================================================
# CHUAN HOA SOFASCORE
# =========================================================

def process_sofascore():
    """
    Goi Reader -> Chon loc cot can thiet -> Gap Hau to _sfs (Lineage) ->
    Ap dung Text Normalization -> Dong dau thoi gian.
    Tra ve: DataFrame sach voi ten cot co hau to _sfs.
    """
    df_raw, extraction_date = get_sofascore_raw()
    if df_raw is None:
        return None

    # Anh xa cot goc -> ten moi co hau to _sfs (Data Lineage)
    # Chi lay nhung cot thuc su ton tai de tranh loi khi schema thay doi
    col_map = {
        "core_info_raw.id":                                      "id_sfs",
        "core_info_raw.name":                                    "name_sfs_raw",
        "league_context":                                        "league_sfs",   # Ten giai dau (Premier League, La Liga...)
        "team_rank_context":                                     "team_rank_sfs", # Thu hang doi bong
        "statistics_raw.domestic_league.team.name":              "team_sfs",
        "statistics_raw.domestic_league.statistics.goals":       "goals_sfs",
        "statistics_raw.domestic_league.statistics.assists":     "assists_sfs",
        "statistics_raw.domestic_league.statistics.appearances": "appearances_sfs",
        "statistics_raw.domestic_league.statistics.minutesPlayed": "minutes_played_sfs", # So phut thi dau thuc te
        "statistics_raw.domestic_league.statistics.rating":        "base_rating_sfs",    # Diem sofascore goc thuc te
    }
    valid_map = {k: v for k, v in col_map.items() if k in df_raw.columns}
    df = df_raw[list(valid_map.keys())].rename(columns=valid_map).copy()

    # Text Normalization cho ten cau thu
    df["name_sfs_norm"] = df["name_sfs_raw"].apply(normalize_text)

    # Suy ra mua giai tu extraction_date (VD: 2026-04-13 -> "2025-2026")
    # Neu thang >= 8: mua bat dau la nam do (VD: 2025-08 -> 2025-2026)
    # Neu thang < 8: mua bat dau la nam truoc (VD: 2026-04 -> 2025-2026)
    try:
        ext_year = int(extraction_date[:4])
        ext_month = int(extraction_date[5:7])
        season_start = ext_year if ext_month >= 8 else ext_year - 1
        df["season_sfs"] = f"{season_start}-{season_start + 1}"
    except (ValueError, TypeError, IndexError):
        df["season_sfs"] = None

    # Xoa ban ghi bi thieu ID (Data Quality co ban nhat)
    df.dropna(subset=["id_sfs"], inplace=True)
    df["id_sfs"] = df["id_sfs"].astype(str)

    # Data Lineage: Dong dau ngay cao du lieu Sofascore
    df["updated_at_sfs"] = extraction_date

    logger.info(f"Sofascore: Chuan hoa xong {len(df)} cau thu.")
    return df


# =========================================================
# CHUAN HOA TRANSFERMARKT
# =========================================================

def process_transfermarkt():
    """
    Goi Reader (da bao gom Join 3 file) -> Chon loc cot -> Gap Hau to _tm (Lineage) ->
    Chuan hoa Ngay Sinh -> Ap dung Text Normalization -> Dong dau thoi gian.
    Tra ve: DataFrame sach voi ten cot co hau to _tm.
    """
    df_raw = get_transfermarkt_raw()
    if df_raw is None:
        return None

    # Anh xa cot goc -> ten moi co hau to _tm (Data Lineage)
    col_map = {
        "player_id":       "id_tm",
        "name":            "name_tm_raw",
        "date_of_birth":   "dob_tm",
        "position":        "position_tm",     # Nhom vi tri: Attack, Defender...
        "sub_position":    "sub_position_tm", # Vi tri cu the: Centre-Forward...
        "club_name":       "team_tm",
        "market_value_eur":"market_value_tm",
    }
    valid_map = {k: v for k, v in col_map.items() if k in df_raw.columns}
    df = df_raw[list(valid_map.keys())].rename(columns=valid_map).copy()

    # Text Normalization cho ten cau thu
    df["name_tm_norm"] = df["name_tm_raw"].apply(normalize_text)

    # Chuan hoa Ngay Sinh ve ISO 8601 (YYYY-MM-DD)
    # pd.to_datetime tu dong xu ly nhieu format khac nhau
    if "dob_tm" in df.columns:
        df["dob_tm"] = pd.to_datetime(df["dob_tm"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Data Lineage: Dong dau ngay tai ve CSV tu Kaggle
    df["updated_at_tm"] = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Transfermarkt: Chuan hoa xong {len(df)} cau thu.")
    return df


# =========================================================
# ENTRY POINT
# =========================================================

def run():
    """
    Ham dieu phoi chinh cua buoc Normalize:
    Goi 2 ham xu ly -> Luu ra file Parquet trong thu muc intermediate/.
    """
    logger.info("=== BUOC 2: BRONZE TO NORMALIZED ===")

    df_sfs = process_sofascore()
    if df_sfs is not None:
        out_path = os.path.join(OUTPUT_DIR, "sofascore_normalized.parquet")
        df_sfs.to_parquet(out_path, index=False)
        logger.info(f"-> Luu: {out_path}")

    df_tm = process_transfermarkt()
    if df_tm is not None:
        out_path = os.path.join(OUTPUT_DIR, "transfermarkt_normalized.parquet")
        df_tm.to_parquet(out_path, index=False)
        logger.info(f"-> Luu: {out_path}")

    logger.info("=== BUOC 2: HOAN TAT ===")


if __name__ == "__main__":
    run()
