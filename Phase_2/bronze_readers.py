import os
import glob
import json
import pandas as pd
from logger_config import setup_logger

logger = setup_logger("bronze_readers")

# =============================================================
# CAU HINH DUONG DAN DONG (Docker-Compatible)
# PROJECT_ROOT co the duoc truyen vao qua bien moi truong trong Docker.
# Neu khong co, tu dong tinh toan dua tren vi tri cua file nay.
# =============================================================
_THIS_FILE = os.path.abspath(__file__)               # .../Phase_2/bronze_readers.py
_PHASE2_DIR = os.path.dirname(_THIS_FILE)            # .../Phase_2/
BASE_DIR = os.getenv("PROJECT_ROOT", os.path.dirname(_PHASE2_DIR))  # .../Football/
root_chunks = os.path.join(BASE_DIR, "local_data_chunks", "sofascore")
adv_chunks = os.path.join(BASE_DIR, "Phase_1_Advanced", "local_data_chunks", "sofascore")
SOFASCORE_BRONZE_DIR = root_chunks if os.path.exists(root_chunks) and glob.glob(os.path.join(root_chunks, "dt=*")) else adv_chunks
TM_DATA_DIR = os.path.join(BASE_DIR, "Phase_1_Advanced", "data")


# =========================================================
# SOFASCORE READER
# =========================================================

def get_sofascore_raw():
    """
    Doc va lam phang du lieu JSON tu Bronze Zone cua Sofascore.
    Tim thu muc dt=YYYY-MM-DD moi nhat, doc tat ca file JSON ben trong.
    Tra ve: DataFrame tho (chua chuan hoa) + string extraction_date.
    """
    folders = glob.glob(os.path.join(SOFASCORE_BRONZE_DIR, "dt=*"))
    if not folders:
        logger.warning("Khong tim thay du lieu Sofascore!")
        return None, None

    latest_folder = sorted(folders)[-1]
    extraction_date = latest_folder.split("=")[-1]

    all_players = []
    for f_path in glob.glob(os.path.join(latest_folder, "raw_data_*.json")):
        with open(f_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "data" in data:
                all_players.extend(data["data"])

    if not all_players:
        logger.warning("Bo JSON Sofascore rong!")
        return None, None

    # Lam phang cau truc JSON long nhau thanh DataFrame phang
    df = pd.json_normalize(all_players)
    
    # Xoa cac ban ghi trung lap do viec chay nhieu lan trong 1 ngay
    # Neu chay nhieu lan, df se co cac dong trung lap (cung 1 id). Ta giu lai ban ghi cuoi cung (keep="last")
    if "core_info_raw.id" in df.columns:
        original_len = len(df)
        df = df.drop_duplicates(subset=["core_info_raw.id"], keep="last")
        if original_len > len(df):
            logger.info(f"Sofascore: Da loai bo {original_len - len(df)} ban ghi trung lap.")
            
    logger.info(f"Sofascore: Doc {len(df)} ban ghi doc nhat tu {extraction_date}.")
    return df, extraction_date


# =========================================================
# TRANSFERMARKT READER
# =========================================================

def get_transfermarkt_raw():
    """
    Doc 3 file CSV tu Kaggle (players, player_valuations, clubs), thuc hien Join.
    - Tinh gia tri thi truong moi nhat bang cach nhom theo player_id va lay ngay lon nhat.
    - Join tren Club de lay ten CLB chinh thuc.
    Tra ve: DataFrame hop nhat day du.
    """
    players_path     = os.path.join(TM_DATA_DIR, "players.csv")
    valuations_path  = os.path.join(TM_DATA_DIR, "player_valuations.csv")
    clubs_path       = os.path.join(TM_DATA_DIR, "clubs.csv")

    # Kiem tra su ton tai cua ca 3 file truoc khi doc
    missing = [p for p in [players_path, valuations_path, clubs_path] if not os.path.exists(p)]
    if missing:
        logger.error(f"Thieu file Kaggle: {missing}. Hay chay ingestor truoc!")
        return None

    # 1. Doc file
    df_players    = pd.read_csv(players_path, dtype=str)
    df_valuations = pd.read_csv(valuations_path, dtype=str)
    df_clubs      = pd.read_csv(clubs_path, dtype=str)

    # 2. Tinh gia tri thi truong moi nhat (Aggregation)
    # Chuyen sang dung kieu du lieu dung truoc khi sap xep
    df_valuations["date"] = pd.to_datetime(df_valuations["date"], errors="coerce")
    df_valuations["market_value_in_eur"] = pd.to_numeric(
        df_valuations["market_value_in_eur"], errors="coerce"
    )
    # Sap xep theo ngay, lay dong cuoi cung (moi nhat) cho moi player_id
    df_latest_val = (
        df_valuations.sort_values("date")
        .groupby("player_id", as_index=False)
        .tail(1)[["player_id", "market_value_in_eur", "date"]]
        .rename(columns={"market_value_in_eur": "market_value_eur", "date": "valuation_date"})
    )

    # 3. Join: Players + Valuations
    df_merged = pd.merge(df_players, df_latest_val, on="player_id", how="left")

    # 4. Join: Merged + Clubs (de lay ten CLB chinh thuc)
    # Trong players.csv, cot noi voi clubs la "current_club_id"
    df_clubs_mini = df_clubs[["club_id", "name"]].rename(columns={"name": "club_name"})
    df_merged = pd.merge(
        df_merged, df_clubs_mini,
        left_on="current_club_id", right_on="club_id",
        how="left"
    )

    # 5. Giu lai cac cot can thiet, bao gom ca vi tri thi dau
    # "position"     : Nhom vi tri tong quat (Attack, Defender, Midfielder, Goalkeeper)
    # "sub_position" : Vi tri cu the (Centre-Forward, Attacking Midfield, Centre-Back...)
    keep_cols = [
        "player_id", "name", "date_of_birth",
        "position", "sub_position",
        "club_name", "market_value_eur"
    ]
    keep_cols = [c for c in keep_cols if c in df_merged.columns]
    df_merged = df_merged[keep_cols]

    logger.info(f"Transfermarkt: Hop nhat {len(df_merged)} ban ghi (players + valuations + clubs).")
    return df_merged
