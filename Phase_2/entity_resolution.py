import os
import json
import pandas as pd
from rapidfuzz import process, fuzz
from logger_config import setup_logger

logger = setup_logger("entity_resolution")

# =============================================================
# CAU HINH DUONG DAN DONG (Docker-Compatible)
# =============================================================
_THIS_FILE   = os.path.abspath(__file__)               # .../Phase_2/entity_resolution.py
_PHASE2_DIR  = os.path.dirname(_THIS_FILE)             # .../Phase_2/
BASE_DIR     = os.getenv("PROJECT_ROOT", os.path.dirname(_PHASE2_DIR))
INTERMEDIATE_DIR = os.path.join(_PHASE2_DIR, "intermediate")
METADATA_DIR     = os.path.join(_PHASE2_DIR, "metadata")
MAPPING_FILE     = os.path.join(METADATA_DIR, "master_player_mapping.json")

os.makedirs(METADATA_DIR, exist_ok=True)

# Nguong do tuong dong de chap nhan Fuzzy Match (0-100)
FUZZY_THRESHOLD = 85


# =========================================================
# QUAN LY TU DIEN ANH XA ID
# =========================================================

def load_mapping():
    """
    Nap tu dien anh xa ID tu file JSON.
    Cau truc cua moi ban ghi:
    {
        "PLR_00001": {
            "sofascore_id": "108579",
            "transfermarkt_id": "38253",
            "display_name": "Harry Kane"
        }
    }
    """
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    # Lan dau chay: tu dien trong
    return {}


def save_mapping(mapping):
    """Ghi cap nhat tu dien anh xa ID xuong file JSON."""
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


# =========================================================
# LOI LOGIC NHAN DIEN DANH TINH
# =========================================================

def resolve_players(df_sfs, df_tm):
    """
    Ghep doi cau thu giua Sofascore va Transfermarkt theo 2 cap do:

    Cap 1 (Nhanh): Tra tu dien - Neu id_sfs da co trong mapping, lay ngay internal_id.
    Cap 2 (Kham pha): Fuzzy Match - Neu cau thu moi (chua co trong tu dien),
        dung rapidfuzz de tim ten tuong dong nhat trong TM.
        Neu vuot nguong FUZZY_THRESHOLD -> tao internal_id moi & ghi vao tu dien.

    Tra ve: DataFrame da merge 2 nguon, co them cot internal_player_id.
    """
    mapping = load_mapping()
    new_matches = 0

    # ------- Cap 1: Tra Tu Dien -------
    # Tao bang nguoc: sofascore_id -> internal_id de tra nhanh
    sfs_to_internal = {str(v["sofascore_id"]): k for k, v in mapping.items()}
    df_sfs = df_sfs.copy()
    # Khoi tao cot voi kieu object (str) de tranh loi kieu du lieu khi gan 'PLR_xxxxx'
    df_sfs["internal_player_id"] = pd.Series(
        df_sfs["id_sfs"].map(sfs_to_internal), dtype=object
    )

    # ------- Cap 2: Fuzzy Match cho cau thu moi -------
    unresolved = df_sfs[df_sfs["internal_player_id"].isna()]

    if not unresolved.empty:
        logger.info(f"Tim thay {len(unresolved)} cau thu moi chua co trong tu dien. Dang Fuzzy Match...")
        # Chuan bi danh sach ten TM de so sanh (dung ten da chuan hoa)
        tm_names = df_tm["name_tm_norm"].dropna().tolist()

        for idx, row in unresolved.iterrows():
            name_to_match = row.get("name_sfs_norm", "")
            if not name_to_match:
                continue

            # Tim ten TM co do tuong dong cao nhat
            result = process.extractOne(
                name_to_match, tm_names, scorer=fuzz.token_sort_ratio
            )

            if result and result[1] >= FUZZY_THRESHOLD:
                matched_name = result[0]
                tm_record = df_tm[df_tm["name_tm_norm"] == matched_name]
                if tm_record.empty:
                    continue

                tm_record = tm_record.iloc[0]
                # Tao internal_id moi (dinh dang PLR_00001)
                new_internal_id = f"PLR_{len(mapping) + 1:05d}"

                mapping[new_internal_id] = {
                    "sofascore_id":    row["id_sfs"],
                    "transfermarkt_id": str(tm_record["id_tm"]),
                    "display_name":    tm_record.get("name_tm_raw", ""),
                }
                df_sfs.at[idx, "internal_player_id"] = new_internal_id
                new_matches += 1

    if new_matches > 0:
        logger.info(f"Da anh xa {new_matches} cau thu moi vao tu dien.")
        save_mapping(mapping)
    else:
        logger.info("Khong co cau thu moi. Tu dien khong doi.")

    # ------- Merge Du Lieu -------
    # Tao bang phu tu mapping: internal_player_id -> id_tm de lam bridge
    bridge_df = pd.DataFrame([
        {"internal_player_id": k, "id_tm": str(v["transfermarkt_id"])}
        for k, v in mapping.items()
    ])

    # Buoc 1: Noi SFS voi bridge de co id_tm
    df_merged = pd.merge(df_sfs, bridge_df, on="internal_player_id", how="left")

    # Buoc 2: Noi voi TM de lay cac cot _tm
    df_final = pd.merge(
        df_merged, df_tm,
        on="id_tm",
        how="left",
        suffixes=("", "_tm_dup")  # Tranh trung cot neu co
    )

    # Xoa cot trung lap neusufix _tm_dup xuat hien
    dup_cols = [c for c in df_final.columns if c.endswith("_tm_dup")]
    df_final.drop(columns=dup_cols, inplace=True)

    return df_final


# =========================================================
# ENTRY POINT
# =========================================================

def run():
    """
    Ham dieu phoi chinh cua buoc Entity Resolution:
    Doc 2 file Parquet tu intermediate/ -> Goi resolve_players() -> Luu ket qua.
    """
    logger.info("=== BUOC 3: ENTITY RESOLUTION ===")

    sfs_path = os.path.join(INTERMEDIATE_DIR, "sofascore_normalized.parquet")
    tm_path  = os.path.join(INTERMEDIATE_DIR, "transfermarkt_normalized.parquet")

    if not os.path.exists(sfs_path) or not os.path.exists(tm_path):
        logger.error("Thieu du lieu normalized. Hay chay bronze_to_normalized.py truoc!")
        return

    df_sfs = pd.read_parquet(sfs_path)
    df_tm  = pd.read_parquet(tm_path)

    df_merged = resolve_players(df_sfs, df_tm)

    out_path = os.path.join(INTERMEDIATE_DIR, "merged_players.parquet")
    df_merged.to_parquet(out_path, index=False)
    logger.info(f"-> Luu {len(df_merged)} ban ghi da ghep: {out_path}")
    logger.info("=== BUOC 3: HOAN TAT ===")


if __name__ == "__main__":
    run()
