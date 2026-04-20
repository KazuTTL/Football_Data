import os
import pandas as pd
from datetime import date

# Import Data Contract de kiem tra Schema truoc khi ghi
from data_contract import MERGED_SCHEMA

# Cau hinh duong dan
BASE_DIR         = r"C:\FastAPI\Football"
INTERMEDIATE_DIR = os.path.join(BASE_DIR, "Phase_2", "intermediate")
SILVER_DIR       = os.path.join(BASE_DIR, "Phase_2", "silver_zone")

os.makedirs(SILVER_DIR, exist_ok=True)

SILVER_OUTPUT = os.path.join(SILVER_DIR, "players_history.parquet")

# Danh sach cac cot duoc dung de xac dinh "cau thu da thay doi" (Change Detection)
TRACKED_COLUMNS = ["goals_sfs", "assists_sfs", "market_value_tm", "team_sfs", "team_tm"]


# =========================================================
# BUOC 1: KIEM SOAT CHAT LUONG (DATA CONTRACT VALIDATION)
# =========================================================

def validate(df):
    """
    Kiem tra DataFrame gegen HOP DONG SCHEMA (data_contract.py).
    Neu co cot bi thieu hoac sai kieu du lieu, nem ra loi ngay lap tuc.
    Ham nay hoat dong nhu mot tram kiem soat truoc cua vao Silver Zone.
    """
    print("[scd2_loader] Dang kiem tra Data Contract...")
    try:
        MERGED_SCHEMA.validate(df, lazy=True)
        print("[scd2_loader] Schema hop le. Tiep tuc...")
    except Exception as e:
        print(f"[scd2_loader] LOI DATA CONTRACT: {e}")
        raise  # Dung toan bo qua trinh neu schema sai


# =========================================================
# BUOC 2: PHAT HIEN THAY DOI (CHANGE DETECTION)
# =========================================================

def detect_changes(df_new, df_existing):
    """
    So sanh DataFrame moi voi du lieu Silver Zone hien tai.
    Tra ve 3 nhom:
    - new_records: Cau thu xuat hien lan dau (chua co internal_player_id trong Silver).
    - changed_records: Cau thu co thay doi o cac cot TRACKED_COLUMNS.
    - unchanged_ids: Cac internal_player_id khong co gi thay doi.
    """
    # Chi so sanh nhung cau thu da duoc anh xa (co internal_player_id)
    df_new_resolved = df_new.dropna(subset=["internal_player_id"])

    # Neu Silver Zone chua ton tai thi tat ca deu la ban ghi moi
    if df_existing.empty or "internal_player_id" not in df_existing.columns:
        print(f"[scd2_loader] Phan tich: {len(df_new_resolved)} cau thu moi | 0 thay doi | 0 khong doi (Silver Zone trong).")
        return df_new_resolved, pd.DataFrame(), set()

    existing_ids = set(df_existing["internal_player_id"].unique())
    new_ids = set(df_new_resolved["internal_player_id"].unique())

    # Nhom moi hoan toan
    brand_new_ids = new_ids - existing_ids
    new_records = df_new_resolved[df_new_resolved["internal_player_id"].isin(brand_new_ids)].copy()

    # Nhom can kiem tra thay doi (da ton tai trong Silver)
    overlap_ids = new_ids & existing_ids
    df_overlap_new = df_new_resolved[df_new_resolved["internal_player_id"].isin(overlap_ids)]

    # Lay ban ghi hien tai dang active (is_current=True) tu Silver
    df_current = df_existing[
        df_existing["internal_player_id"].isin(overlap_ids) & (df_existing["is_current"] == True)
    ]

    changed_ids = set()
    if not df_current.empty:
        df_compare = pd.merge(
            df_overlap_new[["internal_player_id"] + TRACKED_COLUMNS],
            df_current[["internal_player_id"] + TRACKED_COLUMNS],
            on="internal_player_id",
            suffixes=("_new", "_cur"),
        )
        for col in TRACKED_COLUMNS:
            # So sanh gia tri: coi NaN la bang nhau
            mask = df_compare[f"{col}_new"].fillna(-1) != df_compare[f"{col}_cur"].fillna(-1)
            changed_ids.update(df_compare.loc[mask, "internal_player_id"].tolist())

    changed_records = df_overlap_new[df_overlap_new["internal_player_id"].isin(changed_ids)].copy()
    unchanged_ids   = overlap_ids - changed_ids

    print(f"[scd2_loader] Phan tich: {len(brand_new_ids)} cau thu moi | "
          f"{len(changed_ids)} thay doi | {len(unchanged_ids)} khong doi.")

    return new_records, changed_records, unchanged_ids


# =========================================================
# BUOC 3: AP DUNG SCD TYPE 2
# =========================================================

def apply_scd2(df_new_records, df_changed_records, df_existing):
    """
    Trien khai logic Slowly Changing Dimension Type 2:

    - Ban ghi MOI: Them vao voi is_current=True, valid_from=hom_nay, valid_to=None.
    - Ban ghi THAY DOI:
        + Dong hieu luc ban cu: is_current=False, valid_to=hom_nay.
        + Them ban moi: is_current=True, valid_from=hom_nay, valid_to=None.
    - Ban ghi KHONG DOI: Giu nguyen.
    """
    today = str(date.today())

    def add_scd2_cols(df, is_current=True, valid_from=None, valid_to=None):
        df = df.copy()
        df["is_current"]  = is_current
        df["valid_from"]  = valid_from or today
        df["valid_to"]    = valid_to
        return df

    frames = []

    # Giu lai toan bo Silver cu (se cap nhat mot phan ngay sau)
    df_result = df_existing.copy() if not df_existing.empty else pd.DataFrame()

    # 1. Dong hieu luc cac ban ghi bi THAY DOI trong Silver cu
    if not df_changed_records.empty and not df_result.empty:
        changed_ids = set(df_changed_records["internal_player_id"].unique())
        mask_close  = (df_result["internal_player_id"].isin(changed_ids)) & (df_result["is_current"] == True)
        df_result.loc[mask_close, "is_current"] = False
        df_result.loc[mask_close, "valid_to"]   = today

    # 2. Them ban ghi THAY DOI moi va ban ghi MOI HOAN TOAN
    for df_insert_group in [df_new_records, df_changed_records]:
        if not df_insert_group.empty:
            frames.append(add_scd2_cols(df_insert_group))

    if frames:
        df_inserts = pd.concat(frames, ignore_index=True)
        df_result  = pd.concat([df_result, df_inserts], ignore_index=True) if not df_result.empty else df_inserts

    return df_result


# =========================================================
# ENTRY POINT
# =========================================================

def run():
    """
    Ham dieu phoi chinh cua buoc Silver SCD2 Loader.
    Doc merged_players.parquet -> Validate -> Detect Changes ->
    Apply SCD2 -> Luu ra players_history.parquet.
    """
    print("=== BUOC 5: SILVER SCD2 LOADER ===")

    merged_path = os.path.join(INTERMEDIATE_DIR, "merged_players.parquet")
    if not os.path.exists(merged_path):
        print("[scd2_loader] Loi: Thieu file merged_players.parquet. Hay chay entity_resolution.py truoc!")
        return

    df_new = pd.read_parquet(merged_path)

    # Buoc 1: Kiem soat Schema
    validate(df_new)

    # Buoc 2: Doc du lieu Silver hien tai (neu da co)
    if os.path.exists(SILVER_OUTPUT):
        df_existing = pd.read_parquet(SILVER_OUTPUT)
        print(f"[scd2_loader] Doc Silver Zone hien tai: {len(df_existing)} ban ghi.")
    else:
        print("[scd2_loader] Chua co Silver Zone. Tao moi tu dau.")
        df_existing = pd.DataFrame()

    # Buoc 3: Phat hien thay doi
    df_new_rec, df_changed_rec, _ = detect_changes(df_new, df_existing)

    # Buoc 4: Ap dung SCD2 va luu
    df_silver = apply_scd2(df_new_rec, df_changed_rec, df_existing)

    df_silver.to_parquet(SILVER_OUTPUT, index=False)
    active_count = len(df_silver[df_silver["is_current"] == True]) if "is_current" in df_silver.columns else 0
    print(f"-> Luu Silver Zone: {len(df_silver)} tong ban ghi | {active_count} ban ghi dang hoat dong.")
    print(f"-> Duong dan: {SILVER_OUTPUT}")
    print("=== BUOC 5: HOAN TAT ===")


if __name__ == "__main__":
    run()
