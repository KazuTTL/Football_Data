import os
import glob
import json
import pandas as pd
from datetime import datetime
from unidecode import unidecode

# Cấu hình đường dẫn
BASE_DIR = r"C:\FastAPI\Football"
SOFASCORE_BRONZE_DIR = os.path.join(BASE_DIR, "Phase_1_Advanced", "local_data_chunks", "sofascore")
TM_CSV_PATH = os.path.join(BASE_DIR, "data", "players.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "Phase_2_Standardization", "intermediate")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def normalize_text(text):
    """(Text Normalization) Chuẩn hóa tên: xóa dấu Tiếng Việt/Latin, viết thường, xóa khoảng trắng thừa."""
    if pd.isna(text) or str(text).strip() == "":
        return ""
    # Ví dụ: "Kévin De Bruyne " -> "kevin de bruyne"
    return unidecode(str(text)).lower().strip()

def process_sofascore():
    print("\n[ SOFASCORE ] Dang lay du lieu tu Bronze Zone...")
    # Lấy thư mục dt=YYYY-MM-DD mới nhất
    folders = glob.glob(os.path.join(SOFASCORE_BRONZE_DIR, "dt=*"))
    if not folders:
        print("Khong tim thay du lieu Sofascore!")
        return None
        
    latest_folder = sorted(folders)[-1]
    extraction_date = latest_folder.split('=')[-1]
    
    all_players = []
    for f_path in glob.glob(os.path.join(latest_folder, "*.json")):
        with open(f_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'data' in data:
                all_players.extend(data['data'])
                
    df = pd.json_normalize(all_players)
    
    # Rút trích các cột quan trọng và đổi tên (Thêm Hậu tố _sfs để truy vết Lineage)
    sfs_cols = {}
    
    # ID & Info
    if 'core_info_raw.id' in df.columns: sfs_cols['core_info_raw.id'] = 'id_sfs'
    if 'core_info_raw.name' in df.columns: sfs_cols['core_info_raw.name'] = 'name_sfs_raw'
    
    # Team
    if 'statistics_raw.domestic_league.team.name' in df.columns:
        sfs_cols['statistics_raw.domestic_league.team.name'] = 'team_sfs'
        
    # Stats (An toàn: kiểm tra xem nhánh JSON có chứa field goals không)
    if 'statistics_raw.domestic_league.statistics.goals' in df.columns:
        sfs_cols['statistics_raw.domestic_league.statistics.goals'] = 'goals_sfs'
    if 'statistics_raw.domestic_league.statistics.assists' in df.columns:
        sfs_cols['statistics_raw.domestic_league.statistics.assists'] = 'assists_sfs'

    df_clean = df[list(sfs_cols.keys())].rename(columns=sfs_cols).copy()
    
    # 1. Text Normalization cho Tên Cầu thủ
    df_clean['name_sfs_norm'] = df_clean['name_sfs_raw'].apply(normalize_text)
    
    # 2. Xóa các cầu thủ bị thiếu ID (Data Quality cơ bản)
    df_clean.dropna(subset=['id_sfs'], inplace=True)
    df_clean['id_sfs'] = df_clean['id_sfs'].astype(str)
    
    # 3. Đóng dấu thời gian (Data Lineage)
    df_clean['updated_at_sfs'] = extraction_date
    
    return df_clean

def process_transfermarkt():
    print("\n[ TRANSFERMARKT ] Dang lay du lieu tu Bronze CSV...")
    if not os.path.exists(TM_CSV_PATH):
        print(f"Khong tim thay file: {TM_CSV_PATH}")
        return None
        
    df = pd.read_csv(TM_CSV_PATH, dtype=str) # Đọc dạng chuỗi để tránh lỗi kiểu dữ liệu tạp
    
    # Rút trích cột và Gắn Hậu tố _tm
    tm_cols = {
        'player_id': 'id_tm',
        'name': 'name_tm_raw',
        'date_of_birth': 'dob_tm',
        'current_club_name': 'team_tm',
        'market_value_in_eur': 'market_value_tm'
    }
    
    # Giữ lại những cột có thật trong file CSV
    valid_cols = {k: v for k, v in tm_cols.items() if k in df.columns}
    df_clean = df[list(valid_cols.keys())].rename(columns=valid_cols).copy()
    
    # 1. Text Normalization
    df_clean['name_tm_norm'] = df_clean['name_tm_raw'].apply(normalize_text)
    
    # 2. Chuẩn hóa Ngày Sinh chuẩn ISO 8601 (YYYY-MM-DD)
    # pd.to_datetime cực kỳ thông minh, nó tự phân tích nhiều format ngày tháng khác nhau
    if 'dob_tm' in df_clean.columns:
        df_clean['dob_tm'] = pd.to_datetime(df_clean['dob_tm'], errors='coerce').dt.strftime('%Y-%m-%d')
        
    # 3. Chuẩn hóa Kiểu dữ liệu Định Giá
    if 'market_value_tm' in df_clean.columns:
        df_clean['market_value_tm'] = pd.to_numeric(df_clean['market_value_tm'], errors='coerce')
        
    # 4. Đóng dấu thời gian (Do CSV không ghi ngày Tải về, giả định là Hôm nay)
    df_clean['updated_at_tm'] = datetime.now().strftime('%Y-%m-%d')
    
    return df_clean

def run():
    print("=== BUOC 1: BRONZE TO NORMALIZED ===")
    df_sfs = process_sofascore()
    df_tm = process_transfermarkt()
    
    if df_sfs is not None:
        sfs_out = os.path.join(OUTPUT_DIR, "sofascore_normalized.parquet")
        df_sfs.to_parquet(sfs_out, index=False)
        print(f"-> Sofascore: Xu ly {len(df_sfs)} dong | Da luu: {sfs_out}")
        
    if df_tm is not None:
        tm_out = os.path.join(OUTPUT_DIR, "transfermarkt_normalized.parquet")
        df_tm.to_parquet(tm_out, index=False)
        print(f"-> Transfermarkt: Xu ly {len(df_tm)} dong | Da luu: {tm_out}")

if __name__ == "__main__":
    run()
