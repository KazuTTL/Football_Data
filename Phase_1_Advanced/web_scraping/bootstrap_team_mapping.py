import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import requests
from bs4 import BeautifulSoup
import time
from config_adv import logger, TM_SEARCH_URL, TM_HEADERS, TM_BASE_URL

def find_team_url_on_tm(team_name):
    """ Hàm tự động tìm kiếm tên đội bóng trên Transfermarkt và trả về URL """
    logger.info(f"Đang dò tìm URL cho đội: {team_name}...")
    
    # Giới hạn tốc độ để không bị block
    time.sleep(2) 
    
    params = {'query': team_name}
    try:
        response = requests.get(TM_SEARCH_URL, headers=TM_HEADERS, params=params, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"[Lỗi {response.status_code}] Không thể tìm kiếm: {team_name}")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Transfermarkt trả về kết quả tìm kiếm. Bảng đầu tiên thường là "Vereine" (Câu lạc bộ)
        # class="items" là bảng chứa kết quả
        search_table = soup.select_one('table.items')
        
        if not search_table:
            logger.warning(f"Không tìm thấy bảng kết quả chứa team {team_name}")
            return None
            
        # Tìm thẻ <a> đầu tiên chứa link trong cột chính có class "hauptlink"
        first_row_link = search_table.select_one('tbody tr td.hauptlink a')
        
        if first_row_link and 'href' in first_row_link.attrs:
            # Đường dẫn trả về là dạng /manchester-city/startseite/verein/281
            relative_url = first_row_link['href']
            full_url = TM_BASE_URL + relative_url
            logger.info(f"-> Thấy rồi! {full_url}")
            return full_url
            
        return None
        
    except Exception as e:
        logger.error(f"Lỗi khi search team {team_name}: {str(e)}")
        return None

def bootstrap_mapping():
    logger.info(" BẮT ĐẦU QUÁ TRÌNH KHAI PHÁ TỰ ĐỘNG (AUTO-BOOTSTRAPPING)")
    
    # 1. Quét danh sách các đội bóng từ dữ liệu API đã tải về (Quét tất cả thư mục con)
    local_dir = os.path.join(ROOT_DIR, 'local_data_chunks')
    if not os.path.exists(local_dir):
        logger.error("Chưa có data từ Sofascore để đọc tên đội. Hãy chạy file main_pipeline_advanced.py trước!")
        return

    unique_teams = set()
    
    # Dùng os.walk để quét sâu vào các thư mục dt=...
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            # Sửa đổi: Chấp nhận cả file có prefix 'raw_data_' (mới) và 'sofascore_' (cũ của bạn)
            if (filename.startswith('raw_data_') or filename.startswith('sofascore_')) and filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        players = data.get('data', [])
                        for player in players:
                            team_name = player.get('core_info_raw', {}).get('team', {}).get('name')
                            # Fallback cho file cũ (sofascore_big5_clean_...)
                            if not team_name:
                                team_name = player.get('detailed_statistics', {}).get('domestic_league', {}).get('team', {}).get('name')
                            
                            if team_name:
                                unique_teams.add(team_name)
                except Exception as e:
                    logger.error(f"Lỗi đọc file {file_path}: {str(e)}")

    logger.info(f"Đã trích xuất được {len(unique_teams)} đội bóng duy nhất. Đang lên mạng tìm kiếm...")

    # 2. Tạo hoặc nạp mapping cũ
    mapping_file = os.path.join(ROOT_DIR, 'team_mapping.json')
    team_mapping = {}
    if os.path.exists(mapping_file):
        with open(mapping_file, 'r', encoding='utf-8') as f:
            team_mapping = json.load(f)
            
    # 3. Phép gán tự động
    for team in list(unique_teams):
        if team not in team_mapping or team_mapping[team] == "":
            url_found = find_team_url_on_tm(team)
            if url_found:
                team_mapping[team] = url_found
            else:
                team_mapping[team] = "" # Để người dùng điền tay sau nếu script thất bại
                
    # 4. Xuất file
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(team_mapping, f, indent=4, ensure_ascii=False)
        
    logger.info(f" ĐÃ LẬP XONG BẢN ĐỒ URL! Kết quả được lưu tại: {mapping_file}")

if __name__ == "__main__":
    bootstrap_mapping()
