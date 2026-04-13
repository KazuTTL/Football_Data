import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import s3_utils_stream
from config_adv import logger, TM_HEADERS

def scrape_team_squad(team_name, team_url):
    """
    Đi vào trang hồ sơ của một đội bóng (Ví dụ: Man City)
    Bóc tách bảng thành viên trong đội.
    """
    logger.info(f" Đang cào toàn bộ đội hình: {team_name}")
    
    # Đợi 3 giây để tránh bị Cloudflare khóa mõm
    time.sleep(3)
    
    try:
        response = requests.get(team_url, headers=TM_HEADERS, timeout=15)
        
        if response.status_code != 200:
            logger.error(f"Transfermarkt từ chối truy cập (Mã {response.status_code}) vào {team_name}. Có thể bị chặn IP.")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Bảng đội hình Transfermarkt thường là class="items"
        squad_table = soup.select_one('table.items')
        
        if not squad_table:
            logger.warning(f"Không tìm thấy bảng cầu thủ tại trang {team_url}")
            return None
            
        squad_members_raw = []
        
        # Các hàng lẻ và chẵn
        rows = squad_table.find('tbody').find_all('tr', class_=['odd', 'even'])
        
        for row in rows:
            # 1. Nhặt Tên và URL cầu thủ
            name_tag = row.select_one('td.hauptlink a')
            if not name_tag:
                continue
                
            player_name = name_tag.text.strip()
            player_relative_url = name_tag.get('href', '')
            
            # Tách ID Transfermarkt (Ví dụ: /erling-haaland/profil/spieler/418560 -> ID là 418560)
            player_tm_id = player_relative_url.split('/')[-1] if player_relative_url else "Unknown"

            # 2. Nhặt Market Value
            # Transfermarkt thường đặt giá trị ở cột có class "rechts hauptlink"
            mv_tag = row.select_one('td.rechts.hauptlink')
            market_value_str = mv_tag.text.strip() if mv_tag else "€0"
            
            # Đóng gói RAW thô sơ nhất có thể
            player_raw_record = {
                "extracted_player_name": player_name,
                "tm_player_id": player_tm_id,
                "market_value_string": market_value_str,
                "profile_url": player_relative_url
            }
            
            squad_members_raw.append(player_raw_record)
            
        logger.info(f"-> Thu hoạch được {len(squad_members_raw)} cầu thủ từ {team_name}")
        return squad_members_raw
        
    except Exception as e:
        logger.error(f"Lỗi trầm trọng khi cào đội {team_name}: {str(e)}")
        return None

def run_scraper_for_mapped_teams():
    logger.info(" KHỞI ĐỘNG CỖ MÁY CÀO TRANSFERMARKT ĐỒNG LOẠT ")
    
    mapping_file = os.path.join(ROOT_DIR, 'team_mapping.json')
    if not os.path.exists(mapping_file):
        logger.error(f"Không tìm thấy {mapping_file}. Vui lòng chạy bootstrap_team_mapping.py trước!")
        return

    with open(mapping_file, 'r', encoding='utf-8') as f:
        team_mapping = json.load(f)

    all_teams_data = []
    
    for team_name, tm_url in team_mapping.items():
        if tm_url: # Bỏ qua các team báo chuỗi rỗng
            squad_raw = scrape_team_squad(team_name, tm_url)
            if squad_raw:
                record = {
                    "sofascore_team_name": team_name,
                    "tm_team_url": tm_url,
                    "players_extracted": len(squad_raw),
                    "squad_list": squad_raw
                }
                all_teams_data.append(record)

    # LƯU FILE RAW TRẦN TRỤI XUỐNG Ổ CỨNG VÀ ĐẨY LÊN S3
    now = datetime.now()
    today_str = now.strftime("%Y%m%d_%H%M")
    partition_date = now.strftime("%Y-%m-%d")
    file_name = f"raw_transfermarkt_squads_{today_str}.json"
    
    final_payload = {
        "metadata": {
            "source": "Transfermarkt Web Scraping",
            "extracted_at": today_str,
            "total_teams_scraped": len(all_teams_data)
        },
        "data": all_teams_data
    }
    
    import asyncio
    # Tái sử dụng s3_utils_stream để xả JSON xuống local (với phân vùng dt=...)
    local_path = asyncio.run(s3_utils_stream.save_chunk_locally(
        final_payload, file_name, source_name="transfermarkt", partition_date=partition_date
    ))
    s3_utils_stream.upload_file_to_s3(
        local_path, file_name, source_name="transfermarkt", partition_date=partition_date
    )
    logger.info(" XONG TOÀN TẬP. DỮ LIỆU TM ĐÃ VÀO BRONZE ZONE.")

if __name__ == "__main__":
    run_scraper_for_mapped_teams()
