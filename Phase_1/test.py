import json
import os
from datetime import datetime
import api_client
# Xóa dòng import s3_utils đi vì không dùng đến
from config import TARGET_LEAGUES

def run_local_pipeline():
    print("===  KHỞI ĐỘNG CHẠY THỬ (DRY RUN) ===\n")
    
    master_dataset = []
    
    # ÉP CÂN ĐỂ TEST: Chỉ lấy giải đầu tiên trong TARGET_LEAGUES
    first_league_name = list(TARGET_LEAGUES.keys())[0]
    first_league_id = TARGET_LEAGUES[first_league_name]
    
    print(f" ĐANG CHẠY THỬ VỚI GIẢI: {first_league_name} (ID: {first_league_id})")
    
    # 1. Mùa giải & Xếp hạng
    current_season_id = api_client.get_latest_season_id(first_league_id)
    if not current_season_id:
        return
        
    standings_data = api_client.get_tournament_standings(first_league_id, current_season_id)
    team_rank_dict = {}
    if standings_data:
        try:
            standings_list = standings_data.get('standings', [{}])[0].get('rows', [])
            for row in standings_list:
                team_rank_dict[row['team']['id']] = row['position']
        except Exception:
            pass

    # 2. Lấy Top Cầu thủ
# 2. Lấy Top Cầu thủ
    top_players_data = api_client.get_top_players(first_league_id, current_season_id)
    
    # Bước 1: Lấy cái Từ điển phân loại
    top_players_dict = top_players_data.get('topPlayers', {}) if top_players_data else {}
    
    # Bước 2: Chọn nhánh 'rating' (hoặc bạn có thể đổi thành 'goals', 'assists')
    # Lúc này players_list mới thực sự là một Mảng (List)
    players_list = top_players_dict.get('rating', []) 
    
    # Bước 3: Cắt mảng như bình thường
    target_players = players_list[:2] 
    print(f"🎯 Đã nhặt {len(target_players)} cầu thủ xuất sắc nhất (theo Rating) để cào thử.\n")
    
    # 3. Vòng lặp
    for item in target_players:
        player_info = item.get('player', {})
        player_id = player_info.get('id')
        player_name = player_info.get('name')
        team_id = item.get('team', {}).get('id')
        current_team_rank = team_rank_dict.get(team_id, 20)
        
        print(f" Xử lý: {player_name}")
        
        stats_data = api_client.get_player_statistics(player_id, first_league_id, current_season_id)
        matches_data = api_client.get_player_last_matches(player_id)
        
        player_profile = {
            "league_context": first_league_name,
            "core_info": player_info,
            "team_rank_context": current_team_rank,
            "tournament_rating": item.get('rating'),
            "detailed_statistics": {"domestic_league": stats_data},
            "recent_matches_history": matches_data
        }
        
        master_dataset.append(player_profile)

    # 4. Gắn Metadata
    today_str = datetime.now().strftime("%Y%m%d_%H%M")
    file_name = f"local_test_{first_league_name.replace(' ', '')}_{today_str}.json"
    
    final_payload = {
        "metadata": {
            "extracted_at": today_str,
            "status": "Local Dry Run",
            "total_players": len(master_dataset)
        },
        "data": master_dataset
    }

    # 5. LƯU XUỐNG Ổ CỨNG MÁY TÍNH (Thay vì S3)
    # Tạo thư mục 'local_data' nếu chưa có
    os.makedirs('local_data', exist_ok=True)
    file_path = os.path.join('local_data', file_name)
    
    # Lưu file JSON với indent=4 để dễ đọc bằng mắt thường
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(final_payload, f, ensure_ascii=False, indent=4)
        
    print(f"\n THÀNH CÔNG! Đã lưu file tại máy tính: {file_path}")

if __name__ == "__main__":
    run_local_pipeline()