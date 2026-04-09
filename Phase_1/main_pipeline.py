from datetime import datetime
import api_client
import s3_utils
from config import TARGET_LEAGUES, CHAMPIONS_LEAGUE_ID

def map_player_data(raw_player):
    garbage_keys = ['fieldTranslations', 'teamColors', 'slug', 'userCount', 'priority', 'sport']
    def prune_garbage(obj):
        if isinstance(obj, dict):
            # Dùng list(obj.keys()) để không bị lỗi khi xóa phần tử trong lúc lặp
            for key in list(obj.keys()):
                if key in garbage_keys:
                    del obj[key]
                else:
                    prune_garbage(obj[key]) # Đệ quy đi sâu vào các dict con
        elif isinstance(obj, list):
            for item in obj:
                prune_garbage(item) # Đệ quy đi sâu vào các mảng
        return obj

    return prune_garbage(raw_player)
def run_etl_pipeline():
    print("KHỞI ĐỘNG MAIN PIPELINE (PRODUCTION)")
    master_dataset = []
    total_api_calls_estimated = 0

    # Lặp qua 5 giải đấu
    for league_name, league_id in TARGET_LEAGUES.items():
        print(f"\n BẮT ĐẦU XỬ LÝ: {league_name}")
        
        current_season_id = api_client.get_latest_season_id(league_id)
        if not current_season_id: continue
            
        standings_data = api_client.get_tournament_standings(league_id, current_season_id)
        team_rank_dict = {}
        if standings_data:
            try:
                for row in standings_data.get('standings', [{}])[0].get('rows', []):
                    team_rank_dict[row['team']['id']] = row['position']
            except Exception: pass

        top_players_data = api_client.get_top_players(league_id, current_season_id)
        top_players_dict = top_players_data.get('topPlayers', {}) if top_players_data else {}
        players_list = top_players_dict.get('rating', [])
        
        # Chỉ lấy 10 người xuất sắc nhất mỗi giải
        target_players = players_list[:10] 
        print(f"Đã nhặt {len(target_players)} cầu thủ để cào.\n")
        
        cl_season_id = api_client.get_latest_season_id(CHAMPIONS_LEAGUE_ID)

        for item in target_players:
            player_info = item.get('player', {})
            player_id = player_info.get('id')
            player_name = player_info.get('name')
            team_id = item.get('team', {}).get('id')
            current_team_rank = team_rank_dict.get(team_id, 20)
            
            print(f" Đang cào và lọc: {player_name}")
            
            domestic_stats = api_client.get_player_statistics(player_id, league_id, current_season_id)
            cl_stats = None
            if cl_season_id:
                cl_stats = api_client.get_player_statistics(player_id, CHAMPIONS_LEAGUE_ID, cl_season_id)
            
            matches_data = api_client.get_player_last_matches(player_id)
            
            # Gói hồ sơ thô
            raw_player_profile = {
                "league_context": league_name,
                "core_info": player_info,
                "team_rank_context": current_team_rank,
                "tournament_rating": item.get('rating'),
                "detailed_statistics": {
                    "domestic_league": domestic_stats,
                    "champions_league": cl_stats
                },
                "recent_matches_history": matches_data
            }
            
            # ĐƯA QUA BỘ LỌC NGAY TẠI CHỖ
            clean_player_profile = map_player_data(raw_player_profile)
            master_dataset.append(clean_player_profile)
            
            total_api_calls_estimated += 3

    # Đóng gói Metadata
    today_str = datetime.now().strftime("%Y%m%d_%H%M")
    file_name = f"sofascore_big5_clean_{today_str}.json"
    
    final_payload = {
        "metadata": {
            "extracted_at": today_str,
            "total_players_extracted": len(master_dataset),
            "estimated_api_cost": total_api_calls_estimated + (len(TARGET_LEAGUES) * 3)
        },
        "data": master_dataset
    }

    s3_utils.upload_to_s3(final_payload, file_name)
    
    print("\n===  KẾT THÚC PIPELINE ===")

if __name__ == "__main__":
    run_etl_pipeline()