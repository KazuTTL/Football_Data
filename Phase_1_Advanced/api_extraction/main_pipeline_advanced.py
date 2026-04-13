import asyncio
import aiohttp
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
import api_client_async
import s3_utils_stream
from config_adv import TARGET_LEAGUES, CHAMPIONS_LEAGUE_ID, logger

# Kích thước mỗi lô cầu thủ lưu trữ
CHUNK_SIZE = 5

def remove_field_translations(data):
    """Đệ quy xóa trường 'fieldTranslations' - metadata UI vô nghĩa với Data Engineering."""
    if isinstance(data, dict):
        return {k: remove_field_translations(v) for k, v in data.items() if k != 'fieldTranslations'}
    elif isinstance(data, list):
        return [remove_field_translations(item) for item in data]
    return data

async def extract_single_player(session, item, league_id, league_name, current_season_id, cl_season_id, team_rank_dict):
    """ Hàm này sẽ chạy song song để cào profile 1 cầu thủ đầy đủ """
    player_info = item.get('player', {})
    player_id = player_info.get('id')
    player_name = player_info.get('name')
    team_id = item.get('team', {}).get('id')
    current_team_rank = team_rank_dict.get(team_id, 20)
    
    logger.info(f"-> Bắt đầu trích xuất: {player_name}")
    
    # CHẠY SONG SONG TRONG NỘI BỘ 1 CẦU THỦ
    domestic_task = api_client_async.get_player_statistics(session, player_id, league_id, current_season_id)
    matches_task = api_client_async.get_player_last_matches(session, player_id)
    
    tasks = [domestic_task, matches_task]
    if cl_season_id:
        cl_task = api_client_async.get_player_statistics(session, player_id, CHAMPIONS_LEAGUE_ID, cl_season_id)
        tasks.append(cl_task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Phân rã kết quả
    domestic_stats = results[0] if not isinstance(results[0], Exception) else None
    matches_data = results[1] if not isinstance(results[1], Exception) else None
    cl_stats = results[2] if len(results) > 2 and not isinstance(results[2], Exception) else None
    
    # LƯU RAW NGUYÊN BẢN (KHÔNG CẮT XÉN - CHUẨN DE)
    raw_player_profile = {
        "league_context": league_name,
        "team_rank_context": current_team_rank,
        "tournament_rating": item.get('rating'),
        "core_info_raw": player_info, 
        "statistics_raw": {
            "domestic_league": domestic_stats,
            "champions_league": cl_stats
        },
        "recent_matches_history_raw": matches_data
    }
    return raw_player_profile

async def process_league(session, league_name, league_id, cl_season_id):
    logger.info(f"=========== BẮT ĐẦU: {league_name} ===========")
    current_season_id = await api_client_async.get_latest_season_id(session, league_id)
    if not current_season_id: return

    # Lấy Standings, sau này cần thiết lập Retry cẩn thận hơn
    standings_data = await api_client_async.get_tournament_standings(session, league_id, current_season_id)
    team_rank_dict = {}
    if standings_data:
        try:
            for row in standings_data.get('standings', [{}])[0].get('rows', []):
                team_rank_dict[row['team']['id']] = row['position']
        except Exception: pass

    # Lấy Top Players
    top_players_data = await api_client_async.get_top_players(session, league_id, current_season_id)
    players_list = top_players_data.get('topPlayers', {}).get('rating', []) if top_players_data else []
    
    # Giả sử lấy top 10 cho nhẹ
    target_players = players_list[:10]
    logger.info(f"Đã chuẩn bị {len(target_players)} cầu thủ từ {league_name}")
    
    # ----- HỆ THỐNG YẾT HẦU SONG SONG (RATE LIMITING BẰNG SEMAPHORE) -----
    # Ngăn việc mở quá nhiều request một lúc gây từ chối dịch vụ (DDoS) mình tự tạo
    sem = asyncio.Semaphore(5) # Cho phép 5 Cầu thủ chạy cùng lúc
    
    async def sem_extract(item):
        async with sem:
            return await extract_single_player(session, item, league_id, league_name, current_season_id, cl_season_id, team_rank_dict)

    player_tasks = [sem_extract(item) for item in target_players]
    
    # Tung luồng lấy thông tin tất cả target_players cùng một lúc, chia Lô (Chunks)
    league_dataset = await asyncio.gather(*player_tasks)
    
    # LƯU CHUNK XUỐNG Ổ CỨNG TRƯỚC KHI RAM ĐẦY (1 GIẢI = 1 FILE CHUNK)
    now = datetime.now()
    today_str = now.strftime("%Y%m%d_%H%M")
    partition_date = now.strftime("%Y-%m-%d")
    chunk_file_name = f"raw_data_{league_name.replace(' ', '')}_{today_str}.json"
    
    # Xóa fieldTranslations (metadata UI, vô nghĩa trong DE) trước khi lưu
    cleaned_dataset = remove_field_translations(list(league_dataset))
    
    payload = {
        "metadata": {
            "extracted_at": today_str,
            "league": league_name,
            "status": "RAW Bronze Data",
            "total_players": len(cleaned_dataset)
        },
        "data": cleaned_dataset
    }
    
    local_path = await s3_utils_stream.save_chunk_locally(payload, chunk_file_name, source_name="sofascore", partition_date=partition_date)
    s3_utils_stream.upload_file_to_s3(local_path, chunk_file_name, source_name="sofascore", partition_date=partition_date)


async def main():
    logger.info(" KHỞI ĐỘNG HỆ THỐNG ASYNC DATA EXTRACTION PIPELINE ⚡")
    start_time = datetime.now()
    
    # Tạo connection pool hiệu năng cao nhờ aiohttp (Dùng để gọi tất cả API trong suốt quá trình chạy)
    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        # Chuẩn bị thông tin Cúp C1 (Dùng chung cho mọi giải)
        cl_season_id = await api_client_async.get_latest_season_id(session, CHAMPIONS_LEAGUE_ID)
        
        # Chạy từng giải đấu một (Để tránh ngộp API - hoặc có thể gather cả 5 giải đấu cùng lúc nếu proxy xịn)
        for league_name, league_id in TARGET_LEAGUES.items():
            await process_league(session, league_name, league_id, cl_season_id)
            
    # Hiển thị thời gian chạy
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f" HOÀN TẤT PIPELINE. TỔNG THỜI GIAN: {duration:.2f} GIÂY")

if __name__ == "__main__":
    asyncio.run(main())
