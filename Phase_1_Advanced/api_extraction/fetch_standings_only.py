import os
import sys
import asyncio
import aiohttp
from datetime import datetime

# Add parent directory of api_extraction to sys.path
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_THIS_DIR))

import api_client_async
import s3_utils_stream
from config_adv import logger


CHAMPIONS_LEAGUE_ID = 7
TARGET_LEAGUES = {
    "Premier League": 17,
    "LaLiga": 8,
    "Serie A": 23,
    "Bundesliga": 35,
    "Ligue 1": 34
}

async def fetch_standings_and_top_players(session, league_name, league_id, partition_date, today_str):
    try:
        logger.info(f"=========== Fetching Standings & Top Players: {league_name} (ID: {league_id}) ===========")
        season_id = await api_client_async.get_latest_season_id(session, league_id)
        if not season_id:
            logger.error(f"Khong lay duoc season_id cho {league_name}")
            return
            
        # 1. Fetch Standings
        logger.info(f"[{league_name}] Fetching standings...")
        standings_data = await api_client_async.get_tournament_standings(session, league_id, season_id)
        if standings_data:
            clean_name = league_name.replace(' ', '')
            filename_std = f"raw_standings_{clean_name}_{today_str}.json"
            local_path = await s3_utils_stream.save_chunk_locally({"league": league_name, "data": standings_data}, filename_std, "sofascore", partition_date)
            s3_utils_stream.upload_file_to_s3(local_path, filename_std, "sofascore", partition_date)
            logger.info(f"[{league_name}] Saved standings to {local_path}")
            
        # 2. Fetch Top Players
        logger.info(f"[{league_name}] Fetching top players...")
        top_players_data = await api_client_async.get_top_players(session, league_id, season_id)
        if top_players_data:
            clean_name = league_name.replace(' ', '')
            filename_tp = f"raw_top_players_{clean_name}_{today_str}.json"
            local_path = await s3_utils_stream.save_chunk_locally({"league": league_name, "data": top_players_data}, filename_tp, "sofascore", partition_date)
            s3_utils_stream.upload_file_to_s3(local_path, filename_tp, "sofascore", partition_date)
            logger.info(f"[{league_name}] Saved top players to {local_path}")
            
    except Exception as e:
        logger.error(f"Loi fetch {league_name}: {e}")

async def main():
    start_time = datetime.now()
    now = datetime.now()
    today_str = now.strftime("%Y%m%d_%H%M")
    partition_date = now.strftime("%Y-%m-%d")
    
    logger.info("BAT DAU THU THAP DU LIEU PHU TRO (STANDINGS & TOP PLAYERS ONLY)")
    
    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        # UCL
        await fetch_standings_and_top_players(session, "UEFA Champions League", CHAMPIONS_LEAGUE_ID, partition_date, today_str)
        
        # Domestic Leagues
        for league_name, league_id in TARGET_LEAGUES.items():
            await fetch_standings_and_top_players(session, league_name, league_id, partition_date, today_str)
            await asyncio.sleep(1) # delay nhe tranh ngop
            
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"HOAN TAT THU THAP DU LIEU PHU TRO. TONG THOI GIAN: {duration:.2f} GIAY")

if __name__ == "__main__":
    asyncio.run(main())
