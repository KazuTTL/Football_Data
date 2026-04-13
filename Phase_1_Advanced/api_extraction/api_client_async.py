import aiohttp
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
import logging
from config_adv import HEADERS, logger

# Thiết lập Retry Policy: 
# Tối đa 4 lần thử. Đợi khoảng thời gian tăng dần 2s, 4s, 8s nếu gặp lỗi mạng hoặc Timeout.
# Điều này chống bị "Ban" hoặc sập pipeline khi rớt mạng tạ thời.
class APICallError(Exception):
    pass

def custom_log(retry_state):
    logger.warning(f"Lỗi truy xuất API. Đang tự động thử lại... (Lần {retry_state.attempt_number})")

@retry(
    stop=stop_after_attempt(4), 
    wait=wait_exponential(multiplier=2, min=2, max=10),
    before_sleep=custom_log
)
async def async_fetch_json(session, url, querystring, api_name="API"):
    try:
        async with session.get(url, headers=HEADERS, params=querystring, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                return data
            elif response.status == 429:
                logger.error(f"[Rate Limit 429] {api_name} - Đã vượt quá giới hạn gọi.")
                raise APICallError("Rate limit hit") # Kích hoạt retry của tenacity
            else:
                logger.error(f"[{response.status}] Lỗi tại {api_name}: {url}")
                raise APICallError(f"Status {response.status}")
    except asyncio.TimeoutError:
         logger.warning(f"[Timeout] kết nối đến {api_name} chậm, đang thử lại...")
         raise APICallError("Timeout")


async def get_latest_season_id(session, tournament_id):
    url = "https://sofascore.p.rapidapi.com/tournaments/get-seasons"
    qs = {"tournamentId": str(tournament_id)}
    
    data = await async_fetch_json(session, url, qs, "Get Seasons")
    if data and 'seasons' in data and len(data['seasons']) > 0:
        return data['seasons'][0]['id']
    return None

async def get_tournament_standings(session, tournament_id, season_id):
    url = "https://sofascore.p.rapidapi.com/tournaments/get-standings"
    qs = {"tournamentId": str(tournament_id), "seasonId": str(season_id)}
    return await async_fetch_json(session, url, qs, "Get Standings")

async def get_top_players(session, tournament_id, season_id):
    url = "https://sofascore.p.rapidapi.com/tournaments/get-top-players"
    qs = {"tournamentId": str(tournament_id), "seasonId": str(season_id)}
    return await async_fetch_json(session, url, qs, "Get Top Players")

async def get_player_statistics(session, player_id, tournament_id, season_id):
    url = "https://sofascore.p.rapidapi.com/players/get-statistics"
    qs = {"playerId": str(player_id), "tournamentId": str(tournament_id), "seasonId": str(season_id)}
    return await async_fetch_json(session, url, qs, f"Get Player Stats {player_id}")

async def get_player_last_matches(session, player_id):
    url = "https://sofascore.p.rapidapi.com/players/get-last-matches"
    qs = {"playerId": str(player_id)}
    return await async_fetch_json(session, url, qs, f"Get Player Matches {player_id}")
