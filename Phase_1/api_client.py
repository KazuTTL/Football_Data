import requests
import time
from config import HEADERS

def get_latest_season_id(tournament_id):
    print(f"Đang tìm ID mùa giải mới nhất...")
    url = "https://sofascore.p.rapidapi.com/tournaments/get-seasons"
    querystring = {"tournamentId": str(tournament_id)}
    
    response = requests.get(url, headers=HEADERS, params=querystring)
    if response.status_code == 200:
        seasons_data = response.json()
        latest_season_id = seasons_data['seasons'][0]['id']
        print(f"Mùa giải hiện tại: {seasons_data['seasons'][0]['year']} (ID: {latest_season_id})")
        return latest_season_id
    print("Lỗi khi lấy ID mùa giải")
    return None

def get_tournament_standings(tournament_id, season_id):
    print("Đang cào Bảng xếp hạng giải đấu...")
    url = "https://sofascore.p.rapidapi.com/tournaments/get-standings"
    querystring = {"tournamentId": str(tournament_id), "seasonId": str(season_id)}
    
    response = requests.get(url, headers=HEADERS, params=querystring)
    if response.status_code == 200:
        return response.json()
    return None

def get_top_players(tournament_id, season_id):
    print("Đang nhặt danh sách Top cầu thủ...")
    url = "https://sofascore.p.rapidapi.com/tournaments/get-top-players"
    querystring = {"tournamentId": str(tournament_id), "seasonId": str(season_id)}
    
    response = requests.get(url, headers=HEADERS, params=querystring)
    if response.status_code == 200:
        return response.json()
    return None

def get_player_statistics(player_id, tournament_id, season_id):
    url = "https://sofascore.p.rapidapi.com/players/get-statistics"
    querystring = {
        "playerId": str(player_id),
        "tournamentId": str(tournament_id),
        "seasonId": str(season_id)
    }
    
    response = requests.get(url, headers=HEADERS, params=querystring)
    time.sleep(1.5) 
    
    if response.status_code == 200:
        return response.json()
    return None

def get_player_last_matches(player_id):
    url = "https://sofascore.p.rapidapi.com/players/get-last-matches"
    querystring = {"playerId": str(player_id)}
    
    response = requests.get(url, headers=HEADERS, params=querystring)
    time.sleep(1.5)
    
    if response.status_code == 200:
        return response.json()
    return None