import os
import glob
import json
import pandas as pd
from datetime import datetime
from logger_config import setup_logger

logger = setup_logger("process_auxiliary_data")

# Duong dan tuong doi phu hop voi Docker/Airflow khi chay tai thu muc goc cua du an
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "local_data_chunks", "sofascore")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "Phase_2", "intermediate")

def get_latest_partition():
    if not os.path.exists(DATA_DIR):
        logger.warning(f"Thu muc du lieu raw khong ton tai: {DATA_DIR}")
        return None
    partitions = [d for d in os.listdir(DATA_DIR) if d.startswith("dt=")]
    if not partitions:
        logger.warning("Khong tim thay bat ky partition dt= nao.")
        return None
    partitions.sort(reverse=True)
    return os.path.join(DATA_DIR, partitions[0])

def process_standings(partition_path):
    standings_files = glob.glob(os.path.join(partition_path, "raw_standings_*.json"))
    all_rows = []
    
    for f in standings_files:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            league_name = data.get("league", "Unknown")
            payload = data.get("data", {})
            if not payload:
                continue
                
            standings_list = payload.get("standings", [])
            if not standings_list:
                continue
                
            rows = standings_list[0].get("rows", [])
            for r in rows:
                team_info = r.get("team", {})
                all_rows.append({
                    "league_name": league_name,
                    "team_id": team_info.get("id"),
                    "team_name": team_info.get("name"),
                    "position": r.get("position"),
                    "matches": r.get("matches"),
                    "wins": r.get("wins"),
                    "draws": r.get("draws"),
                    "losses": r.get("losses"),
                    "goals_scored": r.get("scoresFor"),
                    "goals_conceded": r.get("scoresAgainst"),
                    "goal_diff": r.get("scoresFor", 0) - r.get("scoresAgainst", 0),
                    "points": r.get("points")
                })
        except Exception as e:
            logger.error(f"Loi xu ly file BXH {f}: {e}")
            
    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates(subset=["league_name", "team_id"], keep="last")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, "silver_standings.parquet")
        df.to_parquet(out_path, index=False)
        logger.info(f"Da luu silver_standings.parquet voi {len(df)} ban ghi.")
        return df
    return pd.DataFrame()

def process_top_players(partition_path):
    tp_files = glob.glob(os.path.join(partition_path, "raw_top_players_*.json"))
    all_rows = []
    
    for f in tp_files:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            league_name = data.get("league", "Unknown")
            payload = data.get("data", {})
            if not payload:
                continue
                
            rating_list = payload.get("topPlayers", {}).get("rating", [])
            for r in rating_list:
                player_info = r.get("player", {})
                team_info = r.get("team", {})
                stats = r.get("statistics", {})
                
                all_rows.append({
                    "league_name": league_name,
                    "player_id": player_info.get("id"),
                    "player_name": player_info.get("name"),
                    "team_id": team_info.get("id"),
                    "team_name": team_info.get("name"),
                    "rating": stats.get("rating")
                })
        except Exception as e:
            logger.error(f"Loi xu ly file Top Players {f}: {e}")
            
    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates(subset=["league_name", "player_id"], keep="last")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, "silver_top_players.parquet")
        df.to_parquet(out_path, index=False)
        logger.info(f"Da luu silver_top_players.parquet voi {len(df)} ban ghi.")
        return df
    return pd.DataFrame()

if __name__ == "__main__":
    latest_part = get_latest_partition()
    if not latest_part:
        logger.error("Khong tim thay partition du lieu raw phu hop de xu ly.")
    else:
        logger.info(f"Bat dau xu ly du lieu phu tro tu: {latest_part}")
        process_standings(latest_part)
        process_top_players(latest_part)
