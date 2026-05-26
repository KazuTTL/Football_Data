import os
import sys
import streamlit as st
import pandas as pd
import logging

logger = logging.getLogger("db_utils")

# Setup sys.path to find Phase_3_Gold.star_schema
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
phase_4_dir = os.path.dirname(_THIS_DIR)
project_root = os.path.dirname(phase_4_dir)
star_schema_dir = os.path.join(project_root, "Phase_3_Gold", "star_schema")

if star_schema_dir not in sys.path:
    sys.path.append(star_schema_dir)

try:
    from db_connection import get_motherduck_connection
except ImportError:
    # Fallback definition if path resolution fails
    def get_motherduck_connection(db_name="football_data"):
        import duckdb
        from dotenv import load_dotenv
        env_path = os.path.join(project_root, "Phase_1_Advanced", ".env")
        load_dotenv(dotenv_path=env_path)
        token = os.getenv("MOTHERDUCK_TOKEN")
        if not token:
            raise ValueError("Thiếu MOTHERDUCK_TOKEN")
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        conn.execute(f"USE {db_name}")
        return conn

@st.cache_resource
def get_cached_motherduck_connection():
    return get_motherduck_connection()

@st.cache_data(ttl=600)
def load_data():
    """
    Load data from MotherDuck Cloud Data Warehouse and merge player market values.
    Returns:
         df_star (pd.DataFrame): Player season statistics.
         df_rating (pd.DataFrame): Gold player rating engine outputs.
         df_history (pd.DataFrame): Historical player market value (SCD2).
    """
    try:
        conn = get_cached_motherduck_connection()

        # Join with silver_players to get penalty_goals_sfs and team_rank_sfs, including cl fields
        df_star = conn.execute("""
            SELECT
                p.internal_player_id,
                p.name                          AS player_name,
                p.sub_position                  AS position,
                p.current_market_value          AS market_value,
                pos.name                        AS position_group,
                t.name                          AS team,
                tour.name                       AS league,
                f.goals, f.assists,
                COALESCE(s.penalty_goals_sfs, 0) AS penalty_goals,
                s.team_rank_sfs                 AS team_rank,
                ROUND(f.final_scout_score, 2)   AS scout_score,
                s.xg_sfs                        AS xg,
                s.xa_sfs                        AS xa,
                s.key_passes_sfs                AS key_passes,
                s.successful_dribbles_sfs       AS successful_dribbles,
                s.tackles_sfs                   AS tackles,
                s.interceptions_sfs             AS interceptions,
                s.clearances_sfs                AS clearances,
                s.saves_sfs                     AS saves,
                s.clean_sheet_sfs               AS clean_sheets,
                s.goals_prevented_sfs           AS goals_prevented,
                s.accurate_passes_pct_sfs       AS accurate_passes_pct,
                s.aerial_duels_won_pct_sfs      AS aerial_duels_won_pct,
                s.ground_duels_won_pct_sfs      AS ground_duels_won_pct,
                
                -- UCL Fields
                COALESCE(s.goals_cl_sfs, 0)     AS goals_cl,
                COALESCE(s.assists_cl_sfs, 0)   AS assists_cl,
                COALESCE(s.penalty_goals_cl_sfs, 0) AS penalty_goals_cl,
                s.base_rating_cl_sfs            AS rating_cl,
                s.xg_cl_sfs                     AS xg_cl,
                s.xa_cl_sfs                     AS xa_cl,
                s.key_passes_cl_sfs             AS key_passes_cl,
                s.successful_dribbles_cl_sfs    AS successful_dribbles_cl,
                s.tackles_cl_sfs                AS tackles_cl,
                s.interceptions_cl_sfs          AS interceptions_cl,
                s.clearances_cl_sfs             AS clearances_cl,
                s.saves_cl_sfs                  AS saves_cl,
                s.clean_sheet_cl_sfs            AS clean_sheets_cl,
                s.accurate_passes_pct_cl_sfs    AS accurate_passes_pct_cl,
                s.aerial_duels_won_pct_cl_sfs   AS aerial_duels_won_pct_cl,
                s.ground_duels_won_pct_cl_sfs   AS ground_duels_won_pct_cl,
                s.team_cl_sfs                   AS team_cl,
                s.valid_from                    AS valid_from
            FROM fact_player_season_stats f
            LEFT JOIN dim_player     p   ON f.player_key      = p.player_key
            LEFT JOIN dim_team       t   ON f.team_key        = t.team_key
            LEFT JOIN dim_position   pos ON f.position_key    = pos.position_key
            LEFT JOIN dim_tournament tour ON f.tournament_key = tour.tournament_key
            LEFT JOIN silver_players s   ON p.internal_player_id = s.internal_player_id AND s.is_current = True
        """).df()

        df_rating = conn.execute("SELECT * FROM gold_player_rating").df()
        
        # Load historical market value data from dim_player for the line chart
        df_history = conn.execute("""
            SELECT 
                name,
                current_market_value,
                valid_from
            FROM dim_player
            WHERE current_market_value IS NOT NULL
            ORDER BY valid_from ASC
        """).df()
        
        # Merge market_value into rating df for radar display compatibility
        if "market_value" not in df_rating.columns and not df_star.empty:
            mv_map = df_star[["internal_player_id", "market_value", "league", "team"]].drop_duplicates("internal_player_id")
            df_rating = df_rating.merge(mv_map, on="internal_player_id", how="left")
            
        return df_star, df_rating, df_history
    except Exception as e:
        st.error(f"Lỗi kết nối MotherDuck: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

@st.cache_data(ttl=3600)
def get_full_league_standings(league_name):
    """
    Query league standings from MotherDuck (DWH-Centric).
    """
    try:
        conn = get_cached_motherduck_connection()
        if league_name in ["Champions League", "UEFA Champions League"]:
            query = """
                SELECT * FROM silver_standings 
                WHERE league_name IN ('Champions League', 'UEFA Champions League')
                ORDER BY position
            """
        else:
            query = f"""
                SELECT * FROM silver_standings 
                WHERE league_name = '{league_name}' 
                   OR REPLACE(league_name, ' ', '') = '{league_name.replace(' ', '')}'
                ORDER BY position
            """
        df = conn.execute(query).df()
        
        if not df.empty:
            records = []
            for _, r in df.iterrows():
                team_id = r.get("team_id")
                logo_url = f"https://api.sofascore.app/api/v1/team/{team_id}/image" if pd.notna(team_id) else None
                
                diff = r.get("goal_diff", r.get("goals_scored", 0) - r.get("goals_conceded", 0))
                diff_str = f"+{int(diff)}" if diff > 0 else str(int(diff))
                
                records.append({
                    "Hạng": int(r.get("position")),
                    "Logo": logo_url,
                    "Câu lạc bộ": r.get("team_name"),
                    "Trận": int(r.get("matches")),
                    "T": int(r.get("wins")),
                    "H": int(r.get("draws")),
                    "B": int(r.get("losses")),
                    "Hiệu số": diff_str,
                    "Điểm": int(r.get("points"))
                })
            return pd.DataFrame(records).set_index("Hạng")
            
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching standings from DWH: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_ucl_top_players():
    """
    Query UEFA Champions League top players from MotherDuck (DWH-Centric).
    """
    try:
        conn = get_cached_motherduck_connection()
        query = """
            SELECT * FROM silver_top_players 
            WHERE league_name = 'UEFA Champions League' 
               OR REPLACE(league_name, ' ', '') = 'UEFAChampionsLeague'
            ORDER BY rating DESC
        """
        df = conn.execute(query).df()
        
        if not df.empty:
            records = []
            for _, r in df.iterrows():
                team_id = r.get("team_id")
                logo_url = f"https://api.sofascore.app/api/v1/team/{team_id}/image" if pd.notna(team_id) else None
                records.append({
                    "Cầu thủ": r.get("player_name"),
                    "Đội bóng": r.get("team_name"),
                    "Logo": logo_url,
                    "Điểm Rating": float(r.get("rating"))
                })
            return pd.DataFrame(records)
            
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching top players from DWH: {e}")
        return pd.DataFrame()
