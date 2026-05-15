import pandas as pd
from rating_engine import RatingEngine

def test_engine():
    # Create Mock Data
    data = {
        "internal_player_id": ["PLR_001", "PLR_002", "PLR_003", "PLR_004", "PLR_005"],
        "name": ["Erling Haaland", "Underdog Striker", "Small Sample Kid", "Rodri", "Ruben Dias"],
        "sub_position": ["ST", "ST", "ST", "DM", "CB"],
        "league": ["Premier League", "Premier League", "Premier League", "Premier League", "Premier League"],
        "team_name": ["Man City", "Luton Town", "Man City", "Man City", "Man City"],
        "team_rank": [1, 20, 1, 1, 1],
        "minutes_played": [3000, 2500, 500, 3100, 2900], # PLR_003 should be "Small Sample"
        "base_rating": [8.1, 7.5, 6.0, 8.0, 7.4],
        "goals": [30, 15, 2, 8, 2],
        "xg": [28.5, 14.0, 1.5, 5.0, 1.0],
        "shots_on_target": [60, 30, 5, 15, 5],
        "goal_conversion_pct": [25.0, 20.0, 10.0, 15.0, 5.0],
        "big_chances_created": [10, 5, 1, 15, 2],
        "possession_lost": [200, 150, 50, 180, 100],
        "big_chances_missed": [20, 10, 2, 2, 1],
        "tackles": [10, 15, 2, 80, 50],
        "interceptions": [5, 10, 1, 60, 80],
        "ground_duels_won_pct": [40.0, 45.0, 30.0, 65.0, 60.0],
        "accurate_passes_pct": [75.0, 70.0, 80.0, 92.0, 94.0],
        "xa_key_pass": [15, 10, 2, 40, 5], # combined feature
        "aerial_duels_won_pct": [50.0, 40.0, 30.0, 60.0, 75.0],
        "errors_lead_goal_dribbled_past": [0, 0, 0, 5, 2] # Penalty for CB
    }
    
    df = pd.DataFrame(data)
    
    engine = RatingEngine(min_minutes=900)
    result_df = engine.run(df)
    
    print("\n--- RATING ENGINE RESULTS ---")
    cols_to_print = [
        "name", "sub_position", "status", "team_rank", 
        "base_score", "penalty", "team_multiplier", "final_scout_score"
    ]
    print(result_df[cols_to_print].to_string(index=False))

if __name__ == "__main__":
    test_engine()
