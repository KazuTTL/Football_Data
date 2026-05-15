import pandas as pd
import numpy as np

def apply_threshold_filter(df, min_minutes=900):
    """
    Step 1: Threshold Filter.
    Identify players with less than min_minutes played.
    Creates a 'status' column.
    """
    df = df.copy()
    
    if "minutes_played" not in df.columns:
        raise ValueError("DataFrame must contain 'minutes_played' column")
    
    df["status"] = np.where(df["minutes_played"] < min_minutes, "Small Sample", "Active")
    return df

def calculate_p90(df, raw_metric_cols):
    """
    Step 2: Per 90 Standardization.
    Converts raw count metrics to Per 90 metrics.
    Skips percentages and base ratings.
    """
    df = df.copy()
    
    for col in raw_metric_cols:
        if col in df.columns:
            p90_col_name = f"{col}_p90"
            # Avoid division by zero
            df[p90_col_name] = np.where(
                df["minutes_played"] > 0,
                (df[col] / df["minutes_played"]) * 90,
                0
            )
            # Fill NaN with 0 just in case
            df[p90_col_name] = df[p90_col_name].fillna(0)
    
    return df

def min_max_scale_by_league(df, metric_cols):
    """
    Step 3: Min-Max Scaling (theo League).
    Scales metrics to 0-100 per league.
    """
    df = df.copy()
    
    if "league" not in df.columns:
        raise ValueError("DataFrame must contain 'league' column for scaling")
    
    for col in metric_cols:
        if col in df.columns:
            scaled_col_name = f"{col}_scaled"
            
            # Group by league and scale
            def min_max_scale(group):
                min_val = group.min()
                max_val = group.max()
                if max_val == min_val:
                    # If all values are the same, assign 0 (or could be 50, but 0 is safe)
                    return pd.Series(0, index=group.index)
                return ((group - min_val) / (max_val - min_val)) * 100
            
            df[scaled_col_name] = df.groupby("league")[col].transform(min_max_scale)
            # Ensure it's between 0 and 100 and fill NA
            df[scaled_col_name] = df[scaled_col_name].clip(0, 100).fillna(0)
            
    return df

def calculate_underdog_bonus(team_rank):
    """
    Step 4.3: Calculate Underdog Bonus.
    Team_Bonus = 1.0 + 0.015 * (Team_Rank - 1)
    E.g., rank 1 -> 1.0, rank 20 -> 1.285
    """
    # Clip team_rank to [1, 20] just in case
    rank = np.clip(team_rank, 1, 20)
    return 1.0 + 0.015 * (rank - 1)
