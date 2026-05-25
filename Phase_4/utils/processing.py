import pandas as pd

def apply_filters(df, sel_leagues=None, sel_positions=None, sel_teams=None, max_mv_filter=None, min_score=0.0):
    """
    Apply sidebar filters on the player season statistics dataframe.
    """
    fdf = df.copy()
    if sel_leagues:
        fdf = fdf[fdf["league"].isin(sel_leagues)]
    if sel_positions:
        fdf = fdf[fdf["position"].isin(sel_positions)]
    if sel_teams:
        fdf = fdf[fdf["team"].isin(sel_teams)]
    if max_mv_filter is not None:
        fdf = fdf[fdf["market_value"].fillna(0) <= max_mv_filter]
    fdf = fdf[fdf["scout_score"] >= min_score]
    return fdf

def format_goals(row):
    """
    Format goals value to display penalty goals if present. E.g., '10 (2 pen)'
    """
    g = int(row['goals']) if pd.notna(row['goals']) else 0
    p = int(row['penalty_goals']) if pd.notna(row['penalty_goals']) else 0
    if p > 0:
        return f"{g} ({p} pen)"
    return f"{g}"

def get_sorted_leaderboard(fdf, sort_by):
    """
    Sort leaderboard based on the user selection.
    """
    show_df = fdf.copy()
    if sort_by == "Bàn thắng":
        show_df = show_df.sort_values(["goals", "penalty_goals"], ascending=[False, True])
    elif sort_by == "Kiến tạo":
        show_df = show_df.sort_values("assists", ascending=False)
    elif sort_by == "Điểm Scout":
        show_df = show_df.sort_values("scout_score", ascending=False)
    elif sort_by == "Giá chuyển nhượng":
        show_df = show_df.sort_values("market_value", ascending=False)
    return show_df

def get_radar_vals(df_rating, fdf, pname, selected_metrics):
    """
    Calculate scaled performance values for radar chart plotting dynamically based on selected metrics.
    """
    pr = df_rating[df_rating["name"] == pname]
    ps = fdf[fdf["player_name"] == pname]
    if ps.empty:
        return [0] * len(selected_metrics)
        
    vals = []
    for m in selected_metrics:
        # Special handling for metrics in df_rating
        if m in ["scout_score", "base_score", "team_multiplier"]:
            if pr.empty:
                vals.append(0.0)
                continue
                
            if m == "scout_score":
                vals.append(float(pr.iloc[0].get("final_scout_score", 0)))
            elif m == "base_score":
                vals.append(float(pr.iloc[0].get("base_score", 0)))
            elif m == "team_multiplier":
                mult = float(pr.iloc[0].get("team_multiplier", 1.0))
                mult_scaled = max(0, min(100, ((mult - 1.0) / 0.285) * 100))
                vals.append(mult_scaled)
        else:
            # Metrics from fdf
            val = float(ps.iloc[0].get(m, 0))
            max_val = max(fdf[m].max() if pd.notna(fdf[m].max()) else 1, 1)
            # Normalize to 0-100
            vals.append(val / max_val * 100)
            
    return vals

def get_stat(df_rating, fdf, pname, metric, is_rating=False):
    """
    Retrieve specific metric value for a player.
    """
    if is_rating:
        r = df_rating[df_rating["name"] == pname]
        return float(r.iloc[0][metric]) if not r.empty and pd.notna(r.iloc[0][metric]) else 0.0
    else:
        s = fdf[fdf["player_name"] == pname]
        return float(s.iloc[0][metric]) if not s.empty and pd.notna(s.iloc[0][metric]) else 0.0
