POSITION_CONFIG = {
    "ST": {
        "weights": {
            "base_rating": 0.30,
            "goals_p90": 0.25,
            "xg_p90": 0.15,
            "shots_on_target_p90": 0.10,
            "goal_conversion_pct": 0.10,
            "big_chances_created_p90": 0.10
        },
        "penalties": {
            "possession_lost": 0.05,
            "big_chances_missed": 0.05
        }
    },
    "CF": {
        "weights": {
            "base_rating": 0.30,
            "goals_p90": 0.25,
            "xg_p90": 0.15,
            "shots_on_target_p90": 0.10,
            "goal_conversion_pct": 0.10,
            "big_chances_created_p90": 0.10
        },
        "penalties": {
            "possession_lost": 0.05,
            "big_chances_missed": 0.05
        }
    },
    "WING": {
        "weights": {
            "base_rating": 0.30,
            "successful_dribbles_p90": 0.20,
            "assists_p90": 0.15,
            "xa_p90": 0.15,
            "goals_p90": 0.10,
            "big_chances_created_p90": 0.10
        },
        "penalties": {
            "possession_lost": 0.05,
            "big_chances_missed": 0.05
        }
    },
    "AM": {
        "weights": {
            "base_rating": 0.30,
            "key_passes_p90": 0.20,
            "xa_p90": 0.15,
            "assists_p90": 0.15,
            "big_chances_created_p90": 0.10,
            "accurate_passes_pct": 0.10
        },
        "penalties": {
            "possession_lost": 0.05
        }
    },
    "CM": {
        "weights": {
            "base_rating": 0.30,
            "accurate_passes_pct": 0.20,
            "key_passes_p90": 0.15,
            "xa_p90": 0.15,
            "assists_p90": 0.10,
            "big_chances_created_p90": 0.10
        },
        "penalties": {
            "possession_lost": 0.05
        }
    },
    "DM": {
        "weights": {
            "base_rating": 0.30,
            "tackles_p90": 0.20,
            "interceptions_p90": 0.15,
            "ground_duels_won_pct": 0.15,
            "accurate_passes_pct": 0.10,
            "xa_key_pass_p90": 0.10  # Combined or average of xA and Key Passes
        },
        "penalties": {
            "possession_lost": 0.05
        }
    },
    "CB": {
        "weights": {
            "base_rating": 0.30,
            "interceptions_p90": 0.20,
            "tackles_p90": 0.20,
            "aerial_duels_won_pct": 0.15,
            "ground_duels_won_pct": 0.15
        },
        "penalties": {
            "errors_lead_goal_dribbled_past": 0.05  # Combined Errors Lead Goal or Dribbled Past
        }
    },
    "FB": {
        "weights": {
            "base_rating": 0.30,
            "tackles_p90": 0.20,
            "ground_duels_won_pct": 0.20,
            "interceptions_p90": 0.15,
            "assists_xa_p90": 0.15  # Combined Assists or xA
        },
        "penalties": {
            "possession_lost": 0.05
        }
    },
    "GK": {
        "weights": {
            "base_rating": 0.30,
            "saves_p90": 0.30,
            "clean_sheets_pct": 0.20,
            "aerial_duels_won_pct": 0.20
        },
        "penalties": {}
    }
}

def get_position_group(sub_position):
    """
    Map a specific sub-position to a position group for weight config.
    """
    if sub_position in ["ST", "CF", "Centre-Forward"]:
        return "ST"
    elif sub_position in ["RW", "LW", "RM", "LM", "Right Winger", "Left Winger", "Right Midfield", "Left Midfield"]:
        return "WING"
    elif sub_position in ["AM", "Attacking Midfield"]:
        return "AM"
    elif sub_position in ["CM", "Central Midfield"]:
        return "CM"
    elif sub_position in ["DM", "Defensive Midfield"]:
        return "DM"
    elif sub_position in ["CB", "Centre-Back"]:
        return "CB"
    elif sub_position in ["RB", "LB", "Right-Back", "Left-Back"]:
        return "FB"
    elif sub_position in ["GK", "Goalkeeper"]:
        return "GK"
    
    # Default fallback
    return "CM"
