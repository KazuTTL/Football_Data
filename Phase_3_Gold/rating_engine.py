import pandas as pd
import numpy as np
import os
from config.position_weights import POSITION_CONFIG, get_position_group
from normalizer import apply_threshold_filter, calculate_p90, min_max_scale_by_league, calculate_underdog_bonus

class RatingEngine:
    def __init__(self, min_minutes=900):
        self.min_minutes = min_minutes

    def get_required_metrics(self):
        """Extract all unique metrics required across all positions from the config."""
        metrics_to_p90 = set()
        metrics_to_scale = set()
        
        for pos, config in POSITION_CONFIG.items():
            for metric in config.get("weights", {}).keys():
                if metric.endswith("_p90"):
                    metrics_to_p90.add(metric.replace("_p90", ""))
                metrics_to_scale.add(metric)
            
            for metric in config.get("penalties", {}).keys():
                if metric.endswith("_p90"):
                    metrics_to_p90.add(metric.replace("_p90", ""))
                metrics_to_scale.add(metric)
                
        return list(metrics_to_p90), list(metrics_to_scale)

    def calculate_scores(self, df):
        """
        Run the 4-step Rating Engine pipeline.
        """
        # Step 1: Threshold Filter
        df = apply_threshold_filter(df, self.min_minutes)
        
        # We only calculate scores for Active players
        active_mask = df["status"] == "Active"
        active_df = df[active_mask].copy()
        
        if active_df.empty:
            return df
            
        # Step 2: Per 90 Standardization
        metrics_to_p90, metrics_to_scale = self.get_required_metrics()
        
        # Compute P90 only if the raw columns exist
        existing_raw_cols = [c for c in metrics_to_p90 if c in active_df.columns]
        active_df = calculate_p90(active_df, existing_raw_cols)
        
        # Collect all metrics that actually exist now (either raw or _p90 or _pct)
        available_metrics_to_scale = [c for c in metrics_to_scale if c in active_df.columns or f"{c}_p90" in active_df.columns]
        # Wait, if metric_to_scale is already in active_df, we use it. If not, maybe it's just missing
        actual_cols_to_scale = [c for c in metrics_to_scale if c in active_df.columns]
        
        # Step 3: Min-Max Scaling (theo League)
        active_df = min_max_scale_by_league(active_df, actual_cols_to_scale)
        
        # Step 4: Final Score Calculation
        base_scores = []
        penalties = []
        team_multipliers = []
        final_scores = []
        
        for idx, row in active_df.iterrows():
            pos_group = get_position_group(row.get("sub_position", ""))
            config = POSITION_CONFIG.get(pos_group, POSITION_CONFIG["CM"])
            
            # Base Score
            base_score = 0
            for metric, weight in config["weights"].items():
                scaled_col = f"{metric}_scaled"
                if scaled_col in active_df.columns:
                    base_score += row[scaled_col] * weight
            
            # Penalty
            penalty = 0
            for metric, weight in config["penalties"].items():
                scaled_col = f"{metric}_scaled"
                if scaled_col in active_df.columns:
                    penalty += (row[scaled_col] / 100) * (weight * 100) # (Scaled / 100) * PenaltyWeight (which is say 0.05 -> 5)
                    # Actually doc says: (Scaled_PenaltyMetric / 100) * PenaltyWeight
                    # Wait, if weight is 0.05, and max penalty is 5 points.
                    # (Scaled / 100) * 5.0 -> so (row[scaled_col] / 100.0) * (weight * 100)
            
            # Team Bonus
            team_rank = row.get("team_rank", 1)
            team_multiplier = calculate_underdog_bonus(team_rank)
            
            # Final Score
            final_score = (base_score - penalty) * team_multiplier
            
            # Cap at 100 and floor at 0
            final_score = max(0.0, min(100.0, final_score))
            
            base_scores.append(base_score)
            penalties.append(penalty)
            team_multipliers.append(team_multiplier)
            final_scores.append(final_score)
            
        active_df["base_score"] = base_scores
        active_df["penalty"] = penalties
        active_df["team_multiplier"] = team_multipliers
        active_df["final_scout_score"] = final_scores
        
        # Merge back
        # For non-active players, fill with NaN
        for col in ["base_score", "penalty", "team_multiplier", "final_scout_score"]:
            if col not in df.columns:
                df[col] = np.nan
        
        df.loc[active_mask, "base_score"] = active_df["base_score"]
        df.loc[active_mask, "penalty"] = active_df["penalty"]
        df.loc[active_mask, "team_multiplier"] = active_df["team_multiplier"]
        df.loc[active_mask, "final_scout_score"] = active_df["final_scout_score"]
        
        return df

    def run(self, df):
        print(f"Starting Rating Engine for {len(df)} players...")
        result_df = self.calculate_scores(df)
        print("Rating Engine completed.")
        return result_df

if __name__ == "__main__":
    # Test script placeholder
    pass
