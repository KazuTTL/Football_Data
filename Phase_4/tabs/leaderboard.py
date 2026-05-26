import streamlit as st
import pandas as pd
import streamlit_lucide as lucide
from utils.processing import get_sorted_leaderboard, format_goals

def render_leaderboard_tab(fdf, theme_config):
    """
    Renders Tab 2: Player Leaderboard with custom sorting.
    """
    with st.container(border=True):
        icon_medal = lucide.get_icon("medal", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_medal} Bảng Xếp Hạng Tuyển Trạch</div>", unsafe_allow_html=True)
        
        if fdf.empty:
            st.info("Không có dữ liệu khớp với bộ lọc hiện tại.")
            return

        sort_col, mode_col = st.columns([1, 2])
        with sort_col:
            sort_by = st.selectbox("Tiêu chí Sắp xếp ưu tiên:", ["Điểm Scout", "Bàn thắng", "Kiến tạo", "Giá chuyển nhượng"])
        
        with mode_col:
            st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
            stat_mode = st.radio("Chế độ thống kê:", ["Giải quốc nội", "UEFA Champions League (UCL)"], horizontal=True, key="leaderboard_stat_mode")

        if stat_mode == "UEFA Champions League (UCL)":
            # Filter to players with UCL statistics
            ucl_fdf = fdf[fdf['rating_cl'].notna() | (fdf['goals_cl'] > 0) | (fdf['assists_cl'] > 0)].copy()
            if ucl_fdf.empty:
                st.info("Không có dữ liệu cúp C1 cho danh sách cầu thủ hiện tại.")
                return
                
            ucl_fdf['goals'] = ucl_fdf['goals_cl']
            ucl_fdf['penalty_goals'] = ucl_fdf['penalty_goals_cl']
            ucl_fdf['assists'] = ucl_fdf['assists_cl']
            ucl_fdf['scout_score'] = ucl_fdf['rating_cl']
            ucl_fdf['team'] = ucl_fdf['team_cl'].fillna(ucl_fdf['team'])
            
            show_df = get_sorted_leaderboard(ucl_fdf, sort_by)
            score_lbl = "UCL Rating"
            score_max = 10.0
        else:
            show_df = get_sorted_leaderboard(fdf, sort_by)
            score_lbl = "Scout Score"
            score_max = 100.0

        # Ensure penalty_goals is int
        show_df["goals"] = show_df["goals"].fillna(0).astype(int)
        show_df["penalty_goals"] = show_df["penalty_goals"].fillna(0).astype(int)
        
        display_cols = ["player_name", "position", "team", "league", "goals", "penalty_goals", "assists", "scout_score", "market_value"]
        show_df = show_df[[c for c in display_cols if c in show_df.columns]]
        show_df = show_df.rename(columns={
            "player_name": "Cầu thủ", "position": "Vị trí", "team": "Đội bóng",
            "league": "Giải đấu", "goals": "Bàn thắng", "penalty_goals": "Penalty", "assists": "Kiến tạo",
            "scout_score": score_lbl, "market_value": "Giá trị (€)"
        })
        
        show_df = show_df.reset_index(drop=True)
        show_df.index += 1

        st.dataframe(
            show_df,
            column_config={
                score_lbl: st.column_config.ProgressColumn(
                    score_lbl, format="%.2f", min_value=0.0, max_value=score_max
                ),
                "Giá trị (€)": st.column_config.NumberColumn(
                    "Giá trị thị trường", format="€%.0f"
                ),
            },
            hide_index=False,
            width="stretch",
            height=600,
        )
