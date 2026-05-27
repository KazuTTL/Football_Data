import streamlit as st
import pandas as pd
import streamlit_lucide as lucide
from utils.processing import get_radar_vals, get_stat
from utils.visual import plot_player_comparison_radar

def render_comparison_tab(fdf, df_rating, theme_config):
    """
    Renders Tab 3: Detailed player comparison tool.
    """
    icon_eye = lucide.get_icon("eye", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_eye} Soi Chiếu Chi Tiết Cầu Thủ</div>", unsafe_allow_html=True)
    
    if fdf.empty:
        st.info("Không có dữ liệu khớp với bộ lọc hiện tại để so sánh.")
        return

    comp_mode = st.radio("Chế độ so sánh:", ["Giải quốc nội", "UEFA Champions League (UCL)"], horizontal=True, key="comp_stat_mode")
    
    col_sel, col_chart = st.columns([1, 2], gap="medium")

    with col_sel:
        with st.container(border=True):
            st.markdown("<div style='font-size: 0.95rem; font-weight: 700; margin-bottom: 12px;'>Lựa chọn Cầu thủ</div>", unsafe_allow_html=True)
            if comp_mode == "UEFA Champions League (UCL)":
                # Only players with UCL statistics
                ucl_players_df = fdf[fdf['rating_cl'].notna() | (fdf['goals_cl'] > 0) | (fdf['assists_cl'] > 0)]
                all_names = sorted(ucl_players_df["player_name"].dropna().unique().tolist())
            else:
                all_names = sorted(fdf["player_name"].dropna().unique().tolist())
                
            if not all_names:
                st.info("Không có cầu thủ nào phù hợp để so sánh.")
                return
                
            player_a = st.selectbox("Cầu thủ A", all_names, index=0, key="pa")
            player_b = st.selectbox("Cầu thủ B", all_names, index=min(1, len(all_names)-1), key="pb")

        # Determine colors dynamically to ensure consistency with the radar chart
        color_a = theme_config["ACCENT"]
        is_warm = False
        if color_a.lower().startswith("#ff") or color_a.lower().startswith("#d2") or color_a.lower().startswith("#ff5757"):
            is_warm = True
            
        if is_warm:
            color_b = "#00e57a" # Neo brutalist green
        else:
            color_b = "#ff5757" # Neo brutalist red

        # Show quick stats cards
        for label, pname, color, icon_name in [
            ("Cầu Thủ A", player_a, color_a, "shield"),
            ("Cầu Thủ B", player_b, color_b, "award")
        ]:
            ps = fdf[fdf["player_name"] == pname]
            if not ps.empty:
                ps = ps.iloc[0]
                mv_str = f"{ps['market_value']/1_000_000:.1f}M€" if pd.notna(ps.get('market_value')) else "N/A"
                icon_html = lucide.get_icon(icon_name, color=color, size=16, style="margin-right: 6px;")
                
                if comp_mode == "UEFA Champions League (UCL)":
                    rating_val = ps.get('rating_cl', 0.0)
                    rating_str = f"{rating_val:.2f}" if pd.notna(rating_val) else "N/A"
                    score_lbl = "UCL Rating"
                    goals_val = ps.get('goals_cl', 0)
                    assists_val = ps.get('assists_cl', 0)
                else:
                    pr = df_rating[df_rating["name"] == pname]
                    score_val = pr.iloc[0]['final_scout_score'] if not pr.empty else 0.0
                    rating_str = f"{score_val:.2f}"
                    score_lbl = "Điểm Scout"
                    goals_val = ps.get('goals', 0)
                    assists_val = ps.get('assists', 0)
                
                card_html = f"""
                <div style='
                    margin-top:12px;
                    background: {theme_config["SURFACE"]};
                    border: 1px solid {theme_config["BORDER"]};
                    border-left: 4px solid {color};
                    border-radius: 12px;
                    padding: 14px 18px;
                    box-shadow: 0 4px 10px rgba(0,0,0,0.03);
                '>
                    <div style='display:flex; align-items:center; font-weight:700; color:{color}; margin-bottom:6px; font-size: 0.92rem;'>
                        {icon_html} {label}: {pname}
                    </div>
                    <div style='color:{theme_config['TEXT_SUB']}; font-size:0.8rem; margin-bottom: 6px;'>
                        {ps.get('position','?')} · {ps.get('team_cl', ps.get('team','?'))}
                    </div>
                    <div style='margin-top:4px; font-size:0.85rem; color: {theme_config['TEXT']};'>
                        {score_lbl}: <b style='color:{color}'>{rating_str}</b> &nbsp;·&nbsp; Giá trị: {mv_str}
                    </div>
                    <div style='font-size:0.8rem; color:{theme_config['TEXT_SUB']}; margin-top: 4px;'>
                        Hiệu suất: {int(goals_val)} bàn &nbsp;·&nbsp; {int(assists_val)} kiến tạo
                    </div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)
                
    with col_chart:
        with st.container(border=True):
            # Define available metrics for Radar dynamically
            if comp_mode == "UEFA Champions League (UCL)":
                RADAR_METRICS = {
                    "UCL Rating": "rating_cl",
                    "Bàn thắng": "goals_cl",
                    "Kiến tạo": "assists_cl",
                    "Bàn kỳ vọng (xG)": "xg_cl",
                    "Kiến tạo kỳ vọng (xA)": "xa_cl",
                    "Đường chuyền quyết định": "key_passes_cl",
                    "Qua người t.công": "successful_dribbles_cl",
                    "Tắc bóng": "tackles_cl",
                    "Cắt bóng": "interceptions_cl",
                    "Phá bóng": "clearances_cl",
                    "Cứu thua": "saves_cl",
                    "Giữ sạch lưới": "clean_sheets_cl",
                    "Tỉ lệ chuyền c.xác (%)": "accurate_passes_pct_cl",
                    "Tranh chấp trên không (%)": "aerial_duels_won_pct_cl",
                    "Tranh chấp tay đôi (%)": "ground_duels_won_pct_cl"
                }
                default_metrics = ["UCL Rating", "Bàn thắng", "Kiến tạo", "Đường chuyền quyết định", "Qua người t.công"]
            else:
                RADAR_METRICS = {
                    "Điểm Scout (0-100)": "scout_score",
                    "Điểm Gốc (Base Score)": "base_score",
                    "Hệ số gánh team": "team_multiplier",
                    "Bàn thắng": "goals",
                    "Kiến tạo": "assists",
                    "Bàn kỳ vọng (xG)": "xg",
                    "Kiến tạo kỳ vọng (xA)": "xa",
                    "Đường chuyền quyết định": "key_passes",
                    "Qua người t.công": "successful_dribbles",
                    "Tắc bóng": "tackles",
                    "Cắt bóng": "interceptions",
                    "Phá bóng": "clearances",
                    "Cứu thua": "saves",
                    "Giữ sạch lưới": "clean_sheets",
                    "Bàn thua tránh được": "goals_prevented",
                    "Tỉ lệ chuyền c.xác (%)": "accurate_passes_pct",
                    "Tranh chấp trên không (%)": "aerial_duels_won_pct",
                    "Tranh chấp tay đôi (%)": "ground_duels_won_pct"
                }
                default_metrics = ["Điểm Scout (0-100)", "Bàn thắng", "Kiến tạo", "Đường chuyền quyết định", "Qua người t.công"]
            # Determine position groups for smart mapping
            pos_a = str(fdf[fdf['player_name'] == player_a]['position'].iloc[0]).upper().strip() if not fdf[fdf['player_name'] == player_a].empty else ""
            pos_b = str(fdf[fdf['player_name'] == player_b]['position'].iloc[0]).upper().strip() if not fdf[fdf['player_name'] == player_b].empty else ""

            def get_pos_group(pos):
                if pos in ["ST", "CF", "LW", "RW", "LF", "RF", "CENTRE-FORWARD", "LEFT WINGER", "RIGHT WINGER", "SECOND STRIKER"]: return "ATT"
                if pos in ["CAM", "CM", "CDM", "LM", "RM", "ATTACKING MIDFIELD", "CENTRAL MIDFIELD", "DEFENSIVE MIDFIELD", "RIGHT MIDFIELD", "LEFT MIDFIELD"]: return "MID"
                if pos in ["CB", "LB", "RB", "LWB", "RWB", "CENTRE-BACK", "LEFT-BACK", "RIGHT-BACK"]: return "DEF"
                if pos in ["GK", "GOALKEEPER"]: return "GK"
                return "UNKNOWN"

            grp_a = get_pos_group(pos_a)
            grp_b = get_pos_group(pos_b)

            if grp_a == grp_b and grp_a != "UNKNOWN":
                if grp_a == "ATT":
                    auto_metrics = ["Bàn thắng", "Kiến tạo", "Bàn kỳ vọng (xG)", "Đường chuyền quyết định", "Qua người t.công"]
                elif grp_a == "MID":
                    auto_metrics = ["Kiến tạo", "Đường chuyền quyết định", "Tỉ lệ chuyền c.xác (%)", "Cắt bóng", "Qua người t.công"]
                elif grp_a == "DEF":
                    auto_metrics = ["Tắc bóng", "Cắt bóng", "Phá bóng", "Tranh chấp trên không (%)", "Tranh chấp tay đôi (%)"]
                elif grp_a == "GK":
                    auto_metrics = ["Cứu thua", "Giữ sạch lưới", "Tỉ lệ chuyền c.xác (%)", "Tranh chấp trên không (%)"]
                
                default_metrics = [m for m in auto_metrics if m in RADAR_METRICS]

            st.markdown(f"<div style='font-size: 0.95rem; font-weight: 600; margin-bottom: 8px;'>Tùy chỉnh thông số Radar Chart</div>", unsafe_allow_html=True)
            advanced_mode = st.checkbox("Chế độ so sánh tự do (Advanced)", value=False, help="Cho phép tự chọn các chỉ số tùy ý để so sánh chéo.")
            
            selected_display_names = st.multiselect(
                "Chọn tối đa 5-6 chỉ số để so sánh:", 
                options=list(RADAR_METRICS.keys()),
                default=default_metrics,
                max_selections=7,
                disabled=not advanced_mode,
                label_visibility="collapsed"
            )
            if not selected_display_names:
                st.warning("Vui lòng chọn ít nhất 1 chỉ số để hiển thị Radar Chart.")
            else:
                cats = selected_display_names
                selected_cols = [RADAR_METRICS[name] for name in cats]
                
                vals_a = get_radar_vals(df_rating, fdf, player_a, selected_cols)
                vals_b = get_radar_vals(df_rating, fdf, player_b, selected_cols)
                
                fig_radar = plot_player_comparison_radar(
                    player_a, player_b, cats, vals_a, vals_b,
                    theme_config["PLOTLY_TEMPLATE"], theme_config["TEXT"],
                    theme_config["TEXT_SUB"], theme_config["BORDER"], theme_config["ACCENT"]
                )
                st.plotly_chart(fig_radar, use_container_width=True)

    # ------------------
    # COMPARISON TABLE
    # ------------------
    st.markdown("<br>", unsafe_allow_html=True)
    with st.container(border=True):
        icon_scale = lucide.get_icon("scale", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_scale} Bảng Đối Chiếu Chỉ Số</div>", unsafe_allow_html=True)
        
        def format_diff(val, invert_color=False):
            if val > 0: 
                return f"<span style='color: {'#f87171' if invert_color else '#3fb950'}; font-weight: bold;'>+{val:.2f}</span>"
            elif val < 0: 
                return f"<span style='color: {'#3fb950' if invert_color else '#f87171'}; font-weight: bold;'>{val:.2f}</span>"
            return f"<span style='color: {theme_config['TEXT_SUB']};'>0.00</span>"

        if comp_mode == "UEFA Champions League (UCL)":
            metrics = [
                ("UCL Rating (0-10)", "rating_cl", False, False),
                ("Bàn thắng UCL", "goals_cl", False, False),
                ("Bàn Penalty UCL", "penalty_goals_cl", False, True),
                ("Kiến tạo UCL", "assists_cl", False, False),
                ("Bàn kỳ vọng (xG UCL)", "xg_cl", False, False),
                ("Kiến tạo kỳ vọng (xA UCL)", "xa_cl", False, False),
                ("Đường chuyền quyết định UCL", "key_passes_cl", False, False),
                ("Tắc bóng UCL", "tackles_cl", False, False),
                ("Cứu thua UCL", "saves_cl", False, False),
            ]
        else:
            metrics = [
                ("Scout Score (0-100)", "final_scout_score", True, False),
                ("Base Score (0-100)", "base_score", True, False),
                ("Penalty (Trừ điểm)", "penalty", True, True),  # Invert color because lower penalty is better
                ("Hệ số Gánh team (Multiplier)", "team_multiplier", True, False),
                ("Số Bàn thắng", "goals", False, False),
                ("Số Bàn Penalty", "penalty_goals", False, True), # Invert color: lower penalty goals is better when equal goals
                ("Số Kiến tạo", "assists", False, False),
            ]
        
        table_html = f"<table style='width:100%; text-align:left; border-collapse: collapse; font-size: 0.95rem;'>"
        table_html += f"<tr style='border-bottom: 2px solid {theme_config['BORDER']};'><th style='padding: 10px;'>Chỉ số</th><th style='padding: 10px; color: {color_a};'>{player_a} (A)</th><th style='padding: 10px; color: {color_b};'>{player_b} (B)</th><th style='padding: 10px;'>Hiệu số (A - B)</th></tr>"
        
        for label, col_name, is_rating, invert_color in metrics:
            val_a = get_stat(df_rating, fdf, player_a, col_name, is_rating)
            val_b = get_stat(df_rating, fdf, player_b, col_name, is_rating)
            diff = val_a - val_b
            
            diff_str = format_diff(diff, invert_color)
                
            table_html += f"<tr style='border-bottom: 1px solid {theme_config['BORDER']};'><td style='padding: 10px; font-weight: 500;'>{label}</td><td style='padding: 10px;'>{val_a:.2f}</td><td style='padding: 10px;'>{val_b:.2f}</td><td style='padding: 10px;'>{diff_str}</td></tr>"
            
        table_html += "</table>"
        st.markdown(f"<div style='background: transparent; padding: 0px; overflow-x: auto;'>{table_html}</div>", unsafe_allow_html=True)
