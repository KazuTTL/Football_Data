import streamlit as st
import pandas as pd
import streamlit_lucide as lucide
from utils.visual import plot_club_distribution, plot_position_distribution, plot_market_value_history, plot_moneyball_scatter

def render_overview_tab(fdf, df_star, df_history, theme_config):
    """
    Renders Tab 1: DWH Overview metrics and distribution charts.
    """
    c1, c2, c3, c4 = st.columns(4)
    def metric_card(col, val, lbl, icon_name, color_hex):
        icon_svg = lucide.get_icon(icon_name, color=color_hex, size=20)
        html_code = f"""
        <div class="metric-card">
            <div class="metric-icon-wrapper" style="background-color: {color_hex}18;">
                {icon_svg}
            </div>
            <div class="metric-content">
                <div class="metric-lbl">{lbl}</div>
                <div class="metric-val">{val}</div>
            </div>
        </div>
        """
        col.markdown(html_code, unsafe_allow_html=True)

    metric_card(c1, len(fdf), "Cầu thủ", "users", theme_config["ACCENT"])
    metric_card(c2, fdf["team"].nunique() if not fdf.empty else 0, "Câu lạc bộ", "shield", theme_config["ACCENT2"])
    metric_card(c3, fdf["league"].nunique() if not fdf.empty else 0, "Giải đấu", "globe", theme_config["WARN"])
    metric_card(c4, f"{fdf['scout_score'].mean():.1f}" if not fdf.empty else "0.0", "Điểm TB", "star", "#a855f7")

    st.markdown("<br>", unsafe_allow_html=True)
    
    # --- MONEYBALL SCATTER PLOT ---
    with st.container(border=True):
        icon_target = lucide.get_icon("target", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_target} Săn Ngọc Thô (Moneyball Scatter Plot)</div>", unsafe_allow_html=True)
        
        col_mb_opt, _ = st.columns([1, 2])
        with col_mb_opt:
            score_mode = st.radio("Tiêu chí điểm số:", ["Scout Score (Mặc định)", "UCL Rating"], horizontal=True, label_visibility="collapsed")
        
        score_col = "rating_cl" if score_mode == "UCL Rating" else "scout_score"
        fig_mb = plot_moneyball_scatter(
            fdf, score_col, 
            theme_config["PLOTLY_TEMPLATE"], 
            theme_config["TEXT"], 
            theme_config["TEXT_SUB"], 
            theme_config["BORDER"], 
            theme_config["ACCENT"],
            theme_config["ACCENT2"]
        )
        
        if fig_mb:
            st.plotly_chart(fig_mb, use_container_width=True)
        else:
            st.info("Không đủ dữ liệu (Giá trị & Điểm số) để vẽ biểu đồ Moneyball.")

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row
    col_bar, col_pie = st.columns([2, 1], gap="large")

    with col_bar:
        with st.container(border=True):
            icon_bar = lucide.get_icon("bar-chart-2", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
            st.markdown(f"<div class='section-title'>{icon_bar} Phân bổ Cầu thủ theo Câu lạc bộ</div>", unsafe_allow_html=True)
            
            top_n_choice = st.selectbox("Hiển thị Câu lạc bộ:", ["Top 5", "Top 10", "Tất cả"], key="top_n_bar")
            
            if not fdf.empty:
                fig_bar = plot_club_distribution(fdf, top_n_choice, theme_config["PLOTLY_TEMPLATE"], theme_config["ACCENT"])
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Không có dữ liệu để hiển thị biểu đồ phân bổ CLB.")

    with col_pie:
        with st.container(border=True):
            icon_pie = lucide.get_icon("pie-chart", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
            st.markdown(f"<div class='section-title'>{icon_pie} Tỷ lệ Vị trí Thi đấu</div>", unsafe_allow_html=True)
            
            if not fdf.empty:
                fig_pie = plot_position_distribution(fdf, theme_config["PLOTLY_TEMPLATE"])
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Không có dữ liệu để hiển thị biểu đồ tỷ lệ vị trí.")
        
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Team Rankings Row
    with st.container(border=True):
        icon_list = lucide.get_icon("list-ordered", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_list} Bảng xếp hạng </div>", unsafe_allow_html=True)
        
        if not df_star.empty:
            leagues_avail = list(sorted(df_star['league'].dropna().unique()))
            leagues_avail.append("Champions League")
            if len(leagues_avail) > 0:
                # Default selection to "Premier League" if available
                default_idx = leagues_avail.index("Premier League") if "Premier League" in leagues_avail else 0
                selected_league = st.selectbox(
                    "Chọn giải đấu để xem bảng xếp hạng:", 
                    options=leagues_avail, 
                    index=default_idx,
                    key="standings_league_select"
                )
                
                # Lấy danh sách đội bóng có trong DB
                if selected_league == "Champions League":
                    scraped_teams = df_star['team'].unique().tolist()
                else:
                    scraped_teams = df_star[df_star['league'] == selected_league]['team'].unique().tolist()
                
                from utils.db import get_full_league_standings
                full_standings = get_full_league_standings(selected_league)
                
                if not full_standings.empty:
                    # Reset index to make Hạng a column for easier display
                    fs_display = full_standings.reset_index()
                    
                    # Style helper to bold scraped clubs
                    def highlight_scraped(row):
                        is_scraped = row['Câu lạc bộ'] in scraped_teams
                        return ['font-weight: 800; color: #58a6ff;' if is_scraped else '' for _ in row.index]
                    
                    styled_fs = fs_display.style.apply(highlight_scraped, axis=1)
                    
                    st.dataframe(
                        styled_fs,
                        column_config={
                            "Logo": st.column_config.ImageColumn("", width="small"),
                            "Hạng": st.column_config.NumberColumn("Hạng", format="%d", width="small"),
                            "Câu lạc bộ": st.column_config.TextColumn("Câu lạc bộ"),
                            "Trận": st.column_config.NumberColumn("Trận", format="%d", width="small"),
                            "T": st.column_config.NumberColumn("Thắng", format="%d", width="small"),
                            "H": st.column_config.NumberColumn("Hòa", format="%d", width="small"),
                            "B": st.column_config.NumberColumn("Thua", format="%d", width="small"),
                            "Hiệu số": st.column_config.TextColumn("Hiệu số", width="small"),
                            "Điểm": st.column_config.NumberColumn("Điểm", format="%d", width="small")
                        },
                        hide_index=True,
                        use_container_width=True,
                        height=500
                    )
                else:
                    st.info(f"Không thể lấy BXH đầy đủ cho {selected_league}.")
                    
                # UCL Top Players Section
                if selected_league == "Champions League":
                    st.markdown("<br>", unsafe_allow_html=True)
                    icon_medal = lucide.get_icon("medal", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
                    st.markdown(f"<div class='section-title'>{icon_medal} Top cầu thủ xuất sắc nhất UCL (Top Players)</div>", unsafe_allow_html=True)
                    
                    from utils.db import get_ucl_top_players
                    ucl_tops = get_ucl_top_players()
                    if not ucl_tops.empty:
                        dwh_players = df_star['player_name'].unique().tolist()
                        def highlight_dwh_players(row):
                            is_dwh = row['Cầu thủ'] in dwh_players
                            return ['font-weight: 800; color: #58a6ff;' if is_dwh else '' for _ in row.index]
                        
                        styled_tops = ucl_tops.style.apply(highlight_dwh_players, axis=1)
                        
                        st.dataframe(
                            styled_tops,
                            column_config={
                                "Logo": st.column_config.ImageColumn("", width="small"),
                                "Cầu thủ": st.column_config.TextColumn("Cầu thủ"),
                                "Đội bóng": st.column_config.TextColumn("Đội bóng"),
                                "Điểm Rating": st.column_config.NumberColumn("Rating", format="%.2f")
                            },
                            hide_index=True,
                            use_container_width=True,
                            height=400
                        )
                    else:
                        st.info("Không thể tải danh sách Top Players UCL từ API.")
            else:
                st.info("Chưa có dữ liệu giải đấu.")
        else:
            st.info("Chưa có dữ liệu bảng xếp hạng câu lạc bộ.")

    st.markdown("<br>", unsafe_allow_html=True)
    
    # Market Value History Line Chart
    with st.container(border=True):
        icon_line = lucide.get_icon("line-chart", color=theme_config["ACCENT"], size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_line} Biểu đồ Biến động Giá chuyển nhượng</div>", unsafe_allow_html=True)
        
        if not df_history.empty:
            all_players_hist = sorted(df_history['name'].unique().tolist())
            selected_hist_player = st.selectbox("Cuộn và chọn cầu thủ để xem lịch sử biến động giá:", all_players_hist)
            
            if selected_hist_player:
                fig_line = plot_market_value_history(df_history, selected_hist_player, theme_config["PLOTLY_TEMPLATE"], theme_config["ACCENT2"])
                if fig_line:
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info("Không có dữ liệu lịch sử giá của cầu thủ đã chọn.")
        else:
            st.info("Chưa có dữ liệu lịch sử giá trị chuyển nhượng.")
