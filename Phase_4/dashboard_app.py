import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit_lucide as lucide  # Import local Lucide icons library

# --- PATH SETUP ---
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(_THIS_DIR)
star_schema_dir = os.path.join(project_root, "Phase_3_Gold", "star_schema")
if star_schema_dir not in sys.path:
    sys.path.append(star_schema_dir)

try:
    # pyrefly: ignore [missing-import]
    from db_connection import get_motherduck_connection
except ImportError:
    st.error("Không thể import db_connection. Vui lòng kiểm tra cấu hình.")
    st.stop()

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Moneyball Scout Dashboard",
    page_icon="⚽",  # Browser tab icon remains standard
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- THEME INIT ---
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True
if "selected_player" not in st.session_state:
    st.session_state.selected_player = None

dark = st.session_state.dark_mode

# --- THEME COLORS ---
if dark:
    BG        = "#0d1117"
    SURFACE   = "#161b22"
    SURFACE2  = "#21262d"
    BORDER    = "#30363d"
    TEXT      = "#e6edf3"
    TEXT_SUB  = "#8b949e"
    ACCENT    = "#58a6ff"
    ACCENT2   = "#3fb950"
    WARN      = "#d29922"
    PLOTLY_TEMPLATE = "plotly_dark"
    CARD_BG   = "#161b22"
else:
    BG        = "#f6f8fa"
    SURFACE   = "#ffffff"
    SURFACE2  = "#f0f3f6"
    BORDER    = "#d0d7de"
    TEXT      = "#1f2328"
    TEXT_SUB  = "#656d76"
    ACCENT    = "#0969da"
    ACCENT2   = "#1a7f37"
    WARN      = "#9a6700"
    PLOTLY_TEMPLATE = "plotly_white"
    CARD_BG   = "#ffffff"

# --- DYNAMIC CSS ---
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"], .stApp {{
        font-family: 'Inter', sans-serif;
        background-color: {BG} !important;
        color: {TEXT} !important;
    }}
    .stApp {{ background-color: {BG} !important; }}

    section[data-testid="stSidebar"] > div {{
        background-color: {SURFACE} !important;
        border-right: 1px solid {BORDER};
    }}
    section[data-testid="stSidebar"] * {{ color: {TEXT} !important; }}

    .hero-title {{
        font-size: 2rem; font-weight: 800;
        color: {ACCENT}; margin-bottom: 0.2rem; line-height: 1.2;
        display: flex;
        align-items: center;
        gap: 12px;
    }}
    .hero-sub {{
        font-size: 0.95rem; color: {TEXT_SUB}; margin-bottom: 1.5rem;
    }}
    .metric-card {{
        background: {SURFACE}; border: 1px solid {BORDER};
        border-radius: 12px; padding: 16px 20px;
        text-align: center; height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }}
    .metric-val {{
        font-size: 1.9rem; font-weight: 700; color: {ACCENT};
        margin-top: 4px;
    }}
    .metric-lbl {{
        font-size: 0.78rem; font-weight: 600; color: {TEXT_SUB};
        text-transform: uppercase; letter-spacing: 0.05em; margin-top: 4px;
    }}
    .section-title {{
        font-size: 1.05rem; font-weight: 700; color: {TEXT};
        margin-bottom: 12px; padding-bottom: 8px;
        border-bottom: 2px solid {ACCENT};
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    div[data-testid="stDataFrame"] {{
        background: {SURFACE} !important;
        border-radius: 10px; border: 1px solid {BORDER};
    }}
    .stSelectbox label, .stMultiSelect label, .stSlider label {{
        color: {TEXT} !important; font-weight: 500;
    }}
    .stButton > button {{
        background: {SURFACE2} !important; color: {TEXT} !important;
        border: 1px solid {BORDER} !important; border-radius: 8px !important;
        font-weight: 600 !important; font-size: 0.85rem !important;
        transition: all 0.2s;
    }}
    .stButton > button:hover {{
        border-color: {ACCENT} !important;
        color: {ACCENT} !important;
    }}
    div[data-testid="stMetricValue"] {{ color: {ACCENT} !important; }}
    div[data-testid="stMetricLabel"] {{ color: {TEXT_SUB} !important; }}
    .stTabs [data-baseweb="tab-list"] {{ background: {SURFACE2}; border-radius: 10px; gap: 4px; }}
    .stTabs [data-baseweb="tab"] {{ color: {TEXT_SUB} !important; border-radius: 8px; font-weight: 500; }}
    .stTabs [aria-selected="true"] {{ background: {ACCENT} !important; color: white !important; }}
</style>
""", unsafe_allow_html=True)


# --- DATA LOADING ---
@st.cache_data(ttl=600)
def load_data():
    try:
        conn = get_motherduck_connection()

        # Join with silver_players to get penalty_goals_sfs and team_rank_sfs
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
                ROUND(f.final_scout_score, 2)   AS scout_score
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
        
        conn.close()
        
        return df_star, df_rating, df_history
    except Exception as e:
        st.error(f"Lỗi kết nối MotherDuck: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


df_star, df_rating, df_history = load_data()

if df_star.empty:
    st.warning("Không thể tải dữ liệu. Vui lòng kiểm tra kết nối.")
    st.stop()

# Merge market_value into rating df for radar display compatibility
if "market_value" not in df_rating.columns and not df_star.empty:
    mv_map = df_star[["internal_player_id", "market_value", "league", "team"]].drop_duplicates("internal_player_id")
    df_rating = df_rating.merge(mv_map, on="internal_player_id", how="left")

# ============================
# SIDEBAR FILTERS
# ============================
with st.sidebar:
    # Theme toggle
    icon_theme = lucide.get_icon("moon" if dark else "sun", color=ACCENT, size=20, style="margin-right: 8px;")
    st.markdown(f"<div style='display: flex; align-items: center; margin-bottom: 12px;'>{icon_theme}<span style='font-weight: 600; font-size: 0.95rem;'>Chế độ hiển thị: {'Tối' if dark else 'Sáng'}</span></div>", unsafe_allow_html=True)
    
    if st.button("Chuyển Chế Độ Theme", use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

    st.markdown("---")
    
    # Header Control Panel
    icon_sliders = lucide.get_icon("sliders", color=ACCENT, size=18, style="margin-right: 8px;")
    st.markdown(f"<div style='display: flex; align-items: center; font-size: 1.1rem; font-weight: 700; color: {ACCENT};'>{icon_sliders} Bảng Điều Khiển Lọc</div>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # League filter
    all_leagues = sorted(df_star["league"].dropna().unique().tolist())
    sel_leagues = st.multiselect("Giải đấu", all_leagues, default=all_leagues)

    # Position filter
    all_positions = sorted(df_star["position"].dropna().unique().tolist())
    sel_positions = st.multiselect("Vị trí thi đấu", all_positions, default=all_positions)

    # Team filter
    available_teams = sorted(df_star[df_star["league"].isin(sel_leagues) if sel_leagues else True]["team"].dropna().unique().tolist())
    sel_teams = st.multiselect("Đội bóng", available_teams, default=[])

    # Market value filter
    max_val_raw = df_star["market_value"].dropna()
    if len(max_val_raw) > 0:
        max_mv = float(max_val_raw.max())
        mv_range = st.slider(
            "Giá trị tối đa (triệu €)",
            min_value=0.0,
            max_value=round(max_mv / 1_000_000, 1),
            value=round(max_mv / 1_000_000, 1),
            step=0.5
        )
        max_mv_filter = mv_range * 1_000_000
    else:
        max_mv_filter = None

    # Scout Score filter
    min_score = st.slider("Điểm Scout tối thiểu", 0.0, 100.0, 0.0, 1.0)

    st.markdown("---")
    if st.button("Làm mới dữ liệu (Cache)", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# --- Apply filters ---
fdf = df_star.copy()
if sel_leagues:
    fdf = fdf[fdf["league"].isin(sel_leagues)]
if sel_positions:
    fdf = fdf[fdf["position"].isin(sel_positions)]
if sel_teams:
    fdf = fdf[fdf["team"].isin(sel_teams)]
if max_mv_filter is not None:
    fdf = fdf[fdf["market_value"].fillna(0) <= max_mv_filter]
fdf = fdf[fdf["scout_score"] >= min_score]


# ============================
# MAIN LAYOUT: TABS
# ============================
icon_trend_big = lucide.get_icon("trending-up", color=ACCENT, size=32, style="margin-right: 4px;")
st.markdown(f"<div class='hero-title'>{icon_trend_big} Moneyball Scout Dashboard</div>", unsafe_allow_html=True)
st.markdown(f"<div class='hero-sub'>Hệ thống trinh sát & đánh giá cầu thủ dựa trên mô hình DWH Gold Layer — MotherDuck Cloud</div>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📊 Tổng quan DWH", "🏆 Bảng xếp hạng Scout", "⚔️ So sánh cầu thủ"])


# ----------------------------------------
# TAB 1: TỔNG QUAN DWH
# ----------------------------------------
with tab1:
    c1, c2, c3, c4 = st.columns(4)
    def metric_card(col, val, lbl, icon_name):
        icon_svg = lucide.get_icon(icon_name, color=ACCENT, size=22, style="margin-bottom: 6px;")
        col.markdown(f"<div class='metric-card'><div>{icon_svg}</div><div class='metric-val'>{val}</div><div class='metric-lbl'>{lbl}</div></div>", unsafe_allow_html=True)

    metric_card(c1, len(fdf), "Cầu thủ", "users")
    metric_card(c2, fdf["team"].nunique(), "Câu lạc bộ", "shield")
    metric_card(c3, fdf["league"].nunique(), "Giải đấu", "globe")
    metric_card(c4, f"{fdf['scout_score'].mean():.1f}", "Điểm TB", "star")

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row
    col_bar, col_pie = st.columns([2, 1], gap="large")

    with col_bar:
        icon_bar = lucide.get_icon("bar-chart-2", color=ACCENT, size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_bar} Phân bổ Cầu thủ theo Câu lạc bộ</div>", unsafe_allow_html=True)
        
        club_counts = fdf['team'].value_counts().reset_index()
        club_counts.columns = ['Câu lạc bộ', 'Số lượng cầu thủ']
        
        top_n_choice = st.selectbox("Hiển thị Câu lạc bộ:", ["Top 5", "Top 10", "Tất cả"], key="top_n_bar")
        if top_n_choice == "Top 5":
            club_counts = club_counts.head(5)
        elif top_n_choice == "Top 10":
            club_counts = club_counts.head(10)
            
        fig_bar = px.bar(
            club_counts, x='Câu lạc bộ', y='Số lượng cầu thủ', text='Số lượng cầu thủ',
            template=PLOTLY_TEMPLATE, color_discrete_sequence=[ACCENT]
        )
        fig_bar.update_traces(textposition='outside')
        fig_bar.update_layout(height=350, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_pie:
        icon_pie = lucide.get_icon("pie-chart", color=ACCENT, size=18, style="margin-right: 4px;")
        st.markdown(f"<div class='section-title'>{icon_pie} Tỷ lệ Vị trí Thi đấu</div>", unsafe_allow_html=True)
        
        pos_counts = fdf['position'].value_counts().reset_index()
        pos_counts.columns = ['Vị trí', 'Số lượng']
        fig_pie = px.pie(
            pos_counts, names='Vị trí', values='Số lượng', hole=0.45,
            template=PLOTLY_TEMPLATE, color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(height=350, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig_pie, use_container_width=True)
        
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Team Rankings Row
    icon_list = lucide.get_icon("list-ordered", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_list} Bảng xếp hạng Đội bóng</div>", unsafe_allow_html=True)
    
    team_ranks = df_star[['league', 'team', 'team_rank']].drop_duplicates().dropna(subset=['team_rank']).sort_values(['league', 'team_rank'])
    leagues_avail = sorted(team_ranks['league'].unique())
    if len(leagues_avail) > 0:
        cols = st.columns(len(leagues_avail))
        for i, l in enumerate(leagues_avail):
            with cols[i]:
                st.markdown(f"**{l}**")
                l_df = team_ranks[team_ranks['league'] == l]
                # Reset index for clean display
                l_df = l_df[['team_rank', 'team']].set_index('team_rank')
                l_df.index.name = "Hạng"
                l_df.columns = ["Câu lạc bộ"]
                st.dataframe(l_df, use_container_width=True, height=250)
    else:
        st.info("Chưa có dữ liệu bảng xếp hạng câu lạc bộ.")

    st.markdown("<br>", unsafe_allow_html=True)
    
    # Market Value History Line Chart
    icon_line = lucide.get_icon("line-chart", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_line} Biểu đồ Biến động Giá chuyển nhượng (SCD2)</div>", unsafe_allow_html=True)
    
    all_players_hist = sorted(df_history['name'].unique().tolist())
    selected_hist_player = st.selectbox("Cuộn và chọn cầu thủ để xem lịch sử biến động giá:", all_players_hist)
    
    if selected_hist_player:
        player_hist = df_history[df_history['name'] == selected_hist_player].copy()
        if len(player_hist) > 0:
            player_hist['valid_from'] = pd.to_datetime(player_hist['valid_from'])
            player_hist['Giá trị (Triệu €)'] = player_hist['current_market_value'] / 1_000_000
            
            fig_line = px.line(
                player_hist, 
                x='valid_from', y='Giá trị (Triệu €)', 
                markers=True,
                labels={'valid_from': 'Ngày cập nhật', 'Giá trị (Triệu €)': 'Giá trị (Triệu €)'},
                template=PLOTLY_TEMPLATE
            )
            fig_line.update_traces(line_color=ACCENT2, marker=dict(size=8))
            fig_line.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_line, use_container_width=True)


# ----------------------------------------
# TAB 2: BẢNG XẾP HẠNG SCOUT
# ----------------------------------------
with tab2:
    icon_medal = lucide.get_icon("medal", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_medal} Bảng Xếp Hạng Tuyển Trạch</div>", unsafe_allow_html=True)
    
    sort_col, _ = st.columns([1, 2])
    with sort_col:
        sort_by = st.selectbox("Tiêu chí Sắp xếp ưu tiên:", ["Điểm Scout", "Bàn thắng", "Kiến tạo", "Giá chuyển nhượng"])
        
    show_df = fdf.copy()
    
    # Custom Sorting Logic
    if sort_by == "Bàn thắng":
        # Sắp xếp số bàn thắng giảm dần. Nếu bằng nhau, penalty ít hơn sẽ xếp trên (penalty_goals tăng dần)
        show_df = show_df.sort_values(["goals", "penalty_goals"], ascending=[False, True])
    elif sort_by == "Kiến tạo":
        show_df = show_df.sort_values("assists", ascending=False)
    elif sort_by == "Điểm Scout":
        show_df = show_df.sort_values("scout_score", ascending=False)
    elif sort_by == "Giá chuyển nhượng":
        show_df = show_df.sort_values("market_value", ascending=False)
        
    # Format Bàn thắng (kèm penalty)
    def format_goals(row):
        g = int(row['goals']) if pd.notna(row['goals']) else 0
        p = int(row['penalty_goals']) if pd.notna(row['penalty_goals']) else 0
        if p > 0:
            return f"{g} ({p} pen)"
        return f"{g}"
        
    show_df["Bàn thắng (Pen)"] = show_df.apply(format_goals, axis=1)
    
    display_cols = ["player_name", "position", "team", "league", "Bàn thắng (Pen)", "assists", "scout_score", "market_value"]
    show_df = show_df[[c for c in display_cols if c in show_df.columns]]
    show_df = show_df.rename(columns={
        "player_name": "Cầu thủ", "position": "Vị trí", "team": "Đội bóng",
        "league": "Giải đấu", "assists": "Kiến tạo",
        "scout_score": "Scout Score", "market_value": "Giá trị (€)"
    })
    
    show_df = show_df.reset_index(drop=True)
    show_df.index += 1

    st.dataframe(
        show_df,
        column_config={
            "Scout Score": st.column_config.ProgressColumn(
                "Scout Score", format="%.2f", min_value=0, max_value=100
            ),
            "Giá trị (€)": st.column_config.NumberColumn(
                "Giá trị thị trường", format="€%.0f"
            ),
        },
        hide_index=False,
        width="stretch",
        height=600,
    )


# ----------------------------------------
# TAB 3: SO SÁNH CẦU THỦ
# ----------------------------------------
with tab3:
    icon_eye = lucide.get_icon("eye", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_eye} Soi Chiếu Chi Tiết Cầu Thủ</div>", unsafe_allow_html=True)
    
    col_sel, col_chart = st.columns([1, 2], gap="medium")

    with col_sel:
        all_names = sorted(fdf["player_name"].dropna().unique().tolist())
        player_a = st.selectbox("Cầu thủ A", all_names, index=0, key="pa")
        player_b = st.selectbox("Cầu thủ B", all_names, index=min(1, len(all_names)-1), key="pb")

        # Show quick stats cards
        for label, pname, color, icon_name in [
            ("Cầu Thủ A", player_a, ACCENT, "shield"),
            ("Cầu Thủ B", player_b, "#f87171", "award")
        ]:
            pr = df_rating[df_rating["name"] == pname]
            ps = fdf[fdf["player_name"] == pname]
            if not pr.empty and not ps.empty:
                pr = pr.iloc[0]
                ps = ps.iloc[0]
                mv_str = f"{ps['market_value']/1_000_000:.1f}M€" if pd.notna(ps.get('market_value')) else "N/A"
                icon_html = lucide.get_icon(icon_name, color=color, size=16, style="margin-right: 6px;")
                card_html = f"<div class='metric-card' style='margin-top:10px;text-align:left;border-left:3px solid {color}; align-items: flex-start; padding: 12px 16px;'><div style='display:flex;align-items:center;font-weight:700;color:{color};margin-bottom:4px;'>{icon_html} {label}: {pname}</div><div style='color:{TEXT_SUB};font-size:0.8rem'>{ps.get('position','?')} · {ps.get('team','?')}</div><div style='margin-top:4px;font-size:0.85rem'>Điểm Scout: <b style='color:{color}'>{pr['final_scout_score']:.2f}</b> &nbsp;|&nbsp; Giá trị: {mv_str}</div><div style='font-size:0.8rem;color:{TEXT_SUB}'>Hiệu suất: {int(ps.get('goals',0))} bàn &nbsp;|&nbsp; {int(ps.get('assists',0))} kiến tạo</div></div>"
                st.markdown(card_html, unsafe_allow_html=True)
                
    with col_chart:
        # Radar axes
        def get_radar_vals(pname):
            pr = df_rating[df_rating["name"] == pname]
            ps = fdf[fdf["player_name"] == pname]
            if pr.empty or ps.empty:
                return [0]*5
            pr, ps = pr.iloc[0], ps.iloc[0]
            mult_scaled = max(0, min(100, ((pr.get("team_multiplier", 1.0) - 1.0) / 0.285) * 100))
            max_g = max(fdf["goals"].max(), 1)
            max_a = max(fdf["assists"].max(), 1)
            return [
                float(pr.get("base_score", 0)),
                float(mult_scaled),
                float(ps.get("goals", 0) / max_g * 100),
                float(ps.get("assists", 0) / max_a * 100),
                float(pr.get("final_scout_score", 0)),
            ]

        cats = ["Base Score", "Underdog Bonus", "Ghi bàn", "Kiến tạo", "Scout Score"]

        fig_radar = go.Figure()
        for pname, color, fill in [
            (player_a, ACCENT, f"rgba(88,166,255,0.25)"),
            (player_b, "#f87171", "rgba(248,113,113,0.25)")
        ]:
            vals = get_radar_vals(pname)
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]],
                theta=cats + [cats[0]],
                name=pname,
                fill="toself",
                line=dict(color=color, width=2),
                fillcolor=fill,
            ))

        fig_radar.update_layout(
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(visible=True, range=[0, 100],
                                tickfont=dict(color=TEXT_SUB, size=10),
                                gridcolor=BORDER),
                angularaxis=dict(tickfont=dict(color=TEXT, size=11), gridcolor=BORDER),
            ),
            showlegend=True,
            legend=dict(font=dict(color=TEXT), orientation="h", y=-0.2, x=0.5, xanchor="center"),
            template=PLOTLY_TEMPLATE,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=400,
            margin=dict(l=30, r=30, t=30, b=30),
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    # ------------------
    # COMPARISON TABLE
    # ------------------
    st.markdown(f"<div class='section-title'>Bảng Đối Chiếu Chỉ Số</div>", unsafe_allow_html=True)
    
    def format_diff(val, invert_color=False):
        # Invert color means smaller is better (e.g. Penalty)
        if val > 0: 
            return f"<span style='color: {'#f87171' if invert_color else '#3fb950'}; font-weight: bold;'>+{val:.2f}</span>"
        elif val < 0: 
            return f"<span style='color: {'#3fb950' if invert_color else '#f87171'}; font-weight: bold;'>{val:.2f}</span>"
        return f"<span style='color: {TEXT_SUB};'>0.00</span>"
        
    def get_stat(pname, metric, is_rating=False):
        if is_rating:
            r = df_rating[df_rating["name"] == pname]
            return float(r.iloc[0][metric]) if not r.empty and pd.notna(r.iloc[0][metric]) else 0.0
        else:
            s = fdf[fdf["player_name"] == pname]
            return float(s.iloc[0][metric]) if not s.empty and pd.notna(s.iloc[0][metric]) else 0.0

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
    table_html += f"<tr style='border-bottom: 2px solid {BORDER};'><th style='padding: 10px;'>Chỉ số</th><th style='padding: 10px; color: {ACCENT};'>{player_a} (A)</th><th style='padding: 10px; color: #f87171;'>{player_b} (B)</th><th style='padding: 10px;'>Hiệu số (A - B)</th></tr>"
    
    for label, col_name, is_rating, invert_color in metrics:
        val_a = get_stat(player_a, col_name, is_rating)
        val_b = get_stat(player_b, col_name, is_rating)
        diff = val_a - val_b
        
        diff_str = format_diff(diff, invert_color)
            
        table_html += f"<tr style='border-bottom: 1px solid {BORDER};'><td style='padding: 10px; font-weight: 500;'>{label}</td><td style='padding: 10px;'>{val_a:.2f}</td><td style='padding: 10px;'>{val_b:.2f}</td><td style='padding: 10px;'>{diff_str}</td></tr>"
        
    table_html += "</table>"
    st.markdown(f"<div style='background: {SURFACE2}; padding: 16px; border-radius: 10px; border: 1px solid {BORDER}; overflow-x: auto;'>{table_html}</div>", unsafe_allow_html=True)
