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
    .gem-card {{
        background: {SURFACE2}; border: 1px solid {BORDER};
        border-radius: 10px; padding: 12px 16px; margin-bottom: 8px;
        display: flex; justify-content: space-between; align-items: center;
        cursor: pointer; transition: border-color 0.2s;
    }}
    .gem-card:hover {{ border-color: {ACCENT}; }}
    .gem-rank {{ font-size: 1.15rem; font-weight: 800; color: {ACCENT}; min-width: 32px; }}
    .gem-name {{ font-weight: 600; font-size: 0.92rem; color: {TEXT}; }}
    .gem-pos  {{ font-size: 0.75rem; color: {TEXT_SUB}; }}
    .gem-score {{ font-size: 1.1rem; font-weight: 700; color: {ACCENT2}; }}
    .gem-price {{ font-size: 0.78rem; color: {TEXT_SUB}; text-align: right; }}
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
                ROUND(f.final_scout_score, 2)   AS scout_score
            FROM fact_player_season_stats f
            LEFT JOIN dim_player     p   ON f.player_key      = p.player_key
            LEFT JOIN dim_team       t   ON f.team_key        = t.team_key
            LEFT JOIN dim_position   pos ON f.position_key    = pos.position_key
            LEFT JOIN dim_tournament tour ON f.tournament_key = tour.tournament_key
            LEFT JOIN dim_season     s   ON f.season_key      = s.season_key
        """).df()

        df_rating = conn.execute("SELECT * FROM gold_player_rating").df()
        conn.close()
        return df_star, df_rating
    except Exception as e:
        st.error(f"Lỗi kết nối MotherDuck: {e}")
        return pd.DataFrame(), pd.DataFrame()


df_star, df_rating = load_data()

if df_star.empty:
    st.warning("Không thể tải dữ liệu. Vui lòng kiểm tra kết nối.")
    st.stop()

# Merge market_value into rating df
if "market_value" not in df_rating.columns and not df_star.empty:
    mv_map = df_star[["internal_player_id", "market_value", "league", "team"]].drop_duplicates("internal_player_id")
    df_rating = df_rating.merge(mv_map, on="internal_player_id", how="left")

# ============================
# SIDEBAR
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
    st.markdown(f"<div style='display: flex; align-items: center; font-size: 1.1rem; font-weight: 700; color: {ACCENT};'>{icon_sliders} Bảng Điều Khiển</div>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # League filter
    all_leagues = sorted(df_star["league"].dropna().unique().tolist())
    sel_leagues = st.multiselect("Giải đấu", all_leagues, default=all_leagues)

    # Position filter
    all_positions = sorted(df_star["position"].dropna().unique().tolist())
    sel_positions = st.multiselect("Vị trí thi đấu", all_positions, default=all_positions)

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
if max_mv_filter is not None:
    fdf = fdf[fdf["market_value"].fillna(0) <= max_mv_filter]
fdf = fdf[fdf["scout_score"] >= min_score]

# ============================
# HEADER METRICS
# ============================
icon_trend_big = lucide.get_icon("trending-up", color=ACCENT, size=32, style="margin-right: 4px;")
st.markdown(f"<div class='hero-title'>{icon_trend_big} Moneyball Scout Dashboard</div>", unsafe_allow_html=True)
st.markdown(f"<div class='hero-sub'>Hệ thống trinh sát & đánh giá cầu thủ dựa trên mô hình DWH Gold Layer — MotherDuck Cloud</div>", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
def metric_card(col, val, lbl, icon_name):
    icon_svg = lucide.get_icon(icon_name, color=ACCENT, size=22, style="margin-bottom: 6px;")
    col.markdown(f"<div class='metric-card'><div>{icon_svg}</div><div class='metric-val'>{val}</div><div class='metric-lbl'>{lbl}</div></div>", unsafe_allow_html=True)

metric_card(c1, len(fdf), "Cầu thủ", "users")
metric_card(c2, fdf["team"].nunique(), "Câu lạc bộ", "shield")
metric_card(c3, fdf["league"].nunique(), "Giải đấu", "globe")
metric_card(c4, f"{fdf['scout_score'].mean():.1f}", "Điểm TB", "star")

st.markdown("<br>", unsafe_allow_html=True)

# ============================
# MAIN LAYOUT: Hero + Gems
# ============================
col_main, col_gems = st.columns([3, 1], gap="medium")

# ---------- HERO: Moneyball Scatter ----------
with col_main:
    icon_chart = lucide.get_icon("trending-up", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_chart} Biểu đồ Moneyball — Giá trị & Hiệu suất</div>", unsafe_allow_html=True)

    scatter_df = fdf.dropna(subset=["market_value", "scout_score"]).copy()
    scatter_df["mv_m"] = scatter_df["market_value"] / 1_000_000
    scatter_df["Cầu thủ"] = scatter_df["player_name"]

    if not scatter_df.empty:
        fig_scatter = px.scatter(
            scatter_df,
            x="mv_m",
            y="scout_score",
            color="position_group",
            size="scout_score",
            size_max=22,
            hover_name="Cầu thủ",
            hover_data={
                "team": True,
                "league": True,
                "goals": True,
                "assists": True,
                "mv_m": ":.2f",
                "scout_score": ":.2f",
                "position_group": False,
            },
            labels={
                "mv_m": "Giá trị thị trường (triệu €)",
                "scout_score": "Điểm Scout Score",
                "position_group": "Nhóm vị trí",
            },
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )

        # Highlight quadrants
        mid_x = scatter_df["mv_m"].median()
        mid_y = scatter_df["scout_score"].median()

        fig_scatter.add_hline(y=mid_y, line_dash="dot", line_color=TEXT_SUB, line_width=1,
                              annotation_text=f"Điểm trung vị ({mid_y:.1f})", annotation_font_color=TEXT_SUB)
        fig_scatter.add_vline(x=mid_x, line_dash="dot", line_color=TEXT_SUB, line_width=1,
                              annotation_text=f"Giá trung vị ({mid_x:.1f}M€)", annotation_font_color=TEXT_SUB)

        # Minimalist quadrant text annotation (no emojis)
        fig_scatter.add_annotation(
            x=0, y=1, xref="paper", yref="paper",
            text="Gems (Giá thấp - Điểm cao)",
            showarrow=False, font=dict(color=ACCENT2, size=11, weight="bold"),
            align="left", xanchor="left", yanchor="top"
        )
        fig_scatter.add_annotation(
            x=1, y=1, xref="paper", yref="paper",
            text="Stars (Giá cao - Điểm cao)",
            showarrow=False, font=dict(color=WARN, size=11, weight="bold"),
            align="right", xanchor="right", yanchor="top"
        )

        fig_scatter.update_layout(
            height=460,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=20, b=40),
            legend=dict(
                orientation="h", yanchor="bottom", y=-0.2,
                xanchor="center", x=0.5,
                font=dict(color=TEXT)
            ),
            font=dict(color=TEXT),
            xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
            yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
        )
        st.plotly_chart(fig_scatter, width="stretch")
    else:
        st.info("Không có dữ liệu để vẽ biểu đồ sau khi lọc.")

# ---------- RIGHT: Top Gems ----------
with col_gems:
    icon_gem = lucide.get_icon("gem", color=ACCENT, size=18, style="margin-right: 4px;")
    st.markdown(f"<div class='section-title'>{icon_gem} Top Ngọc Thô</div>", unsafe_allow_html=True)
    st.caption("Cầu thủ điểm cao, giá trị thấp")

    # Ngọc thô = scout_score >= median, market_value <= median
    gem_df = fdf.dropna(subset=["market_value", "scout_score"]).copy()
    if not gem_df.empty:
        med_score = gem_df["scout_score"].median()
        med_mv    = gem_df["market_value"].median()
        gems = gem_df[
            (gem_df["scout_score"] >= med_score) &
            (gem_df["market_value"] <= med_mv)
        ].sort_values("scout_score", ascending=False).head(5)

        for i, (_, row) in enumerate(gems.iterrows()):
            mv_str = f"{row['market_value']/1_000_000:.1f}M€" if row['market_value'] else "N/A"
            st.markdown(f"""
            <div class='gem-card'>
                <div>
                    <div style='display:flex;align-items:center;gap:12px'>
                        <span class='gem-rank'>#{i+1}</span>
                        <div>
                            <div class='gem-name'>{row['player_name']}</div>
                            <div class='gem-pos'>{row['position']} · {row['team']}</div>
                        </div>
                    </div>
                </div>
                <div style='text-align:right'>
                    <div class='gem-score'>{row['scout_score']:.1f}</div>
                    <div class='gem-price'>{mv_str}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Không có dữ liệu.")

# ============================
# BOTTOM: Player Drill-Down
# ============================
st.markdown("<br>", unsafe_allow_html=True)
icon_eye = lucide.get_icon("eye", color=ACCENT, size=18, style="margin-right: 4px;")
st.markdown(f"<div class='section-title'>{icon_eye} Soi Chiếu Chi Tiết Cầu Thủ</div>", unsafe_allow_html=True)

tab_radar, tab_table = st.tabs(["Biểu đồ Radar", "Bảng Xếp Hạng"])

with tab_radar:
    col_sel, col_chart = st.columns([1, 2], gap="medium")

    with col_sel:
        all_names = sorted(fdf["player_name"].dropna().unique().tolist())
        player_a = st.selectbox("Cầu thủ A", all_names, index=0, key="pa")
        player_b = st.selectbox("Cầu thủ B", all_names,
                                index=min(1, len(all_names)-1), key="pb")

        # Show quick stats cards (with customized minimalist outline icons matching player color themes)
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
            legend=dict(font=dict(color=TEXT)),
            template=PLOTLY_TEMPLATE,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=400,
            margin=dict(l=30, r=30, t=30, b=30),
            title=dict(
                text=f"Radar Comparison: {player_a} vs {player_b}",
                font=dict(color=TEXT, size=14),
                x=0.5
            )
        )
        st.plotly_chart(fig_radar, width="stretch")

with tab_table:
    display_cols = ["player_name", "position", "team", "league", "goals", "assists", "scout_score", "market_value"]
    show_df = fdf[[c for c in display_cols if c in fdf.columns]].copy()
    show_df = show_df.rename(columns={
        "player_name": "Cầu thủ", "position": "Vị trí", "team": "Đội bóng",
        "league": "Giải đấu", "goals": "Bàn thắng", "assists": "Kiến tạo",
        "scout_score": "Scout Score", "market_value": "Giá trị (€)"
    })
    show_df = show_df.sort_values("Scout Score", ascending=False).reset_index(drop=True)
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
        height=400,
    )
