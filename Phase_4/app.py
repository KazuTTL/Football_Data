import os
import sys
import streamlit as st
import pandas as pd

# Path setup to ensure local modules can be imported correctly
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.append(_THIS_DIR)

import streamlit_lucide as lucide
from utils.db import load_data
from utils.processing import apply_filters
from tabs.overview import render_overview_tab
from tabs.leaderboard import render_leaderboard_tab
from tabs.comparison import render_comparison_tab

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Moneyball Scout Dashboard",
    page_icon="⚽",
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

theme_config = {
    "BG": BG,
    "SURFACE": SURFACE,
    "SURFACE2": SURFACE2,
    "BORDER": BORDER,
    "TEXT": TEXT,
    "TEXT_SUB": TEXT_SUB,
    "ACCENT": ACCENT,
    "ACCENT2": ACCENT2,
    "WARN": WARN,
    "PLOTLY_TEMPLATE": PLOTLY_TEMPLATE,
    "CARD_BG": CARD_BG
}

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
        border-radius: 16px; padding: 18px 22px;
        display: flex;
        align-items: center;
        gap: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
    }}
    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0,0,0,0.1);
    }}
    .metric-icon-wrapper {{
        width: 48px;
        height: 48px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
    }}
    .metric-content {{
        display: flex;
        flex-direction: column;
    }}
    .metric-val {{
        font-size: 1.7rem; font-weight: 700; color: {TEXT};
        line-height: 1.2;
        margin: 0;
    }}
    .metric-lbl {{
        font-size: 0.72rem; font-weight: 600; color: {TEXT_SUB};
        text-transform: uppercase; letter-spacing: 0.05em;
        margin: 0;
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
    /* Muted tags for MultiSelect */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {SURFACE2} !important;
        border: 1px solid {BORDER} !important;
        border-radius: 6px !important;
        color: {TEXT} !important;
    }}
    .stMultiSelect [data-baseweb="tag"] span[title] {{
        color: {TEXT} !important;
    }}
    .stMultiSelect [data-baseweb="tag"] svg {{
        fill: {TEXT_SUB} !important;
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

    /* st.container(border=True) customized as premium cards */
    div[data-testid="stVerticalBlockBorderWrapper"] {{
        background-color: {SURFACE} !important;
        border: 1px solid {BORDER} !important;
        border-radius: 16px !important;
        padding: 24px !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05) !important;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    div[data-testid="stVerticalBlockBorderWrapper"]:hover {{
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.08) !important;
    }}
</style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
df_star, df_rating, df_history = load_data()

if df_star.empty:
    st.warning("Không thể tải dữ liệu. Vui lòng kiểm tra kết nối.")
    st.stop()

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
    
    # Latest crawl filter
    latest_only = st.checkbox("Chỉ hiển thị cầu thủ thuộc lần cào gần nhất", value=False)

    st.markdown("---")
    if st.button("Làm mới dữ liệu (Cache)", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# --- Apply filters ---
fdf = apply_filters(df_star, sel_leagues, sel_positions, sel_teams, max_mv_filter, min_score, latest_only)

# ============================
# MAIN LAYOUT: TABS
# ============================
icon_trend_big = lucide.get_icon("trending-up", color=ACCENT, size=32, style="margin-right: 4px;")
st.markdown(f"<div class='hero-title'>{icon_trend_big} Moneyball Scout Dashboard</div>", unsafe_allow_html=True)
st.markdown(f"<div class='hero-sub'>Hệ thống trinh sát & đánh giá cầu thủ dựa trên mô hình DWH Gold Layer — MotherDuck Cloud</div>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Tổng quan", "Bảng xếp hạng", "So sánh chi tiết"])

with tab1:
    render_overview_tab(fdf, df_star, df_history, theme_config)

with tab2:
    render_leaderboard_tab(fdf, theme_config)

with tab3:
    render_comparison_tab(fdf, df_rating, theme_config)
