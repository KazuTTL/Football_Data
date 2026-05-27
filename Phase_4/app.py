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

# --- THEME COLORS (NEO BRUTALISM) ---
if dark:
    BG        = "#1a1a1a"
    SURFACE   = "#2c2c2c"
    SURFACE2  = "#3d3d3d"
    BORDER    = "#000000"
    TEXT      = "#ffffff"
    TEXT_SUB  = "#cccccc"
    ACCENT    = "#ff5757"
    ACCENT2   = "#00e57a"
    WARN      = "#ffde59"
    PLOTLY_TEMPLATE = "plotly_dark"
    CARD_BG   = "#2c2c2c"
    SHADOW    = "#000000"
else:
    BG        = "#fdf6e3"
    SURFACE   = "#ffffff"
    SURFACE2  = "#f4f4f0"
    BORDER    = "#111111"
    TEXT      = "#111111"
    TEXT_SUB  = "#555555"
    ACCENT    = "#5865F2"
    ACCENT2   = "#FF5252"
    WARN      = "#FFC300"
    PLOTLY_TEMPLATE = "plotly_white"
    CARD_BG   = "#ffffff"
    SHADOW    = "#111111"

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
    "CARD_BG": CARD_BG,
    "SHADOW": SHADOW
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
        background: {SURFACE}; border: 3px solid {BORDER};
        border-radius: 8px; padding: 18px 22px;
        display: flex;
        align-items: center;
        gap: 16px;
        box-shadow: 6px 6px 0px {SHADOW};
        transition: transform 0.1s ease, box-shadow 0.1s ease;
        height: 100%;
    }}
    .metric-card:hover {{
        transform: translate(2px, 2px);
        box-shadow: 4px 4px 0px {SHADOW};
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
        background: {SURFACE} !important; color: {TEXT} !important;
        border: 3px solid {BORDER} !important; border-radius: 8px !important;
        font-weight: 800 !important; font-size: 0.85rem !important;
        box-shadow: 4px 4px 0px {SHADOW} !important;
        text-transform: uppercase;
        transition: transform 0.1s, box-shadow 0.1s;
    }}
    .stButton > button:hover {{
        transform: translate(2px, 2px) !important;
        box-shadow: 2px 2px 0px {SHADOW} !important;
        border-color: {BORDER} !important;
        color: {ACCENT} !important;
    }}
    div[data-testid="stMetricValue"] {{ color: {ACCENT} !important; }}
    div[data-testid="stMetricLabel"] {{ color: {TEXT_SUB} !important; }}
    .stTabs [data-baseweb="tab-list"] {{ 
        background: {SURFACE2}; 
        border: 3px solid {BORDER};
        border-radius: 8px; 
        padding: 4px;
    }}
    .stTabs [data-baseweb="tab"] {{
        color: {TEXT_SUB} !important;
        border-radius: 4px;
        font-weight: 700;
        padding-left: 28px !important;
        position: relative !important;
        border: 2px solid transparent !important;
    }}
    .stTabs [aria-selected="true"] {{ 
        background: {ACCENT} !important; 
        color: white !important; 
        border: 2px solid {BORDER} !important;
        box-shadow: 2px 2px 0px {SHADOW} !important;
    }}

    /* Tab Icon styles using pseudo-elements and SVGs */
    .stTabs [data-baseweb="tab"]::before {{
        content: "" !important;
        position: absolute !important;
        left: 8px !important;
        top: 50% !important;
        transform: translateY(-50%) !important;
        width: 14px !important;
        height: 14px !important;
        background-repeat: no-repeat !important;
        background-size: contain !important;
        opacity: 0.75 !important;
        transition: opacity 0.2s;
    }}
    .stTabs [aria-selected="true"]::before {{
        opacity: 1.0 !important;
    }}

    /* Tab 1: Layout Dashboard (Overview) */
    .stTabs [data-baseweb="tab"]:nth-child(1)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="{TEXT_SUB.replace('#', '%23')}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/></svg>') !important;
    }}
    .stTabs [data-baseweb="tab"][aria-selected="true"]:nth-child(1)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%23ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/></svg>') !important;
    }}

    /* Tab 2: Trophy (Leaderboard) */
    .stTabs [data-baseweb="tab"]:nth-child(2)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="{TEXT_SUB.replace('#', '%23')}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.45 1-1 1H4v2h16v-2h-5c-.55 0-1-.45-1-1v-2.34"/><path d="M12 2a6 6 0 0 1 6 6c0 3.24-2.5 6-6 6S6 11.24 6 8a6 6 0 0 1 6-6Z"/></svg>') !important;
    }}
    .stTabs [data-baseweb="tab"][aria-selected="true"]:nth-child(2)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%23ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.45 1-1 1H4v2h16v-2h-5c-.55 0-1-.45-1-1v-2.34"/><path d="M12 2a6 6 0 0 1 6 6c0 3.24-2.5 6-6 6S6 11.24 6 8a6 6 0 0 1 6-6Z"/></svg>') !important;
    }}

    /* Tab 3: Git Compare (Comparison) */
    .stTabs [data-baseweb="tab"]:nth-child(3)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="{TEXT_SUB.replace('#', '%23')}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M13 6h3a2 2 0 0 1 2 2v7"/><path d="M11 18H8a2 2 0 0 1-2-2V9"/></svg>') !important;
    }}
    .stTabs [data-baseweb="tab"][aria-selected="true"]:nth-child(3)::before {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%23ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M13 6h3a2 2 0 0 1 2 2v7"/><path d="M11 18H8a2 2 0 0 1-2-2V9"/></svg>') !important;
    }}

    /* st.container(border=True) customized as premium cards */
    div[data-testid="stVerticalBlockBorderWrapper"] {{
        background-color: {SURFACE} !important;
        border: 3px solid {BORDER} !important;
        border-radius: 8px !important;
        padding: 24px !important;
        box-shadow: 6px 6px 0px {SHADOW} !important;
        transition: transform 0.1s ease, box-shadow 0.1s ease;
    }}
    div[data-testid="stVerticalBlockBorderWrapper"]:hover {{
        transform: translate(2px, 2px) !important;
        box-shadow: 4px 4px 0px {SHADOW} !important;
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
