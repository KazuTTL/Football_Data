import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Thiết lập đường dẫn động để import db_connection từ Phase 3
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(_THIS_DIR)
star_schema_dir = os.path.join(project_root, "Phase_3_Gold", "star_schema")
if star_schema_dir not in sys.path:
    sys.path.append(star_schema_dir)

try:
    from db_connection import get_motherduck_connection
except ImportError:
    st.error("Không thể kết nối đến hệ thống quản lý cơ sở dữ liệu. Vui lòng kiểm tra lại cấu hình thư mục.")
    st.stop()

# Cấu hình giao diện Streamlit chuyên nghiệp
st.set_page_config(
    page_title="Hệ thống Trinh sát & Đánh giá Cầu thủ (Scout Board)",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS để giao diện trông hiện đại và cao cấp hơn
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }
    .main {
        background-color: #0b0f19;
        color: #f8fafc;
    }
    /* Style toàn diện cho Metric Card của Streamlit */
    [data-testid="stMetric"] {
        background-color: #1e293b !important;
        padding: 20px !important;
        border-radius: 12px !important;
        border: 1px solid #334155 !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1) !important;
    }
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important; /* Xám xanh nhạt */
        font-size: 0.95rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
    }
    [data-testid="stMetricValue"] {
        color: #38bdf8 !important; /* Màu xanh ngọc sáng cực đẹp */
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }
    h1, h2, h3 {
        font-family: 'Outfit', sans-serif;
        color: #38bdf8;
    }
</style>
""", unsafe_allow_html=True)

# --- KHỞI TẠO VÀ TẢI DỮ LIỆU TỪ CLOUD (CÓ CACHING) ---
@st.cache_data(ttl=600)  # Bộ nhớ đệm 10 phút để tối ưu hóa hiệu năng truy vấn
def load_data_from_dwh():
    try:
        conn = get_motherduck_connection()
        
        # 1. Truy vấn Star Schema kết hợp toàn bộ Dimensions
        query_schema = """
            SELECT 
                p.internal_player_id,
                p.name AS "Cầu thủ",
                p.sub_position AS "Vị trí",
                pos.name AS "Nhóm vị trí",
                t.name AS "Đội bóng",
                tour.name AS "Giải đấu",
                s.name AS "Mùa giải",
                f.goals AS "Bàn thắng",
                f.assists AS "Kiến tạo",
                ROUND(f.final_scout_score, 2) AS "Điểm Scout Score"
            FROM fact_player_season_stats f
            LEFT JOIN dim_player p ON f.player_key = p.player_key
            LEFT JOIN dim_team t ON f.team_key = t.team_key
            LEFT JOIN dim_position pos ON f.position_key = pos.position_key
            LEFT JOIN dim_tournament tour ON f.tournament_key = tour.tournament_key
            LEFT JOIN dim_season s ON f.season_key = s.season_key;
        """
        df_players = conn.execute(query_schema).df()
        
        # 2. Truy vấn bảng thành phần rating chi tiết để vẽ biểu đồ Radar
        query_rating = "SELECT * FROM gold_player_rating;"
        df_rating = conn.execute(query_rating).df()
        
        conn.close()
        return df_players, df_rating
    except Exception as e:
        st.error(f"Lỗi khi kết nối tới kho dữ liệu MotherDuck: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Tải dữ liệu
df_players, df_rating = load_data_from_dwh()

if df_players.empty or df_rating.empty:
    st.warning("⚠️ Không thể tải dữ liệu từ kho dữ liệu đám mây. Vui lòng bấm làm mới hoặc kiểm tra lại kết nối.")
    st.stop()

# --- TIÊU ĐỀ CHÍNH ---
st.title("⚽ HỆ THỐNG TRINH SÁT & ĐÁNH GIÁ CẦU THỦ CƠ SỞ DỮ LIỆU DWH (GOLD ZONE)")
st.caption("Ứng dụng quản trị và khai thác dữ liệu hiệu suất nâng cao của các cầu thủ từ kho dữ liệu đám mây MotherDuck Cloud")

# Nút reload dữ liệu ở thanh bên
if st.sidebar.button("🔄 Làm mới dữ liệu (Clear Cache & Reload)"):
    st.cache_data.clear()
    st.rerun()

# --- SIDEBAR BỘ LỌC ĐỘNG ---
st.sidebar.header("🔍 Bộ Lọc Trinh Sát")

# Lọc giải đấu
leagues = df_players["Giải đấu"].unique().tolist()
selected_leagues = st.sidebar.multiselect("Chọn Giải Đấu", leagues, default=leagues)

# Lọc nhóm vị trí
positions = df_players["Nhóm vị trí"].unique().tolist()
selected_positions = st.sidebar.multiselect("Chọn Nhóm Vị Trí", positions, default=positions)

# Lọc đội bóng dựa trên giải đấu đã chọn
available_teams = df_players[df_players["Giải đấu"].isin(selected_leagues)]["Đội bóng"].unique().tolist()
selected_teams = st.sidebar.multiselect("Chọn Câu Lạc Bộ", available_teams, default=available_teams)

# Lọc thang điểm scout
min_score, max_score = float(df_players["Điểm Scout Score"].min()), float(df_players["Điểm Scout Score"].max())
score_range = st.sidebar.slider(
    "Thang Điểm Scout Score", 
    min_value=0.0, 
    max_value=100.0, 
    value=(0.0, 100.0)
)

# Áp dụng bộ lọc
filtered_df = df_players[
    (df_players["Giải đấu"].isin(selected_leagues)) &
    (df_players["Nhóm vị trí"].isin(selected_positions)) &
    (df_players["Đội bóng"].isin(selected_teams)) &
    (df_players["Điểm Scout Score"] >= score_range[0]) &
    (df_players["Điểm Scout Score"] <= score_range[1])
]

# --- PHÂN CHIA CÁC TAB ---
tab1, tab2, tab3 = st.tabs([
    "📊 TỔNG QUAN HỆ THỐNG DWH", 
    "🏆 BẢNG XẾP HẠNG TRINH SÁT", 
    "⚔️ SO SÁNH ĐỐI ĐẦU & BIỂU ĐỒ RADAR"
])

# ==========================================
# TAB 1: TỔNG QUAN PHÂN TÍCH (DWH OVERVIEW)
# ==========================================
with tab1:
    st.subheader("📈 Phân Tích Thống Kê Tổng Quan")
    
    # Hộp số liệu nhanh (Metrics)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Tổng Số Cầu Thủ", len(df_players))
    with col2:
        st.metric("Tổng Số Đội Bóng", df_players["Đội bóng"].nunique())
    with col3:
        st.metric("Số Lượng Giải Đấu", df_players["Giải đấu"].nunique())
    with col4:
        st.metric("Điểm Scout Trung Bình", f"{df_players['Điểm Scout Score'].mean():.2f}")
        
    st.markdown("---")
    
    # Biểu đồ Plotly phân bổ cầu thủ
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("📋 Số Lượng Cầu Thủ Theo Nhóm Vị Trí")
        pos_counts = df_players["Nhóm vị trí"].value_counts().reset_index()
        pos_counts.columns = ["Nhóm vị trí", "Số lượng"]
        fig_pos = px.bar(
            pos_counts, 
            x="Nhóm vị trí", 
            y="Số lượng", 
            color="Nhóm vị trí",
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig_pos, use_container_width=True)
        
    with chart_col2:
        st.subheader("🌎 Tỉ Lệ Phân Bổ Cầu Thủ Theo Giải Đấu")
        league_counts = df_players["Giải đấu"].value_counts().reset_index()
        league_counts.columns = ["Giải đấu", "Số lượng"]
        fig_league = px.pie(
            league_counts, 
            values="Số lượng", 
            names="Giải đấu",
            hole=0.4,
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        st.plotly_chart(fig_league, use_container_width=True)

# ==========================================
# TAB 2: BẢNG XẾP HẠNG SCOUT (LEADERBOARD)
# ==========================================
with tab2:
    st.subheader("🏆 Bảng Xếp Hạng Hiệu Suất Cầu Thủ (Leaderboard)")
    st.write(f"Đang hiển thị **{len(filtered_df)}** trên tổng số **{len(df_players)}** cầu thủ sau khi lọc.")
    
    # Ô tìm kiếm tên nhanh
    search_query = st.text_input("🔍 Tìm nhanh cầu thủ theo tên:", "")
    if search_query:
        display_df = filtered_df[filtered_df["Cầu thủ"].str.contains(search_query, case=False, na=False)]
    else:
        display_df = filtered_df.copy()
        
    # Định dạng hiển thị bảng dữ liệu
    st.dataframe(
        display_df.sort_values(by="Điểm Scout Score", ascending=False),
        column_config={
            "Điểm Scout Score": st.column_config.ProgressColumn(
                "Điểm Scout Score",
                help="Thang điểm từ 0-100 sau khi tích hợp rating nâng cao",
                format="%.2f",
                min_value=0.0,
                max_value=100.0,
            )
        },
        hide_index=True,
        use_container_width=True
    )

# ==========================================
# TAB 3: SO SÁNH ĐỐI ĐẦU & BIỂU ĐỒ RADAR
# ==========================================
with tab3:
    st.subheader("⚔️ Phân Tích So Sánh Đối Đầu Trực Diện")
    st.write("Chọn 2 cầu thủ để so sánh sâu các thành phần điểm và hiệu suất của họ.")
    
    # 2 Ô chọn cầu thủ
    select_col1, select_col2 = st.columns(2)
    all_player_names = sorted(df_players["Cầu thủ"].unique().tolist())
    
    with select_col1:
        player_a = st.selectbox("Chọn Cầu Thủ A:", all_player_names, index=0)
    with select_col2:
        player_b = st.selectbox("Chọn Cầu Thủ B:", all_player_names, index=min(1, len(all_player_names)-1))
        
    st.markdown("---")
    
    # Lấy dữ liệu chi tiết của 2 cầu thủ được chọn
    p_a_data = df_rating[df_rating["name"] == player_a].iloc[0]
    p_b_data = df_rating[df_rating["name"] == player_b].iloc[0]
    
    # Hiển thị thông tin nhanh của hai cầu thủ
    card_col1, card_col2 = st.columns(2)
    
    with card_col1:
        st.markdown(f"### 🛡️ {p_a_data['name']}")
        st.markdown(f"**Câu lạc bộ**: {p_a_data['team_name']} | **Vị trí**: {p_a_data['sub_position']} (Hạng Đội: {int(p_a_data['team_rank'])})")
        st.markdown(f"**Trạng thái mẫu**: `{p_a_data['status']}`")
        st.markdown(f"## ⭐ Điểm Scout: `{p_a_data['final_scout_score']:.2f}`")
        
    with card_col2:
        st.markdown(f"### 🎯 {p_b_data['name']}")
        st.markdown(f"**Câu lạc bộ**: {p_b_data['team_name']} | **Vị trí**: {p_b_data['sub_position']} (Hạng Đội: {int(p_b_data['team_rank'])})")
        st.markdown(f"**Trạng thái mẫu**: `{p_b_data['status']}`")
        st.markdown(f"## ⭐ Điểm Scout: `{p_b_data['final_scout_score']:.2f}`")
        
    st.markdown("---")
    
    # --- VẼ BIỂU ĐỒ RADAR SO SÁNH ---
    # Các chiều đánh giá trên biểu đồ Radar (chuẩn hóa về 0-100 để vẽ đồng bộ)
    # 1. Base Score (scaled)
    # 2. Penalty (scaled or inverted để càng cao càng tốt)
    # 3. Team Multiplier (scaled: x1.0 -> 0%, x1.285 -> 100%)
    # 4. Hiệu suất bàn thắng (x quy đổi)
    # 5. Hiệu suất kiến tạo (x quy đổi)
    
    # Chuẩn hóa Multiplier về thang 0-100 để hiển thị trên Radar
    mult_a = ((p_a_data['team_multiplier'] - 1.0) / 0.285) * 100
    mult_b = ((p_b_data['team_multiplier'] - 1.0) / 0.285) * 100
    
    # Lấy số bàn thắng, kiến tạo thực tế từ Star Schema
    real_goals_a = float(df_players[df_players["Cầu thủ"] == player_a]["Bàn thắng"].values[0])
    real_assists_a = float(df_players[df_players["Cầu thủ"] == player_a]["Kiến tạo"].values[0])
    real_goals_b = float(df_players[df_players["Cầu thủ"] == player_b]["Bàn thắng"].values[0])
    real_assists_b = float(df_players[df_players["Cầu thủ"] == player_b]["Kiến tạo"].values[0])
    
    # Chuẩn hóa bàn thắng kiến tạo tương đối theo max của 2 người để vẽ
    max_goals = max(real_goals_a, real_goals_b, 1.0)
    max_assists = max(real_assists_a, real_assists_b, 1.0)
    goals_a_scaled = (real_goals_a / max_goals) * 100
    goals_b_scaled = (real_goals_b / max_goals) * 100
    assists_a_scaled = (real_assists_a / max_assists) * 100
    assists_b_scaled = (real_assists_b / max_assists) * 100
    
    categories = [
        'Điểm Base Score', 
        'Hệ số Underdog (Bonus)', 
        'Hiệu suất ghi bàn', 
        'Hiệu suất kiến tạo',
        'Tổng Điểm Scout Score'
    ]
    
    fig = go.Figure()
    
    # Cầu thủ A
    fig.add_trace(go.Scatterpolar(
        r=[
            p_a_data['base_score'] * 100 / 100, # Base Score ban đầu có thang 0-100
            mult_a,
            goals_a_scaled,
            assists_a_scaled,
            p_a_data['final_scout_score']
        ],
        theta=categories,
        fill='toself',
        name=player_a,
        line_color='#60a5fa',
        fillcolor='rgba(96, 165, 250, 0.3)'
    ))
    
    # Cầu thủ B
    fig.add_trace(go.Scatterpolar(
        r=[
            p_b_data['base_score'] * 100 / 100,
            mult_b,
            goals_b_scaled,
            assists_b_scaled,
            p_b_data['final_scout_score']
        ],
        theta=categories,
        fill='toself',
        name=player_b,
        line_color='#f87171',
        fillcolor='rgba(248, 113, 113, 0.3)'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        showlegend=True,
        template="plotly_dark",
        title=f"Biểu đồ so sánh mạng nhện (Radar Chart): {player_a} vs {player_b}"
    )
    
    # Hiển thị biểu đồ và bảng so sánh trực quan
    chart_col, data_col = st.columns([3, 2])
    with chart_col:
        st.plotly_chart(fig, use_container_width=True)
        
    with data_col:
        st.subheader("📋 Bảng So Sánh Chỉ Số Thô")
        
        compare_table = pd.DataFrame({
            "Chỉ số": ["Vị trí thi đấu", "Mùa giải", "Điểm Base Score", "Hệ số Đội (Underdog)", "Số bàn thắng", "Số kiến tạo", "Đóng góp Scout Score"],
            player_a: [
                p_a_data['sub_position'],
                "2025-2026",
                f"{p_a_data['base_score']:.2f}",
                f"x{p_a_data['team_multiplier']:.3f}",
                int(real_goals_a),
                int(real_assists_a),
                f"{p_a_data['final_scout_score']:.2f}"
            ],
            player_b: [
                p_b_data['sub_position'],
                "2025-2026",
                f"{p_b_data['base_score']:.2f}",
                f"x{p_b_data['team_multiplier']:.3f}",
                int(real_goals_b),
                int(real_assists_b),
                f"{p_b_data['final_scout_score']:.2f}"
            ]
        })
        
        st.dataframe(compare_table, hide_index=True, use_container_width=True)
