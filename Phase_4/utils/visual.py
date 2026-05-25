import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def plot_club_distribution(fdf, top_n_choice, template, accent_color):
    """
    Generate bar chart of player counts per club.
    """
    club_counts = fdf['team'].value_counts().reset_index()
    club_counts.columns = ['Câu lạc bộ', 'Số lượng cầu thủ']
    
    if top_n_choice == "Top 5":
        club_counts = club_counts.head(5)
    elif top_n_choice == "Top 10":
        club_counts = club_counts.head(10)
        
    fig = px.bar(
        club_counts, x='Câu lạc bộ', y='Số lượng cầu thủ', text='Số lượng cầu thủ',
        template=template, color_discrete_sequence=[accent_color]
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(height=350, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    return fig

def plot_position_distribution(fdf, template):
    """
    Generate pie chart for position group distributions.
    """
    POSITION_ABBR = {
        "goalkeeper": "<b>GK</b>",
        "centre back": "<b>CB</b>",
        "centre-back": "<b>CB</b>",
        "right back": "<b>RB</b>",
        "right-back": "<b>RB</b>",
        "left back": "<b>LB</b>",
        "left-back": "<b>LB</b>",
        "defensive midfield": "<b>CDM</b>",
        "central midfield": "<b>CM</b>",
        "attacking midfield": "<b>CAM</b>",
        "right winger": "<b>RW</b>",
        "left winger": "<b>LW</b>",
        "right midfield": "<b>RM</b>",
        "left midfield": "<b>LM</b>",
        "second striker": "<b>CF</b>",
        "centre forward": "<b>ST</b>",
        "centre-forward": "<b>ST</b>"
    }
    pos_counts = fdf['position'].value_counts().reset_index()
    pos_counts.columns = ['Vị trí', 'Số lượng']
    pos_counts['Vị trí'] = pos_counts['Vị trí'].map(lambda x: POSITION_ABBR.get(str(x).lower().strip(), f"<b>{x}</b>"))
    
    fig = px.pie(
        pos_counts, names='Vị trí', values='Số lượng', hole=0.45,
        template=template, color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=350, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
    return fig

def plot_market_value_history(df_history, selected_hist_player, template, accent2_color):
    """
    Generate line chart showing the player's historical market value.
    """
    player_hist = df_history[df_history['name'] == selected_hist_player].copy()
    if len(player_hist) > 0:
        player_hist['valid_from'] = pd.to_datetime(player_hist['valid_from'])
        player_hist['Giá trị (Triệu €)'] = player_hist['current_market_value'] / 1_000_000
        
        fig = px.line(
            player_hist, 
            x='valid_from', y='Giá trị (Triệu €)', 
            markers=True,
            labels={'valid_from': 'Ngày cập nhật', 'Giá trị (Triệu €)': 'Giá trị (Triệu €)'},
            template=template
        )
        fig.update_traces(line_color=accent2_color, marker=dict(size=8))
        fig.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        return fig
    return None

def plot_player_comparison_radar(player_a, player_b, cats, vals_a, vals_b, template, text_color, text_sub_color, border_color, accent_color):
    """
    Generate radar chart for comparison of two players.
    """
    fig = go.Figure()
    
    # Determine fill color dynamically
    fill_a = "rgba(88,166,255,0.25)" if accent_color == "#58a6ff" else "rgba(9,105,218,0.25)"
    
    fig.add_trace(go.Scatterpolar(
        r=vals_a + [vals_a[0]],
        theta=cats + [cats[0]],
        name=player_a,
        fill="toself",
        line=dict(color=accent_color, width=2),
        fillcolor=fill_a,
    ))

    fig.add_trace(go.Scatterpolar(
        r=vals_b + [vals_b[0]],
        theta=cats + [cats[0]],
        name=player_b,
        fill="toself",
        line=dict(color="#f87171", width=2),
        fillcolor="rgba(248,113,113,0.25)",
    ))

    fig.update_layout(
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(visible=True, range=[0, 100],
                            tickfont=dict(color=text_sub_color, size=10),
                            gridcolor=border_color),
            angularaxis=dict(tickfont=dict(color=text_color, size=11), gridcolor=border_color),
        ),
        showlegend=True,
        legend=dict(font=dict(color=text_color), orientation="h", y=-0.2, x=0.5, xanchor="center"),
        template=template,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(l=30, r=30, t=30, b=30),
    )
    return fig
