import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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

POSITION_COLOR_MAP = {
    "<b>GK</b>": "#B6E880", # Light Green
    "<b>CB</b>": "#FFA15A", # Orange
    "<b>RB</b>": "#FF6692", # Pink
    "<b>LB</b>": "#FF97FF", # Purple
    "<b>CDM</b>": "#636EFA", # Blue
    "<b>CM</b>": "#19D3F3", # Cyan
    "<b>CAM</b>": "#AB63FA", # Violet
    "<b>RW</b>": "#00CC96", # Greenish/Teal
    "<b>LW</b>": "#EF553B", # Reddish
    "<b>RM</b>": "#FFD700", # Gold
    "<b>LM</b>": "#C0C0C0", # Silver
    "<b>CF</b>": "#FF6347", # Tomato
    "<b>ST</b>": "#FECB52"  # Yellow
}

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
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.15)", zeroline=False)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_layout(
        height=350, 
        plot_bgcolor="rgba(0,0,0,0)", 
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=30, b=20)
    )
    return fig

def plot_position_distribution(fdf, template):
    """
    Generate pie chart for position group distributions.
    """
    pos_counts = fdf['position'].value_counts().reset_index()
    pos_counts.columns = ['Vị trí', 'Số lượng']
    pos_counts['Vị trí'] = pos_counts['Vị trí'].map(lambda x: POSITION_ABBR.get(str(x).lower().strip(), f"<b>{x}</b>"))
    
    fig = px.pie(
        pos_counts, names='Vị trí', values='Số lượng', hole=0.45,
        template=template, color='Vị trí', color_discrete_map=POSITION_COLOR_MAP
    )
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        marker=dict(line=dict(color="rgba(128,128,128,0.15)", width=1.5))
    )
    fig.update_layout(
        height=350, 
        plot_bgcolor="rgba(0,0,0,0)", 
        paper_bgcolor="rgba(0,0,0,0)", 
        showlegend=False,
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=30, b=20)
    )
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
        fig.update_traces(
            line_color=accent2_color, 
            line_shape='spline',
            marker=dict(size=8, symbol='circle', line=dict(color='white', width=1.5))
        )
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.15)", zeroline=False)
        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_layout(
            height=400, 
            plot_bgcolor="rgba(0,0,0,0)", 
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif"),
            margin=dict(l=20, r=20, t=30, b=20)
        )
        return fig
    return None

def plot_player_comparison_radar(player_a, player_b, cats, vals_a, vals_b, template, text_color, text_sub_color, border_color, accent_color):
    """
    Generate radar chart for comparison of two players.
    """
    fig = go.Figure()
    
    # Determine colors dynamically to ensure high contrast between Player A and Player B
    color_a = accent_color
    
    # Detect if the accent color is warm (reddish/yellowish)
    is_warm = False
    if color_a.lower().startswith("#ff") or color_a.lower().startswith("#d2") or color_a.lower().startswith("#ff5757"):
        is_warm = True
        
    if is_warm:
        # Player A is warm (e.g. brutalist red #ff5757), make Player B cool (cyan/green)
        color_b = "#00e57a" # Neo brutalist green
        fill_b = "rgba(0, 229, 122, 0.2)"
    else:
        # Player A is cool (blue/purple), make Player B warm (coral/red)
        color_b = "#ff5757" # Neo brutalist red
        fill_b = "rgba(255, 87, 87, 0.2)"

    # Helper to convert hex to rgba for Player A fill
    if color_a.startswith("#"):
        h = color_a.lstrip('#')
        rgb = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        fill_a = f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.2)"
    else:
        fill_a = "rgba(88, 166, 255, 0.2)"
    
    fig.add_trace(go.Scatterpolar(
        r=vals_a + [vals_a[0]],
        theta=cats + [cats[0]],
        name=player_a,
        fill="toself",
        line=dict(color=color_a, width=2),
        fillcolor=fill_a,
    ))

    fig.add_trace(go.Scatterpolar(
        r=vals_b + [vals_b[0]],
        theta=cats + [cats[0]],
        name=player_b,
        fill="toself",
        line=dict(color=color_b, width=2),
        fillcolor=fill_b,
    ))

    fig.update_layout(
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(visible=True, range=[0, 100],
                            tickfont=dict(color=text_sub_color, size=10),
                            gridcolor="rgba(128,128,128,0.15)"),
            angularaxis=dict(tickfont=dict(color=text_color, size=11), gridcolor="rgba(128,128,128,0.15)"),
        ),
        showlegend=True,
        legend=dict(font=dict(color=text_color), orientation="h", y=-0.2, x=0.5, xanchor="center"),
        template=template,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(l=30, r=30, t=30, b=30),
        font=dict(family="Inter, sans-serif")
    )
    return fig

def plot_moneyball_scatter(fdf, score_col, template, text_color, text_sub_color, border_color, accent_color, accent2_color):
    """
    Generate the core Moneyball Scatter Plot.
    X: Market Value
    Y: Scout Score / Rating
    """
    plot_df = fdf.copy()
    plot_df = plot_df[plot_df['market_value'].notna() & plot_df[score_col].notna()]
    plot_df['market_value_m'] = plot_df['market_value'] / 1_000_000

    if plot_df.empty:
        return None

    # Map positions to matching values in POSITION_COLOR_MAP
    plot_df['Vị trí'] = plot_df['position'].map(lambda x: POSITION_ABBR.get(str(x).lower().strip(), f"<b>{x}</b>"))

    mean_x = plot_df['market_value_m'].mean()
    mean_y = plot_df[score_col].mean()

    fig = px.scatter(
        plot_df,
        x='market_value_m',
        y=score_col,
        color='Vị trí',
        color_discrete_map=POSITION_COLOR_MAP,
        hover_name='player_name',
        hover_data={'team': True, 'Vị trí': True, 'market_value_m': ':.1f', score_col: ':.2f', 'position': False},
        template=template
    )

    # Note: DO NOT set color here as it overrides color_discrete_map
    fig.update_traces(
        marker=dict(size=10, opacity=0.75, line=dict(width=1, color="rgba(128,128,128,0.2)"))
    )

    # Add average lines
    fig.add_vline(x=mean_x, line_dash="dash", line_color=text_sub_color, opacity=0.6)
    fig.add_hline(y=mean_y, line_dash="dash", line_color=text_sub_color, opacity=0.6)

    # Add Quadrant Annotations
    max_x = plot_df['market_value_m'].max()
    max_y = plot_df[score_col].max()
    min_x = plot_df['market_value_m'].min()
    min_y = plot_df[score_col].min()

    # Calculate positions for annotations (midpoints of quadrants)
    x_left = min_x + (mean_x - min_x) / 2
    x_right = mean_x + (max_x - mean_x) / 2
    y_top = mean_y + (max_y - mean_y) / 2
    y_bottom = min_y + (mean_y - min_y) / 2

    fig.add_annotation(x=x_left, y=y_top, text="Underrated", showarrow=False, font=dict(color="#3fb950", size=14, weight="bold"), opacity=1.0)
    fig.add_annotation(x=x_right, y=y_top, text="World Class", showarrow=False, font=dict(color="#ffd700", size=14, weight="bold"), opacity=1.0)
    fig.add_annotation(x=x_left, y=y_bottom, text="Rotation / Normal", showarrow=False, font=dict(color="#8b949e", size=14, weight="bold"), opacity=1.0)
    fig.add_annotation(x=x_right, y=y_bottom, text="Overrated", showarrow=False, font=dict(color="#ff7b72", size=14, weight="bold"), opacity=1.0)

    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.15)", zeroline=False)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.15)", zeroline=False)
    fig.update_layout(
        xaxis_title="Giá trị chuyển nhượng (Triệu €)",
        yaxis_title="Scout Score" if score_col == "scout_score" else "UCL Rating",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=500,
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(title="Vị trí", font=dict(color=text_color)),
        font=dict(family="Inter, sans-serif")
    )
    
    return fig
