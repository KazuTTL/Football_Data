"""
Streamlit Lucide Icons Utility Module
Provides consistent, lightweight, themeable SVG icons matching Lucide outline standards.
"""

ICONS = {
    "sliders": """
        <line x1="4" y1="21" x2="4" y2="14" />
        <line x1="4" y1="10" x2="4" y2="3" />
        <line x1="12" y1="21" x2="12" y2="12" />
        <line x1="12" y1="8" x2="12" y2="3" />
        <line x1="20" y1="21" x2="20" y2="16" />
        <line x1="20" y1="12" x2="20" y2="3" />
        <line x1="2" y1="14" x2="6" y2="14" />
        <line x1="10" y1="8" x2="14" y2="8" />
        <line x1="18" y1="16" x2="22" y2="16" />
    """,
    "sun": """
        <circle cx="12" cy="12" r="4" />
        <path d="M12 2v2" />
        <path d="M12 20v2" />
        <path d="m4.93 4.93 1.41 1.41" />
        <path d="m17.66 17.66 1.41 1.41" />
        <path d="M2 12h2" />
        <path d="M20 12h2" />
        <path d="m6.34 17.66-1.41 1.41" />
        <path d="m19.07 4.93-1.41 1.41" />
    """,
    "moon": """
        <path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z" />
    """,
    "refresh-cw": """
        <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
        <path d="M3 3v5h5" />
        <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16" />
        <path d="M16 16h5v5" />
    """,
    "trending-up": """
        <polyline points="22 7 13.5 15.5 8.5 10.5 2 17" />
        <polyline points="16 7 22 7 22 13" />
    """,
    "gem": """
        <path d="M6 3h12l4 6-10 12L2 9z" />
        <path d="M11 3 8 9l4 12 4-12-3-6" />
        <path d="M2 9h20" />
    """,
    "eye": """
        <path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z" />
        <circle cx="12" cy="12" r="3" />
    """,
    "users": """
        <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
        <circle cx="9" cy="7" r="4" />
        <path d="M22 21v-2a4 4 0 0 0-3-3.87" />
        <path d="M16 3.13a4 4 0 0 1 0 7.75" />
    """,
    "shield": """
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
    """,
    "globe": """
        <circle cx="12" cy="12" r="10" />
        <path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20" />
        <path d="M2 12h20" />
    """,
    "star": """
        <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
    """,
    "award": """
        <circle cx="12" cy="8" r="7" />
        <polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88" />
    """,
    "trophy": """
        <path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6" />
        <path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18" />
        <path d="M4 22h16" />
        <path d="M10 14.66V17c0 .55-.45 1-1 1H4v2h16v-2h-5c-.55 0-1-.45-1-1v-2.34" />
        <path d="M12 2a8 8 0 0 0-8 8v1.66c0 1.25.7 2.39 1.8 2.94L12 18l6.2-3.4A3.33 3.33 0 0 0 20 11.66V10a8 8 0 0 0-8-8z" />
    """,
    "search": """
        <circle cx="11" cy="11" r="8" />
        <path d="m21 21-4.3-4.3" />
    """
}

def get_icon(name, size=24, stroke_width=2, color="currentColor", style=""):
    """
    Returns a clean single-line SVG string for the requested Lucide outline icon.
    """
    path_data = ICONS.get(name.lower(), "")
    if not path_data:
        path_data = '<circle cx="12" cy="12" r="10" />'
        
    # Làm sạch path data (nén bỏ xuống dòng và khoảng trắng thừa)
    path_cleaned = " ".join(path_data.split())
    
    # Trả về chuỗi SVG trên một dòng duy nhất, loại bỏ mọi thụt lề đầu dòng
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="{stroke_width}" stroke-linecap="round" stroke-linejoin="round" style="vertical-align: middle; display: inline-block; {style}">{path_cleaned}</svg>'
