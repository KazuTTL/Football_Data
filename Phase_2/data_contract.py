import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema
import numpy as np

# =========================================================
# HOP DONG SCHEMA - DATA CONTRACT
# =========================================================
# Dinh nghia cau truc bat buoc cua DataFrame sau buoc Entity Resolution.
# silver_scd2_loader.py se goi file nay truoc khi ghi du lieu.
#
# Neu nguon du lieu thay doi Schema (doi ten cot, xoa cot), validate() se
# bao loi ngay lap tuc thay vi de loi lan vao Silver Zone.
# =========================================================

MERGED_SCHEMA = DataFrameSchema(
    {
        # --- Dinh danh ---
        "internal_player_id": Column(
            str,
            nullable=True,  # Mot so cau thu co the chua duoc anh xa
            description="ID noi bo duy nhat cua he thong (PLR_00001)",
        ),
        "id_sfs": Column(
            str,
            nullable=False,
            description="ID cau thu tren Sofascore (nguon chinh)",
        ),

        # --- Ten cau thu ---
        "name_sfs_raw": Column(
            str,
            nullable=False,
            description="Ten goc tu Sofascore (chua chuan hoa)",
        ),
        "name_sfs_norm": Column(
            str,
            nullable=False,
            description="Ten da chuan hoa: xoa dau, viet thuong",
        ),

        # --- Thong ke tu Sofascore ---
        "goals_sfs": Column(
            float,
            nullable=True,
            # coerce=True: Pandera tu dong cast int64 sang float64
            coerce=True,
            description="So ban thang (Sofascore)",
        ),
        "assists_sfs": Column(
            float,
            nullable=True,
            coerce=True,
            description="So kien tao (Sofascore)",
        ),

        # --- Thong tin tu Transfermarkt ---
        "id_tm": Column(
            str,
            nullable=True,  # Co the chua ghep duoc voi TM
            description="ID cau thu tren Transfermarkt",
        ),
        "dob_tm": Column(
            str,
            nullable=True,
            description="Ngay sinh dinh dang YYYY-MM-DD (Transfermarkt)",
        ),
        "market_value_tm": Column(
            float,
            nullable=True,
            description="Gia tri thi truong moi nhat (Transfermarkt, don vi EUR)",
        ),
        "position_tm": Column(
            str,
            nullable=True,
            description="Nhom vi tri thi dau (Attack, Defender, Midfielder, Goalkeeper)",
        ),
        "sub_position_tm": Column(
            str,
            nullable=True,
            description="Vi tri thi dau cu the (Centre-Forward, Attacking Midfield, Centre-Back...)",
        ),

        # --- Thong tin Giai dau & Mua giai (tu Sofascore Bronze) ---
        "league_sfs": Column(
            str,
            nullable=True,
            description="Ten giai dau tu Sofascore (Premier League, La Liga, Serie A, Bundesliga, Ligue 1)",
        ),
        "season_sfs": Column(
            str,
            nullable=True,
            description="Ma mua giai suy ra tu ngay cao du lieu (VD: 2025-2026)",
        ),

        # --- Data Lineage ---
        "updated_at_sfs": Column(
            str,
            nullable=False,
            description="Ngay cao du lieu tu Sofascore (YYYY-MM-DD)",
        ),
        "updated_at_tm": Column(
            str,
            # nullable=True vi cau thu chua ghep duoc voi TM se khong co gia tri nay
            nullable=True,
            description="Ngay tai CSV tu Kaggle/Transfermarkt (YYYY-MM-DD)",
        ),
    },
    # Cho phep co them cot khac ngoai nhung cot da khai bao
    strict=False,
)
