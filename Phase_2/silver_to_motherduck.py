import os
import sys
import duckdb
from dotenv import load_dotenv

# Load bien moi truong tu file .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "Phase_1", ".env"))

# Cau hinh duong dan
BASE_DIR    = r"C:\FastAPI\Football"
SILVER_PATH = os.path.join(BASE_DIR, "Phase_2", "silver_zone", "players_history.parquet")
DB_NAME     = "football_data"
TABLE_NAME  = "silver_players"


def get_connection():
    """
    Ket noi toi MotherDuck Cloud.
    - Buoc 1: Ket noi vao MotherDuck session (khong chi dinh DB cu the).
    - Buoc 2: Tu dong tao database 'football_data' neu chua co.
    - Buoc 3: Chuyen vao database do.
    """
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("Khong tim thay MOTHERDUCK_TOKEN trong file .env!")

    print(f"[motherduck] Dang ket noi toi MotherDuck...")

    # Ket noi vao MotherDuck khong chi dinh DB - tranh loi "database not found"
    conn = duckdb.connect(f"md:?motherduck_token={token}")

    # Tu dong tao database neu chua ton tai
    existing_dbs = [row[0] for row in conn.execute("SHOW DATABASES").fetchall()]
    if DB_NAME not in existing_dbs:
        print(f"[motherduck] Database '{DB_NAME}' chua ton tai. Dang tao moi...")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # Chuyen vao dung database
    conn.execute(f"USE {DB_NAME}")
    print(f"[motherduck] Ket noi thanh cong vao database '{DB_NAME}'!")
    return conn



def sync_silver_to_cloud(conn):
    """
    Dong bo file Parquet tu Silver Zone local len MotherDuck.

    Su dung lenh: CREATE OR REPLACE TABLE
    - CREATE: Tao bang moi neu chua co.
    - OR REPLACE: Ghi de hoan toan du lieu cu neu da ton tai.
    Phu hop voi mo hinh "Moi lan chay pipeline thi reload lai Silver tren Cloud".
    """
    # Kiem tra file local truoc khi ket noi
    if not os.path.exists(SILVER_PATH):
        print(f"[motherduck] Loi: Khong tim thay file Silver Zone tai: {SILVER_PATH}")
        print("[motherduck] Vui long chay silver_scd2_loader.py truoc!")
        return False

    print(f"[motherduck] Dang doc du lieu tu: {SILVER_PATH}")
    print(f"[motherduck] Dang dong bo len bang '{TABLE_NAME}' tren MotherDuck...")

    # DuckDB co the doc truc tiep file Parquet bang SQL
    # Khong can chuyen qua Pandas - day la mot diem manh cua DuckDB
    conn.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT
            *,
            CURRENT_DATE AS synced_at
        FROM read_parquet('{SILVER_PATH.replace(chr(92), '/')}')
    """)

    return True


def verify_sync(conn):
    """
    Xac nhan dong bo thanh cong bang cach dem so ban ghi tren Cloud.
    """
    result = conn.execute(f"SELECT COUNT(*) as total, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as active FROM {TABLE_NAME}").fetchone()
    total, active = result
    print(f"[motherduck] Xac nhan tren Cloud: {total} tong ban ghi | {active} dang hoat dong (is_current=True)")


def run():
    """
    Ham dieu phoi chinh: Ket noi -> Dong bo -> Xac nhan.
    """
    print("=== DONG BO SILVER ZONE -> MOTHERDUCK ===")

    conn = get_connection()
    success = sync_silver_to_cloud(conn)

    if success:
        verify_sync(conn)
        print(f"=== DONG BO HOAN TAT! Truy cap MotherDuck de xem bang '{TABLE_NAME}' ===")
    else:
        print("=== DONG BO THAT BAI ===")
        sys.exit(1)


if __name__ == "__main__":
    run()
