import os
import sys
import duckdb
from dotenv import load_dotenv
from logger_config import setup_logger

logger = setup_logger("silver_to_motherduck")

# Load bien moi truong tu file .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "Phase_1", ".env"))

# =============================================================
# CAU HINH DUONG DAN DONG (Docker-Compatible)
# =============================================================
_THIS_FILE  = os.path.abspath(__file__)               # .../Phase_2/silver_to_motherduck.py
_PHASE2_DIR = os.path.dirname(_THIS_FILE)             # .../Phase_2/
BASE_DIR    = os.getenv("PROJECT_ROOT", os.path.dirname(_PHASE2_DIR))
SILVER_PATH = os.path.join(_PHASE2_DIR, "silver_zone", "players_history.parquet")
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

    logger.info("Dang ket noi toi MotherDuck...")

    # Ket noi vao MotherDuck khong chi dinh DB - tranh loi "database not found"
    conn = duckdb.connect(f"md:?motherduck_token={token}")

    # Tu dong tao database neu chua ton tai
    existing_dbs = [row[0] for row in conn.execute("SHOW DATABASES").fetchall()]
    if DB_NAME not in existing_dbs:
        logger.info(f"Database '{DB_NAME}' chua ton tai. Dang tao moi...")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # Chuyen vao dung database
    conn.execute(f"USE {DB_NAME}")
    logger.info(f"Ket noi thanh cong vao database '{DB_NAME}'!")
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
        logger.error(f"Khong tim thay file Silver Zone tai: {SILVER_PATH}. Vui long chay silver_scd2_loader.py truoc!")
        return False

    logger.info(f"Dang doc du lieu tu: {SILVER_PATH}")
    logger.info(f"Dang dong bo len bang '{TABLE_NAME}' tren MotherDuck...")

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
    logger.info(f"Xac nhan tren Cloud: {total} tong ban ghi | {active} dang hoat dong (is_current=True)")

def run():
    """
    Ham dieu phoi chinh: Ket noi -> Dong bo -> Xac nhan.
    """
    logger.info("=== DONG BO SILVER ZONE -> MOTHERDUCK ===")

    conn = get_connection()
    success = sync_silver_to_cloud(conn)

    if success:
        verify_sync(conn)
        logger.info(f"=== DONG BO HOAN TAT! Truy cap MotherDuck de xem bang '{TABLE_NAME}' ===")
    else:
        logger.error("=== DONG BO THAT BAI ===")
        sys.exit(1)


if __name__ == "__main__":
    run()
