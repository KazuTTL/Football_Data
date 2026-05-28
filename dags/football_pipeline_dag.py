import os
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def send_telegram_alert(context):
    """Callback function to send alert to Telegram on task failure."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("WARNING: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured in .env. Skipping Telegram alert.")
        return
        
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    message = (
        f"🚨 *AIRFLOW TASK FAILED* 🚨\n\n"
        f"📌 *DAG:* `{dag_id}`\n"
        f"📌 *Task:* `{task_id}`\n"
        f"📅 *Execution Date:* `{execution_date}`\n"
        f"❌ *Error Exception:* `{exception}`\n"
        f"🔍 *Check Logs:* http://localhost:8080"
    )
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        print("Telegram alert sent successfully.")
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# Default arguments for the DAG tasks
default_args = {
    'owner': 'tien_loc',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_telegram_alert,
}

# Define the DAG
with DAG(
    dag_id='football_etl_pipeline',
    default_args=default_args,
    description='Automated Football Data Pipeline ETL (Bronze to Gold)',
    schedule_interval='0 7 * * 1,5',  # 7:00 AM every Monday and Friday
    start_date=days_ago(1),
    catchup=False,
    tags=['football', 'etl'],
) as dag:

    # 1. Ingestion: Raw player stats and match standings crawling
    task_crawl = BashOperator(
        task_id='crawl_raw_data',
        bash_command=(
            "python /opt/airflow/Phase_1_Advanced/api_extraction/main_pipeline_advanced.py && "
            "python /opt/airflow/Phase_1_Advanced/api_extraction/fetch_standings_only.py"
        ),
    )

    # 2. Normalization & Identity Matching & SCD2 loading
    task_normalize_and_scd2 = BashOperator(
        task_id='normalize_and_scd2',
        bash_command=(
            "python /opt/airflow/Phase_2/bronze_to_normalized.py && "
            "python /opt/airflow/Phase_2/entity_resolution.py && "
            "python /opt/airflow/Phase_2/silver_scd2_loader.py"
        ),
    )

    # 3. Synchronize Silver Parquet files to MotherDuck Cloud DWH
    task_sync_silver_to_dwh = BashOperator(
        task_id='sync_silver_to_dwh',
        bash_command="python /opt/airflow/Phase_2/silver_to_motherduck.py",
    )

    # 4. Gold Layer: Run Rating Engine to calculate scout scores
    task_rating_engine = BashOperator(
        task_id='run_rating_engine',
        bash_command="python /opt/airflow/Phase_3_Gold/rating_engine/run_rating_on_silver.py",
    )

    # 5. Star Schema: Rebuild dimension & fact tables and upload to DWH
    task_star_schema = BashOperator(
        task_id='rebuild_star_schema_to_dwh',
        bash_command=(
            "python /opt/airflow/Phase_3_Gold/star_schema/run_all.py && "
            "python /opt/airflow/Phase_3_Gold/star_schema/push_star_schema_to_motherduck.py"
        ),
    )

    # Define task execution workflow sequence
    task_crawl >> task_normalize_and_scd2 >> task_sync_silver_to_dwh >> task_rating_engine >> task_star_schema
