from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

BASE_PATH = Path("/opt/airflow")
if str(BASE_PATH) not in sys.path:
    sys.path.append(str(BASE_PATH))

from app.etl.pg_to_bq import run_etl_postgres_to_bq  # noqa: E402


DEFAULT_ARGS = {
    "owner": "finpro",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="pg_to_bq_daily",
    start_date=datetime(2025, 11, 1),
    schedule="0 2 * * *",  # jalan tiap hari jam 02:00
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["finpro", "bigquery", "etl"],
):

    etl_task = PythonOperator(
        task_id="etl_postgres_to_bigquery",
        python_callable=run_etl_postgres_to_bq,
        op_kwargs={"execution_date": "{{ ds }}"},  # 'YYYY-MM-DD'
    )
