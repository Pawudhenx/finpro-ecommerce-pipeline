from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Dengan PYTHONPATH=/opt/airflow dan app ter-mount ke /opt/airflow/app,
# import di bawah ini akan bekerja:
from app.batch.dummy_users import load_dummy_users  # type: ignore


DEFAULT_ARGS = {
    "owner": "finpro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="insert_dummy_users_hourly",
    start_date=datetime(2025, 11, 1),
    schedule="0 * * * *",  # tiap jam
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["finpro", "batch", "postgres"],
):
    insert_users = PythonOperator(
        task_id="insert_dummy_users",
        python_callable=load_dummy_users,
        op_kwargs={"n": 20},
    )
