from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from app.batch.dummy_products import load_dummy_products  # type: ignore


DEFAULT_ARGS = {
    "owner": "finpro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="insert_dummy_products_hourly",
    start_date=datetime(2025, 11, 1),
    schedule="0 * * * *",  # tiap jam
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["finpro", "batch", "postgres"],
):
    insert_products = PythonOperator(
        task_id="insert_dummy_products",
        python_callable=load_dummy_products,
        op_kwargs={"n": 20},
    )
