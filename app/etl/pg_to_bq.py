from datetime import datetime, timedelta, date
import psycopg2
from google.cloud import bigquery
from decimal import Decimal

from app.config import PG_DSN, BQ_PROJECT_ID, BQ_DATASET


def _fetch_h_minus_1(table_name: str, execution_date_str: str):
    """
    Ambil data H-1 dari Postgres berdasarkan created_date.
    execution_date_str: 'YYYY-MM-DD' (dari Airflow {{ ds }})
    """
    exec_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    h1 = exec_date - timedelta(days=1)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE DATE(created_date) = %s
    """

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (h1,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    return cols, rows, h1


def _serialize_for_bq(value):
    """
    Convert Python types into JSON-safe values:
    - datetime/date → ISO string
    - Decimal → float
    """
    from datetime import datetime, date

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)   # atau int(value) kalau kolom pasti integer

    return value


def _load_to_bq(table_name: str, cols: list, rows: list):
    """
    Load list of rows (list of tuples) ke BigQuery.
    Autodetect schema, append ke tabel.
    """
    if not rows:
        return

    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    dict_rows = []
    for row in rows:
        item = {}
        for col, val in zip(cols, row):
            item[col] = _serialize_for_bq(val)
        dict_rows.append(item)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    job = client.load_table_from_json(dict_rows, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows into {table_id}")


def run_etl_postgres_to_bq(execution_date: str):
    """
    Function yang akan dipanggil DAG.
    execution_date: 'YYYY-MM-DD' (Airflow {{ ds }})
    """
    tables = ["users", "products", "orders"]  # orders boleh kosong dulu

    for tbl in tables:
        cols, rows, h1 = _fetch_h_minus_1(tbl, execution_date)
        if rows:
            print(f"{tbl}: found {len(rows)} rows for {h1}")
            _load_to_bq(tbl, cols, rows)
        else:
            print(f"{tbl}: no rows for {h1}, skipping")
