import psycopg2
from contextlib import contextmanager
from app.config import PG_DSN


@contextmanager
def get_conn():
    """
    Context manager untuk koneksi Postgres.
    PG_DSN diambil dari environment / app.config.
    """
    conn = psycopg2.connect(PG_DSN)
    try:
        yield conn
    finally:
        conn.close()


def insert_many(sql: str, rows: list[tuple]):
    """
    Helper untuk insert banyak row sekaligus.
    """
    if not rows:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
