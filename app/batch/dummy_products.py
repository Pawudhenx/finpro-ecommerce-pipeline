import uuid
from datetime import datetime, timedelta

from .db_utils import insert_many

CATEGORIES = ["electronics", "fashion", "grocery", "toys", "sports"]


def generate_dummy_products(n: int = 10):
    """
    Generate n baris dummy product dengan created_date = H-1.
    """
    rows = []
    for i in range(n):
        product_id = str(uuid.uuid4())
        product_name = f"Product {i+1}"
        category = CATEGORIES[i % len(CATEGORIES)]
        price = 10000 + (i * 5000)

        # H-1
        created_date = datetime.utcnow() - timedelta(days=1)

        rows.append((product_id, product_name, category, price, created_date))
    return rows


def load_dummy_products(n: int = 10):
    """
    Insert dummy products ke tabel products.
    Dipakai di DAG Airflow.
    """
    sql = """
        INSERT INTO products (product_id, product_name, category, price, created_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """
    rows = generate_dummy_products(n)
    insert_many(sql, rows)
