import uuid
from datetime import datetime, timedelta

from faker import Faker
from .db_utils import insert_many

fake = Faker()


def generate_dummy_users(n: int = 10):
    """
    Generate n baris dummy user dengan created_date = H-1.
    """
    rows = []
    for _ in range(n):
        user_id = str(uuid.uuid4())
        name = fake.name()
        email = fake.email()

        # H-1
        created_date = datetime.utcnow() - timedelta(days=1)

        rows.append((user_id, name, email, created_date))
    return rows


def load_dummy_users(n: int = 10):
    """
    Insert dummy users ke tabel users.
    Dipakai di DAG Airflow.
    """
    sql = """
        INSERT INTO users (user_id, name, email, created_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
    """
    rows = generate_dummy_users(n)
    insert_many(sql, rows)
