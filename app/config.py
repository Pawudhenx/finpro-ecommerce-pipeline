import os

PG_DSN = os.getenv(
    "PG_DSN",
    "host=postgres user=postgres password=postgres dbname=ecommerce port=5432"
)

BQ_PROJECT_ID = "jcdeah-006"  
BQ_DATASET = "fauzan_finpro"  
