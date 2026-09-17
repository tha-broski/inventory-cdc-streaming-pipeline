import os

TARGET_POSTGRES_USER = os.getenv("TARGET_POSTGRES_USER")
TARGET_POSTGRES_PASSWORD = os.getenv("TARGET_POSTGRES_PASSWORD")
TARGET_POSTGRES_DB = os.getenv("TARGET_POSTGRES_DB")

PSYCOPG_CONNECTION = {
    "host": "postgres-target",
    "port": 5432,
    "dbname": TARGET_POSTGRES_DB,
    "user": TARGET_POSTGRES_USER,
    "password": TARGET_POSTGRES_PASSWORD,
}

JDBC_URL = f"jdbc:postgresql://postgres-target:5432/{TARGET_POSTGRES_DB}"

JDBC_PROPERTIES = {
    "user": TARGET_POSTGRES_USER,
    "password": TARGET_POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}
