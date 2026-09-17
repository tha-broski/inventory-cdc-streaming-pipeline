from pyspark.sql.functions import col

import psycopg

from config import PSYCOPG_CONNECTION

from transformations import get_latest_events

from sinks.postgres import truncate_table, write_to_staging, delete_by_ids


def process_products_batch(batch_df, batch_id):

    print(f"Processing batch {batch_id}")

    latest_events_df = get_latest_events(batch_df, "product_id").cache()

    upsert_df = latest_events_df.filter(col("op").isin("r", "c", "u")).select(
        "product_id", "name", "category", "price", "is_active", "updated_at"
    )

    delete_df = latest_events_df.filter(col("op") == "d").select("product_id")
    delete_ids = [row["product_id"] for row in delete_df.collect()]

    delete_by_ids("products", "product_id", delete_ids)

    truncate_table("products_staging")

    write_to_staging(upsert_df, "products_staging")

    with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO products (
                    product_id,
                    name,
                    category,
                    price,
                    is_active,
                    updated_at
                )
                SELECT
                    product_id,
                    name,
                    category,
                    price,
                    is_active,
                    updated_at
                FROM products_staging
                ON CONFLICT (product_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at
            """)
    latest_events_df.unpersist()
