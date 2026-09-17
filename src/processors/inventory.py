from pyspark.sql.functions import col

import psycopg

from config import PSYCOPG_CONNECTION
from transformations import get_latest_events
from sinks.postgres import (
    truncate_table,
    write_to_staging,
    delete_by_ids,
)


def process_inventory_batch(batch_df, batch_id):

    print(f"Processing inventory batch {batch_id}")

    latest_events_df = get_latest_events(
        batch_df,
        "inventory_id",
    ).cache()

    upsert_df = latest_events_df.filter(col("op").isin("r", "c", "u")).select(
        "inventory_id",
        "product_id",
        "warehouse_id",
        "quantity",
        "updated_at",
    )

    delete_df = latest_events_df.filter(col("op") == "d").select("inventory_id")

    delete_ids = [row["inventory_id"] for row in delete_df.collect()]

    delete_by_ids(
        "inventory",
        "inventory_id",
        delete_ids,
    )

    truncate_table("inventory_staging")

    write_to_staging(
        upsert_df,
        "inventory_staging",
    )

    with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO inventory (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    quantity,
                    updated_at
                )
                SELECT
                    inventory_id,
                    product_id,
                    warehouse_id,
                    quantity,
                    updated_at
                FROM inventory_staging
                ON CONFLICT (inventory_id)
                DO UPDATE SET
                    product_id = EXCLUDED.product_id,
                    warehouse_id = EXCLUDED.warehouse_id,
                    quantity = EXCLUDED.quantity,
                    updated_at = EXCLUDED.updated_at
            """)

    latest_events_df.unpersist()
