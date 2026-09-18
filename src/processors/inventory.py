from pyspark.sql.functions import col

from transformations import get_latest_events
from sinks.postgres import truncate_table, write_to_staging, delete_by_ids, execute_sql

from logger import get_logger

logger = get_logger(__name__)


def process_inventory_batch(batch_df, batch_id):

    logger.info(
        "Processing inventory batch %s",
        batch_id,
    )

    latest_events_df = get_latest_events(
        batch_df,
        "inventory_id",
    ).cache()

    try:
        upsert_df = latest_events_df.filter(col("op").isin("r", "c", "u")).select(
            "inventory_id",
            "product_id",
            "warehouse_id",
            "quantity",
            "updated_at",
        )

        delete_df = latest_events_df.filter(col("op") == "d").select("inventory_id")

        delete_ids = [row["inventory_id"] for row in delete_df.collect()]

        logger.info(
            "Inventory batch %s contains %s deletes",
            batch_id,
            len(delete_ids),
        )

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

        execute_sql("""
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
    finally:
        latest_events_df.unpersist()
