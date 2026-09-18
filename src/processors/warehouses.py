from pyspark.sql.functions import col

from transformations import get_latest_events
from sinks.postgres import truncate_table, write_to_staging, delete_by_ids, execute_sql

from logger import get_logger

logger = get_logger(__name__)


def process_warehouses_batch(batch_df, batch_id):

    logger.info(
        "Processing warehouses batch %s",
        batch_id,
    )

    latest_events_df = get_latest_events(batch_df, "warehouse_id").cache()

    try:
        upsert_df = latest_events_df.filter(col("op").isin("r", "c", "u")).select(
            "warehouse_id", "name", "location", "is_active", "updated_at"
        )

        delete_df = latest_events_df.filter(col("op") == "d").select("warehouse_id")

        delete_ids = [row["warehouse_id"] for row in delete_df.collect()]

        logger.info(
            "Warehouses batch %s contains %s deletes",
            batch_id,
            len(delete_ids),
        )

        delete_by_ids("warehouses", "warehouse_id", delete_ids)

        truncate_table("warehouses_staging")

        write_to_staging(upsert_df, "warehouses_staging")

        execute_sql("""
            INSERT INTO warehouses (
                warehouse_id,
                name,
                location,
                is_active,
                updated_at
            )
            SELECT
                warehouse_id,
                name,
                location,
                is_active,
                updated_at
            FROM warehouses_staging
            ON CONFLICT (warehouse_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                location = EXCLUDED.location,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            """)

    finally:
        latest_events_df.unpersist()
