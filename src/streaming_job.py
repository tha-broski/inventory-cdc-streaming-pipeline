from pyspark.sql import SparkSession
from schemas import (
    debezium_product_schema,
    debezium_warehouse_schema,
    debezium_inventory_schema,
)
from validation import validate_products, validate_warehouses, validate_inventory
from transformations import build_products_df, build_warehouses_df, build_inventory_df
from processors.products import process_products_batch
from processors.warehouses import process_warehouses_batch
from processors.inventory import process_inventory_batch
from sinks.kafka import build_dlq_df, start_dlq_query
from sources.kafka import read_cdc_stream

spark = SparkSession.builder.appName("inventory-cdc-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

products_events_df = read_cdc_stream(
    spark, "inventory.public.products", debezium_product_schema
)

warehouses_events_df = read_cdc_stream(
    spark, "inventory.public.warehouses", debezium_warehouse_schema
)

inventory_events_df = read_cdc_stream(
    spark, "inventory.public.inventory", debezium_inventory_schema
)

products_valid_df, products_invalid_df = validate_products(products_events_df)

warehouses_valid_df, warehouses_invalid_df = validate_warehouses(warehouses_events_df)

inventory_valid_df, inventory_invalid_df = validate_inventory(inventory_events_df)

products_df = build_products_df(products_valid_df)

products_query = (
    products_df.writeStream.foreachBatch(process_products_batch)
    .option("checkpointLocation", "/opt/spark-checkpoints/products")
    .start()
)

warehouses_df = build_warehouses_df(warehouses_valid_df)

warehouses_query = (
    warehouses_df.writeStream.foreachBatch(process_warehouses_batch)
    .option("checkpointLocation", "/opt/spark-checkpoints/warehouses")
    .start()
)

inventory_df = build_inventory_df(inventory_valid_df)

inventory_query = (
    inventory_df.writeStream.foreachBatch(process_inventory_batch)
    .option(
        "checkpointLocation",
        "/opt/spark-checkpoints/inventory",
    )
    .start()
)

products_dlq_df = build_dlq_df(products_invalid_df)

products_dlq_query = start_dlq_query(
    products_dlq_df,
    "inventory.products.dlq",
    "/opt/spark-checkpoints/products-dlq",
)

warehouses_dlq_df = build_dlq_df(warehouses_invalid_df)

warehouses_dlq_query = start_dlq_query(
    warehouses_dlq_df,
    "inventory.warehouses.dlq",
    "/opt/spark-checkpoints/warehouses-dlq",
)

inventory_dlq_df = build_dlq_df(inventory_invalid_df)

inventory_dlq_query = start_dlq_query(
    inventory_dlq_df,
    "inventory.inventory.dlq",
    "/opt/spark-checkpoints/inventory-dlq",
)

spark.streams.awaitAnyTermination()
