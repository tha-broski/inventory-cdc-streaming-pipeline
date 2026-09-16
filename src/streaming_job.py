from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    try_to_timestamp,
    coalesce,
    row_number,
    desc,
    when,
    expr,
    to_json,
    struct,
    lit,
)
from pyspark.sql.types import DecimalType
from schemas import debezium_product_schema
import os
import psycopg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("inventory-cdc-streaming").getOrCreate()
TARGET_POSTGRES_USER = os.getenv("TARGET_POSTGRES_USER")
TARGET_POSTGRES_PASSWORD = os.getenv("TARGET_POSTGRES_PASSWORD")
TARGET_POSTGRES_DB = os.getenv("TARGET_POSTGRES_DB")
spark.sparkContext.setLogLevel("WARN")

PSYCOPG_CONNECTION = {
    "host": "postgres-target",
    "port": 5432,
    "dbname": TARGET_POSTGRES_DB,
    "user": TARGET_POSTGRES_USER,
    "password": TARGET_POSTGRES_PASSWORD,
}

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "inventory.public.products")
    .option("startingOffsets", "earliest")
    .load()
)

value_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS raw_value", "topic", "partition", "offset", "timestamp"
)

parsed_df = value_df.select(
    from_json(col("raw_value"), debezium_product_schema).alias("data"),
    col("raw_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

events_df = parsed_df.select(
    col("data.op").alias("op"),
    col("data.before").alias("before"),
    col("data.after").alias("after"),
    col("raw_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

validated_df = (
    events_df.withColumn(
        "product_id", coalesce(col("after.product_id"), col("before.product_id"))
    )
    .withColumn("price_raw", col("after.price"))
    .withColumn("price_parsed", expr("try_cast(price_raw AS DECIMAL(10,2))"))
    .withColumn("updated_at_raw", col("after.updated_at"))
    .withColumn("updated_at_parsed", try_to_timestamp(col("updated_at_raw")))
)

validated_df = validated_df.withColumn(
    "error_type",
    when(~col("op").isin("r", "c", "u", "d"), "INVALID_OPERATION")
    .when(col("product_id").isNull(), "MISSING_PRODUCT_ID")
    .when(
        col("op").isin("r", "c", "u")
        & col("price_raw").isNotNull()
        & col("price_parsed").isNull(),
        "INVALID_PRICE",
    )
    .when(
        col("op").isin("r", "c", "u")
        & col("updated_at_raw").isNotNull()
        & col("updated_at_parsed").isNull(),
        "INVALID_UPDATED_AT",
    ),
)

valid_df = validated_df.filter(col("error_type").isNull())

invalid_df = validated_df.filter(col("error_type").isNotNull())

dlq_df = invalid_df.select(
    to_json(
        struct(
            col("raw_value").alias("raw_payload"),
            col("topic").alias("source_topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("error_type"),
            lit("Record failed validation").alias("error_message"),
        )
    ).alias("value")
)

products_df = valid_df.select(
    col("op"),
    col("product_id"),
    col("after.name").alias("name"),
    col("after.category").alias("category"),
    col("price_parsed").alias("price"),
    col("after.is_active").alias("is_active"),
    col("updated_at_parsed").alias("updated_at"),
    col("partition"),
    col("offset"),
)

JDBC_URL = f"jdbc:postgresql://postgres-target:5432/{TARGET_POSTGRES_DB}"

JDBC_PROPERTIES = {
    "user": TARGET_POSTGRES_USER,
    "password": TARGET_POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def process_products_batch(batch_df, batch_id):

    print(f"Processing batch {batch_id}")

    product_window = Window.partitionBy("product_id").orderBy(desc("offset"))

    upsert_df = (
        batch_df.filter(col("op").isin("r", "c", "u"))
        .withColumn("row_num", row_number().over(product_window))
        .filter(col("row_num") == 1)
        .select("product_id", "name", "category", "price", "is_active", "updated_at")
    )

    delete_df = batch_df.filter(col("op") == "d").select(col("product_id"))
    delete_ids = [row["product_id"] for row in delete_df.collect()]

    if delete_ids:
        with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM products WHERE product_id = ANY(%s)", (delete_ids,)
                )

    with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE products_staging")

    upsert_df.write.jdbc(
        url=JDBC_URL,
        table="products_staging",
        mode="append",
        properties=JDBC_PROPERTIES,
    )

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


query = (
    products_df.writeStream.foreachBatch(process_products_batch)
    .option("checkpointLocation", "/opt/spark-checkpoints/products")
    .start()
)

dlq_query = (
    dlq_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "inventory.products.dlq")
    .option("checkpointLocation", "/opt/spark-checkpoints/products-dlq")
    .start()
)

spark.streams.awaitAnyTermination()
