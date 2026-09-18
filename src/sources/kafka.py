from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json


def read_cdc_stream(
    spark: SparkSession,
    topic: str,
    schema,
) -> DataFrame:

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    value_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS raw_value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )

    parsed_df = value_df.select(
        from_json(
            col("raw_value"),
            schema,
        ).alias("data"),
        col("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    return parsed_df.select(
        col("data.op").alias("op"),
        col("data.before").alias("before"),
        col("data.after").alias("after"),
        col("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )
