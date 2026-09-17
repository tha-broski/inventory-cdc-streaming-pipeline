from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json


def build_dlq_df(invalid_df: DataFrame) -> DataFrame:
    return invalid_df.select(
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


def start_dlq_query(
    dlq_df: DataFrame,
    topic: str,
    checkpoint_location: str,
):
    return (
        dlq_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
