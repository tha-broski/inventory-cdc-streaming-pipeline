from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window


def build_products_df(valid_df):
    return valid_df.select(
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


def build_warehouses_df(valid_df):
    return valid_df.select(
        col("op"),
        col("warehouse_id"),
        col("after.name").alias("name"),
        col("after.location").alias("location"),
        col("after.is_active").alias("is_active"),
        col("updated_at_parsed").alias("updated_at"),
        col("partition"),
        col("offset"),
    )


def build_inventory_df(valid_df):
    return valid_df.select(
        col("op"),
        col("inventory_id"),
        col("product_id"),
        col("warehouse_id"),
        col("quantity"),
        col("updated_at_parsed").alias("updated_at"),
        col("partition"),
        col("offset"),
    )


def get_latest_events(
    df: DataFrame,
    key_column: str,
) -> DataFrame:

    window = Window.partitionBy(key_column).orderBy(desc("offset"))

    return (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
