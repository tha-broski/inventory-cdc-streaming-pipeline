from pyspark.sql.functions import (
    col,
    try_to_timestamp,
    coalesce,
    when,
    expr,
)


def validate_products(events_df):
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

    return valid_df, invalid_df


def validate_warehouses(events_df):
    validated_df = (
        events_df.withColumn(
            "warehouse_id",
            coalesce(col("after.warehouse_id"), col("before.warehouse_id")),
        )
        .withColumn("updated_at_raw", col("after.updated_at"))
        .withColumn("updated_at_parsed", try_to_timestamp(col("updated_at_raw")))
    )

    validated_df = validated_df.withColumn(
        "error_type",
        when(~col("op").isin("r", "c", "u", "d"), "INVALID_OPERATION")
        .when(col("warehouse_id").isNull(), "MISSING_WAREHOUSE_ID")
        .when(
            col("op").isin("r", "c", "u")
            & col("updated_at_raw").isNotNull()
            & col("updated_at_parsed").isNull(),
            "INVALID_UPDATED_AT",
        ),
    )

    valid_df = validated_df.filter(col("error_type").isNull())
    invalid_df = validated_df.filter(col("error_type").isNotNull())

    return valid_df, invalid_df


def validate_inventory(events_df):

    validated_df = (
        events_df.withColumn(
            "inventory_id",
            coalesce(
                col("after.inventory_id"),
                col("before.inventory_id"),
            ),
        )
        .withColumn(
            "product_id",
            coalesce(
                col("after.product_id"),
                col("before.product_id"),
            ),
        )
        .withColumn(
            "warehouse_id",
            coalesce(
                col("after.warehouse_id"),
                col("before.warehouse_id"),
            ),
        )
        .withColumn("quantity", col("after.quantity"))
        .withColumn("updated_at_raw", col("after.updated_at"))
        .withColumn("updated_at_parsed", try_to_timestamp(col("updated_at_raw")))
    )

    validated_df = validated_df.withColumn(
        "error_type",
        when(
            ~col("op").isin("r", "c", "u", "d"),
            "INVALID_OPERATION",
        )
        .when(
            col("inventory_id").isNull(),
            "MISSING_INVENTORY_ID",
        )
        .when(
            col("op").isin("r", "c", "u") & col("product_id").isNull(),
            "MISSING_PRODUCT_ID",
        )
        .when(
            col("op").isin("r", "c", "u") & col("warehouse_id").isNull(),
            "MISSING_WAREHOUSE_ID",
        )
        .when(
            col("op").isin("r", "c", "u") & col("quantity").isNull(),
            "MISSING_QUANTITY",
        )
        .when(
            col("op").isin("r", "c", "u") & (col("quantity") < 0),
            "NEGATIVE_QUANTITY",
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

    return valid_df, invalid_df
