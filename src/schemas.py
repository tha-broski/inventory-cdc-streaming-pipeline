from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
)

product_schema = StructType(
    [
        StructField("product_id", IntegerType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("price", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_at", StringType()),
    ]
)

debezium_product_schema = StructType(
    [
        StructField("before", product_schema),
        StructField("after", product_schema),
        StructField("op", StringType()),
    ]
)

warehouse_schema = StructType(
    [
        StructField("warehouse_id", IntegerType()),
        StructField("name", StringType()),
        StructField("location", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_at", StringType()),
    ]
)

debezium_warehouse_schema = StructType(
    [
        StructField("before", warehouse_schema),
        StructField("after", warehouse_schema),
        StructField("op", StringType()),
    ]
)

inventory_schema = StructType(
    [
        StructField("inventory_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("warehouse_id", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("updated_at", StringType()),
    ]
)

debezium_inventory_schema = StructType(
    [
        StructField("before", inventory_schema),
        StructField("after", inventory_schema),
        StructField("op", StringType()),
    ]
)
