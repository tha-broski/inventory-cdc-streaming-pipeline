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
