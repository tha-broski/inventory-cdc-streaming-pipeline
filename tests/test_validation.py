from validation import (
    validate_inventory,
    validate_products,
    validate_warehouses,
)


def test_validate_inventory_accepts_valid_record(spark):

    data = [
        (
            "u",
            None,
            {
                "inventory_id": 10,
                "product_id": 2,
                "warehouse_id": 3,
                "quantity": 50,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            '{"test":"payload"}',
            "inventory.public.inventory",
            0,
            100,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 0


def test_validate_inventory_rejects_negative_quantity(spark):

    data = [
        (
            "u",
            None,
            {
                "inventory_id": 10,
                "product_id": 2,
                "warehouse_id": 3,
                "quantity": -5,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            '{"test":"payload"}',
            "inventory.public.inventory",
            0,
            101,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "NEGATIVE_QUANTITY"


def test_validate_inventory_accepts_delete_using_before_id(spark):

    data = [
        (
            "d",
            {
                "inventory_id": 10,
                "product_id": 2,
                "warehouse_id": 3,
                "quantity": 50,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            None,
            '{"test":"payload"}',
            "inventory.public.inventory",
            0,
            102,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 0

    row = valid_df.select(
        "inventory_id",
        "op",
    ).collect()[0]

    assert row["inventory_id"] == 10
    assert row["op"] == "d"


def test_validate_inventory_rejects_invalid_updated_at(spark):

    data = [
        (
            "u",
            None,
            {
                "inventory_id": 10,
                "product_id": 2,
                "warehouse_id": 3,
                "quantity": 50,
                "updated_at": "not-a-timestamp",
            },
            '{"test":"payload"}',
            "inventory.public.inventory",
            0,
            103,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "INVALID_UPDATED_AT"


def test_validate_inventory_rejects_invalid_operation(spark):

    data = [
        (
            "x",
            None,
            {
                "inventory_id": 10,
                "product_id": 2,
                "warehouse_id": 3,
                "quantity": 50,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            '{"test":"payload"}',
            "inventory.public.inventory",
            0,
            104,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "INVALID_OPERATION"


def test_validate_products_rejects_invalid_price(spark):

    data = [
        (
            "u",
            None,
            {
                "product_id": 10,
                "name": "Test Product",
                "category": "Test",
                "price": "ABC",
                "is_active": True,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            '{"test":"payload"}',
            "inventory.public.products",
            0,
            200,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            product_id: INT,
            name: STRING,
            category: STRING,
            price: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        after STRUCT<
            product_id: INT,
            name: STRING,
            category: STRING,
            price: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_products(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "INVALID_PRICE"


def test_validate_products_accepts_delete_using_before_id(spark):

    data = [
        (
            "d",
            {
                "product_id": 10,
                "name": "Test Product",
                "category": "Test",
                "price": "10.00",
                "is_active": True,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            None,
            '{"test":"payload"}',
            "inventory.public.products",
            0,
            201,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            product_id: INT,
            name: STRING,
            category: STRING,
            price: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        after STRUCT<
            product_id: INT,
            name: STRING,
            category: STRING,
            price: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid_df, invalid_df = validate_products(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 0

    row = valid_df.select(
        "product_id",
        "op",
    ).collect()[0]

    assert row["product_id"] == 10
    assert row["op"] == "d"


def test_validate_warehouses_accepts_valid_record(spark):

    data = [
        (
            "u",
            None,
            {
                "warehouse_id": 5,
                "name": "Test Warehouse",
                "location": "Warsaw",
                "is_active": True,
                "updated_at": "2026-09-18T18:00:00Z",
            },
            '{"test":"payload"}',
            "inventory.public.warehouses",
            0,
            300,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            warehouse_id: INT,
            name: STRING,
            location: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        after STRUCT<
            warehouse_id: INT,
            name: STRING,
            location: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(data, schema=schema)

    valid_df, invalid_df = validate_warehouses(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 0


def test_validate_warehouses_rejects_invalid_updated_at(spark):

    data = [
        (
            "u",
            None,
            {
                "warehouse_id": 5,
                "name": "Test Warehouse",
                "location": "Warsaw",
                "is_active": True,
                "updated_at": "wrong-date",
            },
            '{"test":"payload"}',
            "inventory.public.warehouses",
            0,
            301,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            warehouse_id: INT,
            name: STRING,
            location: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        after STRUCT<
            warehouse_id: INT,
            name: STRING,
            location: STRING,
            is_active: BOOLEAN,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(data, schema=schema)

    valid_df, invalid_df = validate_warehouses(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "INVALID_UPDATED_AT"


def test_validate_inventory_rejects_malformed_payload(spark):

    data = [
        (
            None,
            None,
            None,
            '{"broken_json":',
            "inventory.public.inventory",
            0,
            105,
            None,
        )
    ]

    schema = """
        op STRING,
        before STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        after STRUCT<
            inventory_id: INT,
            product_id: INT,
            warehouse_id: INT,
            quantity: INT,
            updated_at: STRING
        >,
        raw_value STRING,
        topic STRING,
        partition INT,
        offset LONG,
        timestamp TIMESTAMP
    """

    df = spark.createDataFrame(data, schema=schema)

    valid_df, invalid_df = validate_inventory(df)

    assert valid_df.count() == 0
    assert invalid_df.count() == 1

    error_type = invalid_df.select("error_type").collect()[0]["error_type"]

    assert error_type == "MALFORMED_PAYLOAD"
