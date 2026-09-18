import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("inventory-cdc-tests")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()
