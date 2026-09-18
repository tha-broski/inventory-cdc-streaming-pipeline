from transformations import get_latest_events


def test_get_latest_events_keeps_highest_offset(spark):

    data = [
        (10, "u", 100),
        (10, "u", 101),
        (10, "d", 102),
        (20, "c", 200),
        (20, "u", 201),
    ]

    df = spark.createDataFrame(
        data,
        ["inventory_id", "op", "offset"],
    )

    result_df = get_latest_events(
        df,
        "inventory_id",
    )

    result = {
        row["inventory_id"]: (row["op"], row["offset"]) for row in result_df.collect()
    }

    assert result == {
        10: ("d", 102),
        20: ("u", 201),
    }


def test_get_latest_events_keeps_update_after_delete(spark):

    data = [
        (10, "d", 100),
        (10, "u", 101),
    ]

    df = spark.createDataFrame(
        data,
        ["inventory_id", "op", "offset"],
    )

    result_df = get_latest_events(
        df,
        "inventory_id",
    )

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["inventory_id"] == 10
    assert result[0]["op"] == "u"
    assert result[0]["offset"] == 101
