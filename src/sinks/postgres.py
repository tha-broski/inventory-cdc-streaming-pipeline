import psycopg

from config import (
    JDBC_URL,
    JDBC_PROPERTIES,
    PSYCOPG_CONNECTION,
)


def truncate_table(table_name: str) -> None:
    with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")


def write_to_staging(df, table_name: str) -> None:
    df.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode="append",
        properties=JDBC_PROPERTIES,
    )


def delete_by_ids(
    table_name: str,
    id_column: str,
    ids: list[int],
) -> None:

    if not ids:
        return

    with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE {id_column} = ANY(%s)
                """,
                (ids,),
            )
