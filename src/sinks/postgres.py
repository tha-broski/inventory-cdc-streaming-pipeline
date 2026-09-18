import time
import psycopg
from psycopg import sql
from config import (
    JDBC_URL,
    JDBC_PROPERTIES,
    PSYCOPG_CONNECTION,
)
from logger import get_logger

logger = get_logger(__name__)


def run_with_retry(
    operation,
    attempts: int = 3,
    delay_seconds: int = 2,
):
    last_exception = None

    for attempt in range(1, attempts + 1):
        try:
            return operation()

        except psycopg.OperationalError as exc:
            last_exception = exc

            logger.warning(
                "PostgreSQL operation failed (attempt %s/%s): %s",
                attempt,
                attempts,
                exc,
            )

            if attempt < attempts:
                time.sleep(delay_seconds)

    logger.error(
        "PostgreSQL operation failed after %s attempts",
        attempts,
    )
    raise last_exception


def truncate_table(table_name: str) -> None:

    def operation():
        with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
                )

    run_with_retry(operation)


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

    def operation():
        with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("""
                    DELETE FROM {}
                    WHERE {} = ANY(%s)
                """).format(sql.Identifier(table_name), sql.Identifier(id_column)),
                    (ids,),
                )

    run_with_retry(operation)


def execute_sql(statement: str) -> None:

    def operation():
        with psycopg.connect(**PSYCOPG_CONNECTION) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement)

    run_with_retry(operation)
