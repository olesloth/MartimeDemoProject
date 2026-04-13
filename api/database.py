from contextlib import contextmanager

import snowflake.connector

from api.config import settings


def _get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        role=settings.snowflake_role,
    )


@contextmanager
def get_cursor():
    """Yield a DictCursor connected to Snowflake, auto-closing."""
    conn = _get_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        yield cur
    finally:
        conn.close()


def fetch_all(query: str, params: dict | None = None) -> list[dict]:
    """Execute query and return all rows as list of dicts."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_one(query: str, params: dict | None = None) -> dict | None:
    """Execute query and return first row or None."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()
