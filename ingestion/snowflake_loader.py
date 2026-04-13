import logging

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from ingestion.config import SNOWFLAKE_CONFIG

logger = logging.getLogger(__name__)


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using config from environment."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def load_dataframe(
    df: pd.DataFrame,
    schema: str,
    table: str,
    overwrite: bool = False,
) -> int:
    """Load a pandas DataFrame into a Snowflake table.

    Args:
        df: DataFrame to load.
        schema: Target schema (e.g. "RAW").
        table: Target table name (e.g. "EU_MRV_EMISSIONS").
        overwrite: If True, truncate table before loading.

    Returns:
        Number of rows loaded.
    """
    if df.empty:
        logger.warning("Empty DataFrame, skipping load to %s.%s", schema, table)
        return 0

    conn = get_connection()
    try:
        conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['database']}.{schema}")

        if overwrite:
            conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {table}")

        success, num_chunks, num_rows, _ = write_pandas(
            conn,
            df,
            table_name=table,
            schema=schema,
            database=SNOWFLAKE_CONFIG["database"],
            quote_identifiers=False,
        )

        if success:
            logger.info(
                "Loaded %d rows into %s.%s (%d chunks)",
                num_rows, schema, table, num_chunks,
            )
        else:
            logger.error("Failed to load data into %s.%s", schema, table)

        return num_rows
    finally:
        conn.close()


def execute_query(query: str, params: dict | None = None) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    conn = get_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        conn.close()
