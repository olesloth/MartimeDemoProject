"""Run the Snowflake setup SQL file from the terminal."""

import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

SQL_FILE = Path(__file__).parent / "snowflake_setup.sql"


def main():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )

    sql = SQL_FILE.read_text()

    # Remove comment lines, then split on semicolons
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    clean_sql = "\n".join(lines)
    statements = [s.strip() for s in clean_sql.split(";") if s.strip()]

    cur = conn.cursor()
    for i, stmt in enumerate(statements, 1):
        print(f"[{i}] Running: {stmt[:80]}...")
        try:
            cur.execute(stmt)
            print(f"     OK")
        except Exception as e:
            print(f"     ERROR: {e}")

    conn.close()
    print("\nSetup complete.")


if __name__ == "__main__":
    main()
