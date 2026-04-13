"""Load the curated Ulstein vessel registry seed data into Snowflake.

This is a manually curated CSV of Ulstein-designed vessels, loaded as
a reference table for joining with emissions data. It flags X-BOW
designs and vessel types to enable hull efficiency comparisons.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)

SEED_FILE = Path(__file__).parent.parent / "dbt_maritime" / "seeds" / "ulstein_vessel_registry.csv"


def ingest_vessel_registry() -> int:
    """Load vessel registry seed CSV into Snowflake RAW schema."""
    if not SEED_FILE.exists():
        logger.error("Seed file not found: %s", SEED_FILE)
        return 0

    df = pd.read_csv(SEED_FILE)

    # Uppercase column names to match Snowflake convention
    df.columns = [c.upper() for c in df.columns]

    logger.info("Loaded %d vessels from seed file", len(df))
    return load_dataframe(df, schema="RAW", table="VESSEL_REGISTRY", overwrite=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    rows = ingest_vessel_registry()
    logger.info("Vessel registry ingestion complete: %d rows", rows)
    sys.exit(0 if rows > 0 else 1)
