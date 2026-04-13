"""Pipeline orchestrator — runs full ingestion + dbt transformation.

Mirrors what Azure Data Factory would do: run ingestion scripts in
parallel, then trigger dbt build for transformations and tests.
"""

import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ingestion.load_ais_positions import ingest_ais_positions
from ingestion.load_eu_mrv import ingest_eu_mrv
from ingestion.load_ocean_weather import ingest_ocean_weather
from ingestion.load_vessel_registry import ingest_vessel_registry

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_maritime"


def run_ingestion() -> dict[str, int]:
    """Run all ingestion scripts in parallel, return row counts."""
    results = {}

    tasks = {
        "eu_mrv": ingest_eu_mrv,
        "ocean_weather": ingest_ocean_weather,
        "ais_positions": ingest_ais_positions,
        "vessel_registry": ingest_vessel_registry,
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(func): name
            for name, func in tasks.items()
        }

        for future in as_completed(futures):
            name = futures[future]
            try:
                rows = future.result()
                results[name] = rows
                logger.info("✓ %s: %d rows loaded", name, rows)
            except Exception:
                logger.exception("✗ %s: ingestion failed", name)
                results[name] = 0

    return results


def run_dbt_build() -> bool:
    """Run dbt build (models + tests) in the dbt project directory."""
    logger.info("Running dbt build...")

    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        logger.info("dbt build succeeded:\n%s", result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
        return True
    else:
        logger.error("dbt build failed:\n%s\n%s", result.stdout[-500:], result.stderr[-500:])
        return False


def main():
    """Run the full pipeline: ingest → transform."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("=" * 60)
    logger.info("Maritime Vessel Analytics Pipeline")
    logger.info("=" * 60)

    # Step 1: Ingest data from all sources
    logger.info("Step 1: Running data ingestion...")
    results = run_ingestion()

    total_rows = sum(results.values())
    logger.info("Ingestion complete: %d total rows across %d sources", total_rows, len(results))

    for source, rows in results.items():
        status = "✓" if rows > 0 else "✗"
        logger.info("  %s %s: %d rows", status, source, rows)

    if total_rows == 0:
        logger.error("No data ingested. Check your credentials and network.")
        sys.exit(1)

    # Step 2: Run dbt transformations
    logger.info("Step 2: Running dbt transformations...")
    dbt_ok = run_dbt_build()

    if dbt_ok:
        logger.info("=" * 60)
        logger.info("Pipeline complete! All data ingested and transformed.")
        logger.info("Start the API: uvicorn api.main:app --reload")
        logger.info("=" * 60)
    else:
        logger.warning("Pipeline completed with dbt errors. Check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
