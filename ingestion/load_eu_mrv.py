"""Ingest EU MRV/THETIS ship emissions data into Snowflake.

The EMSA THETIS-MRV system does not offer a public CSV API. This script
supports two ingestion modes:

1. **Local file** (recommended): Download the Excel/CSV manually from
   https://mrv.emsa.europa.eu/#public/emission-report and place it in
   data/eu_mrv_{year}.xlsx. The script reads it from there.

2. **THETIS export endpoint**: Attempts to download from the EMSA export
   URL. This may fail if EMSA changes their endpoint or blocks automated
   requests.

This is the richest free maritime dataset available (~5,000+ vessels
per reporting period with CO2 emissions, fuel consumption, and
efficiency data).
"""

import logging
import sys
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
import requests

from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

# EMSA THETIS-MRV export endpoints to try
THETIS_EXPORT_URLS = [
    "https://mrv.emsa.europa.eu/api/public-emission-report/reporting-period-document/binary/csv/{period}",
    "https://mrv.emsa.europa.eu/api/public-emission-report/reporting-period-document/binary/xlsx/{period}",
]

# Column mapping from Excel/CSV headers to our Snowflake table columns.
# Uses case-insensitive matching and supports both old CSV and new Excel formats.
COLUMN_MAPPING = {
    "IMO Number": "IMO_NUMBER",
    "Name": "VESSEL_NAME",
    "Ship type": "SHIP_TYPE",
    "Ship Type": "SHIP_TYPE",
    "Reporting Period": "REPORTING_PERIOD",
    "Technical efficiency": "TECHNICAL_EFFICIENCY",
    "Port of Registry": "PORT_OF_REGISTRY",
    "Home Port": "HOME_PORT",
    "Ice Class": "ICE_CLASS",
    "DoC issue date": "DOC_ISSUE_DATE",
    "DoC expiry date": "DOC_EXPIRY_DATE",
    "Verifier Name": "VERIFIER_NAME",
    "Verifier NAB": "VERIFIER_NAB",
    "Verifier Address": "VERIFIER_ADDRESS",
    "Verifier City": "VERIFIER_CITY",
    "Verifier Country": "VERIFIER_COUNTRY",
    "Verifier Accreditation number": "VERIFIER_ACCREDITATION_NUMBER",
    # Fuel consumption
    "Total fuel consumption [m tonnes]": "A_TOTAL_FUEL_CONSUMPTION_MT",
    "A - Total fuel consumption [m tonnes]": "A_TOTAL_FUEL_CONSUMPTION_MT",
    "Fuel consumptions assigned to On laden [m tonnes]": "A_ON_LADEN_VOYAGES_MT",
    "A - Fuel consumptions assigned to On laden voyages [m tonnes]": "A_ON_LADEN_VOYAGES_MT",
    # CO2 emissions (handles both CO₂ and CO² unicode variants)
    "Total CO\u2082 emissions [m tonnes]": "TOTAL_CO2_EMISSIONS_MT",
    "Total CO₂ emissions [m tonnes]": "TOTAL_CO2_EMISSIONS_MT",
    "CO\u2082 emissions from all voyages between ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_BETWEEN_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO₂ emissions from all voyages between ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_BETWEEN_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions from all voyages which departed from ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_DEPARTED_FROM_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO₂ emissions from all voyages which departed from ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_DEPARTED_FROM_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions from all voyages to ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_TO_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO₂ emissions from all voyages to ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_TO_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions which occurred within ports under a MS jurisdiction at berth [m tonnes]": "CO2_EMISSIONS_WITHIN_PORTS_UNDER_MS_JURISDICTION_AT_BERTH_MT",
    "CO₂ emissions which occurred within ports under a MS jurisdiction at berth [m tonnes]": "CO2_EMISSIONS_WITHIN_PORTS_UNDER_MS_JURISDICTION_AT_BERTH_MT",
    "CO\u2082 emissions assigned to Passenger transport [m tonnes]": "CO2_EMISSIONS_PASSENGER_TRANSPORT_MT",
    "CO₂ emissions assigned to Passenger transport [m tonnes]": "CO2_EMISSIONS_PASSENGER_TRANSPORT_MT",
    "CO\u2082 emissions assigned to Freight transport [m tonnes]": "CO2_EMISSIONS_FREIGHT_TRANSPORT_MT",
    "CO₂ emissions assigned to Freight transport [m tonnes]": "CO2_EMISSIONS_FREIGHT_TRANSPORT_MT",
    "CO\u2082 emissions assigned to On laden [m tonnes]": "CO2_EMISSIONS_ON_LADEN_VOYAGES_MT",
    "CO₂ emissions assigned to On laden [m tonnes]": "CO2_EMISSIONS_ON_LADEN_VOYAGES_MT",
    "CO\u2082 emissions on laden voyages [m tonnes]": "CO2_EMISSIONS_ON_LADEN_VOYAGES_MT",
    # Time and distance
    "Annual Time spent at sea [hours]": "TIME_AT_SEA_HOURS",
    "Annual Total time spent at sea [hours]": "TIME_AT_SEA_HOURS",
    "Annual average Fuel consumption per distance [kg / n mile]": "ANNUAL_AVERAGE_CO2_EMISSIONS_PER_DISTANCE_KG_PER_NM",
    "Annual average CO\u2082 emissions per distance [kg CO\u2082 / n mile]": "ANNUAL_AVERAGE_CO2_EMISSIONS_PER_DISTANCE_KG_PER_NM",
    "Annual average CO₂ emissions per distance [kg CO₂ / n mile]": "ANNUAL_AVERAGE_CO2_EMISSIONS_PER_DISTANCE_KG_PER_NM",
    "Through water speed [knots]": "AVERAGE_SPEED_KNOTS",
    "Deadweight [tonnes]": "DEADWEIGHT_TONNES",
    "Gross tonnage": "GROSS_TONNAGE",
    "EEOI (Annual average)": "ANNUAL_AVERAGE_EEOI",
    "Distance travelled [n miles]": "DISTANCE_TRAVELLED_NM",
    # Monitoring methods (Excel uses single letters A/B/C/D)
    "A": "MONITORING_METHOD_A",
    "B": "MONITORING_METHOD_B",
    "C": "MONITORING_METHOD_C",
    "D": "MONITORING_METHOD_D",
    "Monitoring Method(s) - A": "MONITORING_METHOD_A",
    "Monitoring Method(s) - B": "MONITORING_METHOD_B",
    "Monitoring Method(s) - C": "MONITORING_METHOD_C",
    "Monitoring Method(s) - D": "MONITORING_METHOD_D",
}


def _find_local_file(reporting_period: int) -> Path | None:
    """Look for a locally downloaded EU MRV file."""
    DATA_DIR.mkdir(exist_ok=True)
    for ext in ["csv", "xlsx", "xls"]:
        for pattern in [
            f"eu_mrv_{reporting_period}.{ext}",
            f"*{reporting_period}*.{ext}",
        ]:
            matches = list(DATA_DIR.glob(pattern))
            if matches:
                return matches[0]
    return None


def _read_file(path: Path) -> pd.DataFrame:
    """Read a CSV or Excel file into a DataFrame.

    EMSA Excel files have merged header rows — real column names are in row 3
    (0-indexed row 2). We detect this by checking if the first column name
    looks like a category header (e.g. 'Ship') rather than 'IMO Number'.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, encoding="utf-8")
    elif suffix in (".xlsx", ".xls"):
        # Try default header first
        df = pd.read_excel(path)
        # If first column is a merged category header, skip to row 2
        if df.columns[0] in ("Ship", "ship") or "IMO" not in df.columns[0]:
            df = pd.read_excel(path, header=2)
        return df
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def _try_download(reporting_period: int) -> pd.DataFrame | None:
    """Attempt to download from EMSA THETIS-MRV export endpoints."""
    for url_template in THETIS_EXPORT_URLS:
        url = url_template.format(period=reporting_period)
        logger.info("Trying THETIS export: %s", url)
        try:
            response = requests.get(url, timeout=120, headers={
                "User-Agent": "Mozilla/5.0 (Maritime Analytics Demo)"
            })
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "csv" in content_type or url.endswith("csv"):
                return pd.read_csv(StringIO(response.text), encoding="utf-8")
            else:
                return pd.read_excel(BytesIO(response.content))
        except Exception as e:
            logger.warning("THETIS export failed for %s: %s", url, e)

    return None


def download_eu_mrv(reporting_period: int = 2023) -> pd.DataFrame:
    """Load EU MRV data from local file or THETIS-MRV export.

    Checks for local files first (data/eu_mrv_{year}.xlsx), then
    falls back to attempting the THETIS export endpoint.
    """
    # Try local file first
    local_file = _find_local_file(reporting_period)
    if local_file:
        logger.info("Loading EU MRV data from local file: %s", local_file)
        df = _read_file(local_file)
        logger.info("Loaded %d rows from %s", len(df), local_file.name)
        return df

    # Try THETIS export
    logger.info("No local file found for %d, trying THETIS export...", reporting_period)
    df = _try_download(reporting_period)
    if df is not None:
        logger.info("Downloaded %d rows for period %d", len(df), reporting_period)
        return df

    # Neither worked
    raise FileNotFoundError(
        f"No EU MRV data available for {reporting_period}. "
        f"Download the Excel file manually from "
        f"https://mrv.emsa.europa.eu/#public/emission-report "
        f"and save it as data/eu_mrv_{reporting_period}.xlsx"
    )


def transform_eu_mrv(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform EU MRV data for Snowflake loading."""
    # Drop duplicate columns (Excel has D and D.1)
    df = df.loc[:, ~df.columns.duplicated()]

    # Rename columns to match our schema
    rename_map = {}
    for csv_col, sf_col in COLUMN_MAPPING.items():
        for actual_col in df.columns:
            if actual_col.strip() == csv_col and actual_col not in rename_map:
                rename_map[actual_col] = sf_col
                break

    df = df.rename(columns=rename_map)

    # Keep only mapped columns that exist, deduplicate
    valid_cols = list(dict.fromkeys(c for c in COLUMN_MAPPING.values() if c in df.columns))
    df = df[valid_cols].copy()

    # Drop any remaining duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Convert monitoring method columns to boolean
    for col in ["MONITORING_METHOD_A", "MONITORING_METHOD_B", "MONITORING_METHOD_C", "MONITORING_METHOD_D"]:
        if col in df.columns:
            series = df[col]
            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            df[col] = series.apply(lambda x: True if str(x).strip().upper() in ("YES", "TRUE", "1", "X") else False)

    # Convert numeric columns
    numeric_cols = [c for c in df.columns if any(
        kw in c for kw in ["_MT", "_NM", "HOURS", "KNOTS", "TONNES", "TONNAGE", "EEOI", "DISTANCE", "KG_PER"]
    )]
    for col in numeric_cols:
        series = df[col]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        df[col] = pd.to_numeric(series, errors="coerce")

    # Convert reporting period to int
    if "REPORTING_PERIOD" in df.columns:
        df["REPORTING_PERIOD"] = pd.to_numeric(df["REPORTING_PERIOD"], errors="coerce").astype("Int64")

    logger.info("Transformed EU MRV data: %d rows, %d columns", len(df), len(df.columns))
    return df


def ingest_eu_mrv(reporting_periods: list[int] | None = None) -> int:
    """Full ingestion pipeline for EU MRV data.

    Args:
        reporting_periods: Years to download. Defaults to [2022, 2023].

    Returns:
        Total rows loaded.
    """
    if reporting_periods is None:
        reporting_periods = [2023, 2024]

    total_rows = 0
    for period in reporting_periods:
        try:
            df = download_eu_mrv(period)
            df = transform_eu_mrv(df)
            rows = load_dataframe(df, schema="RAW", table="EU_MRV_EMISSIONS")
            total_rows += rows
        except Exception:
            logger.exception("Failed to ingest EU MRV data for period %d", period)

    return total_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    rows = ingest_eu_mrv()
    logger.info("EU MRV ingestion complete: %d total rows", rows)
    sys.exit(0 if rows > 0 else 1)
