"""Ingest EU MRV/THETIS ship emissions data into Snowflake.

Downloads the public CSV from EMSA's THETIS-MRV system containing
CO2 emissions, fuel consumption, and efficiency data for vessels
operating in EU waters. This is the richest free maritime dataset
available (~50K rows per reporting period).
"""

import logging
import sys
from io import StringIO

import pandas as pd
import requests

from ingestion.config import EU_MRV_URL
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)

# Column mapping from CSV headers to our Snowflake table columns
COLUMN_MAPPING = {
    "IMO Number": "IMO_NUMBER",
    "Name": "VESSEL_NAME",
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
    "A - Total fuel consumption [m tonnes]": "A_TOTAL_FUEL_CONSUMPTION_MT",
    "A - Fuel consumptions assigned to On laden voyages [m tonnes]": "A_ON_LADEN_VOYAGES_MT",
    "Total CO\u2082 emissions [m tonnes]": "TOTAL_CO2_EMISSIONS_MT",
    "CO\u2082 emissions from all voyages between ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_BETWEEN_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions from all voyages which departed from ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_DEPARTED_FROM_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions from all voyages to ports under a MS jurisdiction [m tonnes]": "CO2_EMISSIONS_FROM_ALL_VOYAGES_TO_PORTS_UNDER_MS_JURISDICTION_MT",
    "CO\u2082 emissions which occurred within ports under a MS jurisdiction at berth [m tonnes]": "CO2_EMISSIONS_WITHIN_PORTS_UNDER_MS_JURISDICTION_AT_BERTH_MT",
    "CO\u2082 emissions assigned to Passenger transport [m tonnes]": "CO2_EMISSIONS_PASSENGER_TRANSPORT_MT",
    "CO\u2082 emissions assigned to Freight transport [m tonnes]": "CO2_EMISSIONS_FREIGHT_TRANSPORT_MT",
    "Fuel consumptions assigned to On laden voyages [m tonnes]": "FUEL_CONSUMPTIONS_ASSIGNED_TO_ON_LADEN_VOYAGES_MT",
    "Total fuel consumption on voyages between ports [m tonnes]": "TOTAL_FUEL_CONSUMPTION_ON_VOYAGES_BETWEEN_PORTS_MT",
    "Fuel consumption: Departed from ports [m tonnes]": "FUEL_CONSUMPTION_DEPARTED_FROM_PORTS_MT",
    "Fuel consumption: To ports [m tonnes]": "FUEL_CONSUMPTION_TO_PORTS_MT",
    "Fuel consumption: Within ports at berth [m tonnes]": "FUEL_CONSUMPTION_WITHIN_PORTS_AT_BERTH_MT",
    "Fuel consumption: Passenger transport [m tonnes]": "FUEL_CONSUMPTION_PASSENGER_TRANSPORT_MT",
    "Fuel consumption: Freight transport [m tonnes]": "FUEL_CONSUMPTION_FREIGHT_TRANSPORT_MT",
    "Annual Total time spent at sea [hours]": "TIME_AT_SEA_HOURS",
    "Annual average Fuel consumption per distance [kg / n mile]": "ANNUAL_AVERAGE_CO2_EMISSIONS_PER_DISTANCE_KG_PER_NM",
    "Annual average CO\u2082 emissions per distance [kg CO\u2082 / n mile]": "ANNUAL_AVERAGE_CO2_EMISSIONS_PER_DISTANCE_KG_PER_NM",
    "Through water speed [knots]": "AVERAGE_SPEED_KNOTS",
    "Deadweight [tonnes]": "DEADWEIGHT_TONNES",
    "Gross tonnage": "GROSS_TONNAGE",
    "EEOI (Annual average)": "ANNUAL_AVERAGE_EEOI",
    "Distance travelled [n miles]": "DISTANCE_TRAVELLED_NM",
    "CO\u2082 emissions on laden voyages [m tonnes]": "CO2_EMISSIONS_ON_LADEN_VOYAGES_MT",
    "Monitoring Method(s) - A": "MONITORING_METHOD_A",
    "Monitoring Method(s) - B": "MONITORING_METHOD_B",
    "Monitoring Method(s) - C": "MONITORING_METHOD_C",
    "Monitoring Method(s) - D": "MONITORING_METHOD_D",
}


def download_eu_mrv(reporting_period: int = 2023) -> pd.DataFrame:
    """Download EU MRV emissions CSV for a given reporting period."""
    url = f"{EU_MRV_URL}{reporting_period}"
    logger.info("Downloading EU MRV data for period %d from %s", reporting_period, url)

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text), encoding="utf-8")
    logger.info("Downloaded %d rows for reporting period %d", len(df), reporting_period)
    return df


def transform_eu_mrv(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform EU MRV data for Snowflake loading."""
    # Rename columns to match our schema
    rename_map = {}
    for csv_col, sf_col in COLUMN_MAPPING.items():
        for actual_col in df.columns:
            if actual_col.strip() == csv_col:
                rename_map[actual_col] = sf_col
                break

    df = df.rename(columns=rename_map)

    # Keep only mapped columns that exist
    valid_cols = [c for c in COLUMN_MAPPING.values() if c in df.columns]
    df = df[valid_cols].copy()

    # Convert monitoring method columns to boolean
    for col in ["MONITORING_METHOD_A", "MONITORING_METHOD_B", "MONITORING_METHOD_C", "MONITORING_METHOD_D"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().upper() in ("YES", "TRUE", "1", "X") else False)

    # Convert numeric columns
    numeric_cols = [c for c in df.columns if any(
        kw in c for kw in ["_MT", "_NM", "HOURS", "KNOTS", "TONNES", "TONNAGE", "EEOI", "DISTANCE", "KG_PER"]
    )]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

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
        reporting_periods = [2022, 2023]

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
