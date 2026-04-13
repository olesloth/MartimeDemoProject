"""Ingest ocean weather forecast data from MET Norway API.

Uses the free Meteorological Institute API (api.met.no) to fetch
ocean forecasts for coordinates along Norwegian shipping lanes.
No API key required — just an identifying User-Agent header per TOS.
"""

import logging
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

from ingestion.config import MET_NORWAY_BASE_URL, MET_NORWAY_USER_AGENT, WEATHER_GRID_POINTS
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)


def fetch_ocean_forecast(lat: float, lon: float) -> list[dict]:
    """Fetch ocean forecast for a single coordinate from MET Norway."""
    params = {"lat": round(lat, 4), "lon": round(lon, 4)}
    headers = {"User-Agent": MET_NORWAY_USER_AGENT}

    response = requests.get(MET_NORWAY_BASE_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    records = []

    for entry in data.get("properties", {}).get("timeseries", []):
        time_str = entry.get("time")
        details = entry.get("data", {}).get("instant", {}).get("details", {})

        records.append({
            "LATITUDE": lat,
            "LONGITUDE": lon,
            "FORECAST_TIME": datetime.fromisoformat(time_str.replace("Z", "+00:00")),
            "WAVE_HEIGHT_M": details.get("sea_surface_wave_height"),
            "WAVE_DIRECTION_DEG": details.get("sea_surface_wave_from_direction"),
            "WAVE_PERIOD_S": details.get("sea_surface_wave_period_at_variance_spectral_density_maximum"),
            "WIND_SPEED_MS": details.get("wind_speed"),
            "WIND_DIRECTION_DEG": details.get("wind_from_direction"),
            "WATER_TEMPERATURE_C": details.get("sea_water_temperature"),
            "CURRENT_SPEED_MS": details.get("sea_water_speed"),
            "CURRENT_DIRECTION_DEG": details.get("sea_water_to_direction"),
        })

    return records


def ingest_ocean_weather(grid_points: list[tuple] | None = None) -> int:
    """Fetch ocean forecasts for all grid points and load to Snowflake.

    Args:
        grid_points: List of (lat, lon) tuples. Defaults to WEATHER_GRID_POINTS.

    Returns:
        Total rows loaded.
    """
    if grid_points is None:
        grid_points = WEATHER_GRID_POINTS

    all_records = []

    for lat, lon in grid_points:
        try:
            logger.info("Fetching ocean forecast for (%.2f, %.2f)", lat, lon)
            records = fetch_ocean_forecast(lat, lon)
            all_records.extend(records)
            logger.info("Got %d forecast entries for (%.2f, %.2f)", len(records), lat, lon)
        except Exception:
            logger.exception("Failed to fetch forecast for (%.2f, %.2f)", lat, lon)

    if not all_records:
        logger.warning("No weather data collected")
        return 0

    df = pd.DataFrame(all_records)
    df["FORECAST_TIME"] = pd.to_datetime(df["FORECAST_TIME"], utc=True).dt.tz_localize(None)

    return load_dataframe(df, schema="RAW", table="OCEAN_WEATHER", overwrite=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    rows = ingest_ocean_weather()
    logger.info("Ocean weather ingestion complete: %d rows", rows)
    sys.exit(0 if rows > 0 else 1)
