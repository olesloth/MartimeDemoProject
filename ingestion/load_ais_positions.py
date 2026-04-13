"""Ingest AIS vessel position data from AISStream.io WebSocket.

Connects to the free AISStream.io WebSocket API and captures a
time-bounded snapshot of vessel positions in the Norwegian Sea.
Demonstrates real-time data acquisition and sensor data handling.

Requires a free API key from aisstream.io (register with GitHub).
"""

import json
import logging
import ssl
import sys
import time
from datetime import datetime, timezone

import certifi
import pandas as pd
import websocket

from ingestion.config import AIS_BOUNDING_BOX, AIS_CAPTURE_DURATION, AISSTREAM_API_KEY
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)


def capture_ais_snapshot(
    api_key: str,
    bounding_box: dict,
    duration_seconds: int,
) -> list[dict]:
    """Capture AIS messages for a bounded time window.

    Args:
        api_key: AISStream.io API key.
        bounding_box: Dict with lat_min, lat_max, lon_min, lon_max.
        duration_seconds: How long to capture data.

    Returns:
        List of parsed AIS position records.
    """
    if not api_key:
        logger.error("AISSTREAM_API_KEY not set. Register at aisstream.io (free).")
        return []

    subscribe_msg = json.dumps({
        "APIKey": api_key,
        "BoundingBoxes": [[
            [bounding_box["lat_min"], bounding_box["lon_min"]],
            [bounding_box["lat_max"], bounding_box["lon_max"]],
        ]],
        "FiltersShipMMSI": [],
        "FilterMessageTypes": ["PositionReport"],
    })

    records = []
    start_time = time.time()

    def on_message(ws, message):
        if time.time() - start_time >= duration_seconds:
            ws.close()
            return

        try:
            msg = json.loads(message)
            meta = msg.get("MetaData", {})
            position = msg.get("Message", {}).get("PositionReport", {})

            if not position:
                return

            records.append({
                "MMSI": str(meta.get("MMSI", "")),
                "IMO_NUMBER": str(meta.get("IMO", "")),
                "VESSEL_NAME": meta.get("ShipName", "").strip(),
                "SHIP_TYPE": meta.get("ShipType", None),
                "LATITUDE": position.get("Latitude"),
                "LONGITUDE": position.get("Longitude"),
                "SPEED_KNOTS": position.get("Sog"),
                "COURSE": position.get("Cog"),
                "HEADING": position.get("TrueHeading"),
                "DESTINATION": meta.get("Destination", "").strip(),
                "NAVIGATION_STATUS": position.get("NavigationalStatus"),
                "MESSAGE_TYPE": msg.get("MessageType"),
                "RECEIVED_AT": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception:
            logger.exception("Error parsing AIS message")

    def on_open(ws):
        logger.info(
            "AIS WebSocket connected. Capturing for %d seconds in bounding box: %s",
            duration_seconds, bounding_box,
        )
        ws.send(subscribe_msg)

    def on_error(ws, error):
        logger.error("AIS WebSocket error: %s", error)

    def on_close(ws, close_status_code, close_msg):
        logger.info("AIS WebSocket closed. Captured %d positions.", len(records))

    ws = websocket.WebSocketApp(
        "wss://stream.aisstream.io/v0/stream",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Run with a timeout slightly longer than capture duration
    sslopt = {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()}
    ws.run_forever(ping_interval=30, ping_timeout=10, sslopt=sslopt)

    return records


def ingest_ais_positions(
    duration_seconds: int | None = None,
) -> int:
    """Capture AIS positions and load to Snowflake.

    Args:
        duration_seconds: Override capture duration.

    Returns:
        Number of rows loaded.
    """
    duration = duration_seconds or AIS_CAPTURE_DURATION

    logger.info("Starting AIS capture for %d seconds...", duration)
    records = capture_ais_snapshot(AISSTREAM_API_KEY, AIS_BOUNDING_BOX, duration)

    if not records:
        logger.warning("No AIS positions captured")
        return 0

    df = pd.DataFrame(records)
    df["RECEIVED_AT"] = pd.to_datetime(df["RECEIVED_AT"])

    # Deduplicate by MMSI + timestamp (keep latest per vessel per second)
    df = df.drop_duplicates(subset=["MMSI", "RECEIVED_AT"], keep="last")

    logger.info("Captured %d unique AIS positions from %d vessels",
                len(df), df["MMSI"].nunique())

    return load_dataframe(df, schema="RAW", table="AIS_POSITIONS")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    rows = ingest_ais_positions()
    logger.info("AIS ingestion complete: %d rows", rows)
    sys.exit(0 if rows > 0 else 1)
