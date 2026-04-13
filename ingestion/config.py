import os

from dotenv import load_dotenv

load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "MARITIME_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "MARITIME_ANALYTICS"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}

# AISStream.io
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")

# MET Norway API (free, no key needed — just User-Agent)
MET_NORWAY_BASE_URL = "https://api.met.no/weatherapi/oceanforecast/2.0/complete"
MET_NORWAY_USER_AGENT = "UlsteinMaritimeAnalytics/1.0 github.com/demo (contact@example.com)"

# Norwegian Sea bounding box for AIS filtering
# Covers major shipping lanes where Ulstein vessels operate
AIS_BOUNDING_BOX = {
    "lat_min": 58.0,
    "lat_max": 65.0,
    "lon_min": 0.0,
    "lon_max": 10.0,
}

# AIS capture duration in seconds (default 10 minutes)
AIS_CAPTURE_DURATION = int(os.getenv("AIS_CAPTURE_DURATION", "600"))

# MET Norway forecast grid points along Norwegian coast
WEATHER_GRID_POINTS = [
    (62.47, 5.15),   # Ulsteinvik area
    (60.39, 5.32),   # Bergen
    (63.43, 10.39),  # Trondheim
    (58.97, 5.73),   # Stavanger
    (61.50, 3.50),   # Offshore Norwegian Sea
    (64.00, 7.00),   # Mid-Norway offshore
    (59.50, 4.00),   # North Sea
]
