from fastapi import APIRouter, Query

from api.database import fetch_all
from api.schemas import WeatherForecast

router = APIRouter(prefix="/weather", tags=["Weather"])


@router.get("/forecast", response_model=list[WeatherForecast])
def get_weather_forecast(
    region: str | None = Query(None, description="Filter by coastal region"),
    limit: int = Query(50, ge=1, le=500),
):
    """Get ocean weather conditions along Norwegian coast.

    Regions include: Ulsteinvik/Sunnmore, Bergen/Hordaland,
    Trondheim/Trondelag, Stavanger/Rogaland, Offshore Norwegian Sea.
    """
    conditions = ["1=1"]
    if region:
        conditions.append(f"region ILIKE '%{region}%'")

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            region,
            TO_VARCHAR(forecast_date, 'YYYY-MM-DD') AS forecast_date,
            avg_wave_height_m,
            max_wave_height_m,
            avg_wind_speed_knots,
            avg_water_temp_c,
            dominant_sea_state
        FROM MARTS.MART_WEATHER_CONDITIONS
        WHERE {where}
        ORDER BY forecast_date DESC, region
        LIMIT {limit}
    """
    return fetch_all(query)
