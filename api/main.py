from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import fleet, vessels, weather
from api.schemas import HealthCheck
from api.config import settings

app = FastAPI(
    title="Maritime Vessel Analytics API",
    description=(
        "REST API for vessel efficiency analytics, fleet comparisons, "
        "and ocean weather data. Built as a demo for Ulstein Group's "
        "Data Engineer position — showcasing Python, Snowflake, dbt, "
        "and FastAPI integration."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(vessels.router, prefix="/api/v1")
app.include_router(fleet.router, prefix="/api/v1")
app.include_router(weather.router, prefix="/api/v1")


@app.get("/api/v1/health", response_model=HealthCheck, tags=["System"])
def health_check():
    """Check API and database connectivity."""
    from api.database import fetch_one

    try:
        result = fetch_one("SELECT CURRENT_WAREHOUSE() AS wh")
        return HealthCheck(
            status="healthy",
            database=settings.snowflake_database,
            warehouse=result["WH"] if result else "unknown",
        )
    except Exception as e:
        return HealthCheck(
            status=f"unhealthy: {e}",
            database=settings.snowflake_database,
            warehouse="unreachable",
        )
