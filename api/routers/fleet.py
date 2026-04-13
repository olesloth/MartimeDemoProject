from fastapi import APIRouter, Query

from api.database import fetch_all
from api.schemas import FleetEfficiency, HullComparison

router = APIRouter(prefix="/fleet", tags=["Fleet Analytics"])


@router.get("/efficiency", response_model=list[FleetEfficiency])
def get_fleet_efficiency(
    ship_type: str | None = Query(None, description="Filter by ship type"),
    reporting_period: int | None = Query(None, description="Filter by year"),
):
    """Get aggregated fleet efficiency statistics."""
    conditions = ["1=1"]
    if ship_type:
        conditions.append(f"ship_type = '{ship_type}'")
    if reporting_period:
        conditions.append(f"reporting_period = {reporting_period}")

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            ship_type, hull_type, reporting_period,
            vessel_count,
            avg_co2_kg_per_nm,
            median_co2_kg_per_nm,
            total_co2_mt,
            avg_speed_knots,
            avg_eeoi
        FROM MARTS.MART_FLEET_OVERVIEW
        WHERE {where}
        ORDER BY reporting_period DESC, ship_type
    """
    return fetch_all(query)


@router.get("/comparison", response_model=list[HullComparison])
def compare_hull_types(
    reporting_period: int | None = Query(None, description="Filter by year"),
    ship_type: str | None = Query(None, description="Filter by ship type for fairer comparison"),
):
    """Compare X-BOW vs conventional hull efficiency.

    This is the key business insight: does the Ulstein X-BOW design
    result in better fuel efficiency and lower emissions?
    """
    conditions = ["distance_travelled_nm > 0"]
    if reporting_period:
        conditions.append(f"reporting_period = {reporting_period}")
    if ship_type:
        conditions.append(f"ship_type = '{ship_type}'")

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            hull_type,
            COUNT(DISTINCT imo_number) AS vessel_count,
            ROUND(AVG(co2_kg_per_nm), 2) AS avg_co2_kg_per_nm,
            ROUND(AVG(fuel_kg_per_nm), 2) AS avg_fuel_kg_per_nm,
            ROUND(AVG(efficiency_rank_pct), 3) AS avg_efficiency_rank
        FROM MARTS.MART_VESSEL_EFFICIENCY
        WHERE {where}
        GROUP BY hull_type
        ORDER BY avg_co2_kg_per_nm
    """
    return fetch_all(query)
