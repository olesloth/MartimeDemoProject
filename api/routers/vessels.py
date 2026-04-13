from fastapi import APIRouter, Query

from api.database import fetch_all, fetch_one
from api.schemas import AISPosition, EmissionsRecord, VesselDetail, VesselSummary

router = APIRouter(prefix="/vessels", tags=["Vessels"])


@router.get("", response_model=list[VesselSummary])
def list_vessels(
    ship_type: str | None = Query(None, description="Filter by ship type"),
    has_x_bow: bool | None = Query(None, description="Filter by X-BOW hull design"),
    ulstein_only: bool = Query(False, description="Show only Ulstein-designed vessels"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List vessels with latest efficiency metrics."""
    conditions = ["1=1"]
    if ship_type:
        conditions.append(f"ship_type = '{ship_type}'")
    if has_x_bow is not None:
        conditions.append(f"has_x_bow = {has_x_bow}")
    if ulstein_only:
        conditions.append("is_ulstein_design = true")

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            imo_number, vessel_name, ship_type,
            is_ulstein_design, has_x_bow, design_type,
            co2_kg_per_nm AS latest_co2_kg_per_nm,
            efficiency_rank_pct
        FROM MARTS.MART_VESSEL_EFFICIENCY
        WHERE {where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY imo_number ORDER BY reporting_period DESC) = 1
        ORDER BY vessel_name
        LIMIT {limit} OFFSET {offset}
    """
    return fetch_all(query)


@router.get("/{imo_number}", response_model=VesselDetail)
def get_vessel(imo_number: str):
    """Get detailed vessel information."""
    query = """
        SELECT
            imo_number, vessel_name, ship_type,
            is_ulstein_design, has_x_bow, design_type,
            hull_type, ship_category,
            deadweight_tonnes, gross_tonnage
        FROM MARTS.MART_VESSEL_EFFICIENCY
        WHERE imo_number = %(imo)s
        QUALIFY ROW_NUMBER() OVER (ORDER BY reporting_period DESC) = 1
    """
    return fetch_one(query, {"imo": imo_number})


@router.get("/{imo_number}/emissions", response_model=list[EmissionsRecord])
def get_vessel_emissions(
    imo_number: str,
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
):
    """Get emissions time series for a vessel."""
    conditions = ["imo_number = %(imo)s"]
    params = {"imo": imo_number}

    if year_from:
        conditions.append("reporting_period >= %(year_from)s")
        params["year_from"] = year_from
    if year_to:
        conditions.append("reporting_period <= %(year_to)s")
        params["year_to"] = year_to

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            imo_number, reporting_period,
            total_co2_emissions_mt, total_fuel_consumption_mt,
            distance_travelled_nm, time_at_sea_hours,
            average_speed_knots, co2_kg_per_nm,
            fuel_kg_per_nm, eeoi, efficiency_rank_pct
        FROM MARTS.MART_VESSEL_EFFICIENCY
        WHERE {where}
        ORDER BY reporting_period
    """
    return fetch_all(query, params)


@router.get("/{imo_number}/positions", response_model=list[AISPosition])
def get_vessel_positions(imo_number: str, limit: int = Query(100, ge=1, le=1000)):
    """Get latest AIS positions for a vessel."""
    query = """
        SELECT
            mmsi, imo_number, vessel_name,
            latitude, longitude, speed_knots,
            course, heading, destination,
            TO_VARCHAR(received_at, 'YYYY-MM-DD HH24:MI:SS') AS received_at
        FROM MARTS.MART_VESSEL_VOYAGES
        WHERE imo_number = %(imo)s
        ORDER BY received_at DESC
        LIMIT %(limit)s
    """
    return fetch_all(query, {"imo": imo_number, "limit": limit})
