from pydantic import BaseModel


class VesselSummary(BaseModel):
    imo_number: str
    vessel_name: str | None
    ship_type: str | None
    is_ulstein_design: bool
    has_x_bow: bool
    design_type: str | None
    latest_co2_kg_per_nm: float | None
    efficiency_rank_pct: float | None


class VesselDetail(BaseModel):
    imo_number: str
    vessel_name: str | None
    ship_type: str | None
    is_ulstein_design: bool
    has_x_bow: bool
    design_type: str | None
    hull_type: str | None
    ship_category: str | None
    deadweight_tonnes: float | None
    gross_tonnage: float | None


class EmissionsRecord(BaseModel):
    imo_number: str
    reporting_period: int
    total_co2_emissions_mt: float | None
    total_fuel_consumption_mt: float | None
    distance_travelled_nm: float | None
    time_at_sea_hours: float | None
    average_speed_knots: float | None
    co2_kg_per_nm: float | None
    fuel_kg_per_nm: float | None
    eeoi: float | None
    efficiency_rank_pct: float | None


class AISPosition(BaseModel):
    mmsi: str
    imo_number: str | None
    vessel_name: str | None
    latitude: float
    longitude: float
    speed_knots: float | None
    course: float | None
    heading: float | None
    destination: str | None
    received_at: str | None


class FleetEfficiency(BaseModel):
    ship_type: str
    hull_type: str
    reporting_period: int
    vessel_count: int
    avg_co2_kg_per_nm: float | None
    median_co2_kg_per_nm: float | None
    total_co2_mt: float | None
    avg_speed_knots: float | None
    avg_eeoi: float | None


class HullComparison(BaseModel):
    hull_type: str
    vessel_count: int
    avg_co2_kg_per_nm: float | None
    avg_fuel_kg_per_nm: float | None
    avg_efficiency_rank: float | None


class WeatherForecast(BaseModel):
    region: str
    forecast_date: str
    avg_wave_height_m: float | None
    max_wave_height_m: float | None
    avg_wind_speed_knots: float | None
    avg_water_temp_c: float | None
    dominant_sea_state: str | None


class HealthCheck(BaseModel):
    status: str
    database: str
    warehouse: str
