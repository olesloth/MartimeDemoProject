"""Tests for API schema validation — no database connection needed."""

from api.schemas import (
    EmissionsRecord,
    FleetEfficiency,
    HealthCheck,
    HullComparison,
    VesselSummary,
    WeatherForecast,
)


class TestSchemas:
    def test_vessel_summary_with_nulls(self):
        v = VesselSummary(
            imo_number="1234567",
            vessel_name=None,
            ship_type=None,
            is_ulstein_design=False,
            has_x_bow=False,
            design_type=None,
            latest_co2_kg_per_nm=None,
            efficiency_rank_pct=None,
        )
        assert v.imo_number == "1234567"
        assert v.is_ulstein_design is False

    def test_emissions_record(self):
        e = EmissionsRecord(
            imo_number="9684875",
            reporting_period=2023,
            total_co2_emissions_mt=15000.5,
            total_fuel_consumption_mt=5000.0,
            distance_travelled_nm=50000.0,
            time_at_sea_hours=4000.0,
            average_speed_knots=12.5,
            co2_kg_per_nm=300.01,
            fuel_kg_per_nm=100.0,
            eeoi=0.000012,
            efficiency_rank_pct=0.25,
        )
        assert e.reporting_period == 2023
        assert e.co2_kg_per_nm == 300.01

    def test_health_check(self):
        h = HealthCheck(status="healthy", database="MARITIME_ANALYTICS", warehouse="MARITIME_WH")
        assert h.status == "healthy"

    def test_hull_comparison(self):
        c = HullComparison(
            hull_type="X-BOW",
            vessel_count=5,
            avg_co2_kg_per_nm=250.0,
            avg_fuel_kg_per_nm=80.0,
            avg_efficiency_rank=0.15,
        )
        assert c.hull_type == "X-BOW"

    def test_weather_forecast(self):
        w = WeatherForecast(
            region="Ulsteinvik / Sunnmore",
            forecast_date="2024-01-15",
            avg_wave_height_m=2.5,
            max_wave_height_m=4.1,
            avg_wind_speed_knots=15.3,
            avg_water_temp_c=7.2,
            dominant_sea_state="Moderate",
        )
        assert w.region == "Ulsteinvik / Sunnmore"
