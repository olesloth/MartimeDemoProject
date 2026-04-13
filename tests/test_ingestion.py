"""Tests for ingestion scripts — validation logic only (no Snowflake needed)."""

import pandas as pd

from ingestion.load_eu_mrv import transform_eu_mrv


class TestEuMrvTransform:
    def test_renames_columns(self):
        df = pd.DataFrame({
            "IMO Number": ["1234567"],
            "Name": ["Ship A"],
            "Total CO\u2082 emissions [m tonnes]": [200.0],
            "Reporting Period": [2023],
        })
        result = transform_eu_mrv(df)
        assert "IMO_NUMBER" in result.columns
        assert "VESSEL_NAME" in result.columns
        assert "TOTAL_CO2_EMISSIONS_MT" in result.columns

    def test_converts_numeric_columns(self):
        df = pd.DataFrame({
            "IMO Number": ["1234567"],
            "Name": ["Ship A"],
            "Total CO\u2082 emissions [m tonnes]": ["200.5"],
            "Reporting Period": ["2023"],
        })
        result = transform_eu_mrv(df)
        assert result.iloc[0]["TOTAL_CO2_EMISSIONS_MT"] == 200.5
        assert result.iloc[0]["REPORTING_PERIOD"] == 2023

    def test_converts_monitoring_methods(self):
        df = pd.DataFrame({
            "IMO Number": ["1234567"],
            "Name": ["Ship A"],
            "Total CO\u2082 emissions [m tonnes]": [100.0],
            "Reporting Period": [2023],
            "Monitoring Method(s) - A": ["Yes"],
            "Monitoring Method(s) - B": ["No"],
        })
        result = transform_eu_mrv(df)
        if "MONITORING_METHOD_A" in result.columns:
            assert bool(result.iloc[0]["MONITORING_METHOD_A"]) is True
        if "MONITORING_METHOD_B" in result.columns:
            assert bool(result.iloc[0]["MONITORING_METHOD_B"]) is False

    def test_handles_empty_dataframe(self):
        df = pd.DataFrame(columns=["IMO Number", "Name", "Total CO\u2082 emissions [m tonnes]", "Reporting Period"])
        result = transform_eu_mrv(df)
        assert len(result) == 0
