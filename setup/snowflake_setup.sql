-- ============================================================
-- Snowflake Setup for Maritime Vessel Analytics Platform
-- Run this script in Snowflake UI (Worksheets) after creating
-- your free trial account.
-- ============================================================

-- Use admin role to create database and warehouse
USE ROLE ACCOUNTADMIN;

-- Create warehouse (X-SMALL to conserve free trial credits)
CREATE WAREHOUSE IF NOT EXISTS MARITIME_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create database
CREATE DATABASE IF NOT EXISTS MARITIME_ANALYTICS;

USE DATABASE MARITIME_ANALYTICS;

-- Create schemas (RAW → STAGING → MARTS)
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- ============================================================
-- RAW TABLES (transient to save on storage costs)
-- ============================================================

CREATE TRANSIENT TABLE IF NOT EXISTS RAW.EU_MRV_EMISSIONS (
    imo_number          VARCHAR,
    vessel_name         VARCHAR,
    ship_type           VARCHAR,
    reporting_period    INTEGER,
    technical_efficiency VARCHAR,
    port_of_registry    VARCHAR,
    home_port           VARCHAR,
    ice_class           VARCHAR,
    doc_issue_date      VARCHAR,
    doc_expiry_date     VARCHAR,
    verifier_name       VARCHAR,
    verifier_nab        VARCHAR,
    verifier_address    VARCHAR,
    verifier_city       VARCHAR,
    verifier_country    VARCHAR,
    verifier_accreditation_number VARCHAR,
    a_total_fuel_consumption_mt FLOAT,
    a_on_laden_voyages_mt FLOAT,
    total_co2_emissions_mt FLOAT,
    co2_emissions_on_laden_voyages_mt FLOAT,
    co2_emissions_from_all_voyages_between_ports_under_ms_jurisdiction_mt FLOAT,
    co2_emissions_from_all_voyages_departed_from_ports_under_ms_jurisdiction_mt FLOAT,
    co2_emissions_from_all_voyages_to_ports_under_ms_jurisdiction_mt FLOAT,
    co2_emissions_within_ports_under_ms_jurisdiction_at_berth_mt FLOAT,
    co2_emissions_passenger_transport_mt FLOAT,
    co2_emissions_freight_transport_mt FLOAT,
    fuel_consumptions_assigned_to_on_laden_voyages_mt FLOAT,
    total_fuel_consumption_on_voyages_between_ports_mt FLOAT,
    fuel_consumption_departed_from_ports_mt FLOAT,
    fuel_consumption_to_ports_mt FLOAT,
    fuel_consumption_within_ports_at_berth_mt FLOAT,
    fuel_consumption_passenger_transport_mt FLOAT,
    fuel_consumption_freight_transport_mt FLOAT,
    distance_travelled_nm FLOAT,
    time_at_sea_hours   FLOAT,
    average_speed_knots FLOAT,
    monitoring_method_a  BOOLEAN,
    monitoring_method_b  BOOLEAN,
    monitoring_method_c  BOOLEAN,
    monitoring_method_d  BOOLEAN,
    deadweight_tonnes   FLOAT,
    gross_tonnage       FLOAT,
    annual_average_eeoi  FLOAT,
    annual_average_co2_emissions_per_distance_kg_per_nm FLOAT,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TRANSIENT TABLE IF NOT EXISTS RAW.AIS_POSITIONS (
    mmsi                VARCHAR,
    imo_number          VARCHAR,
    vessel_name         VARCHAR,
    ship_type           INTEGER,
    latitude            FLOAT,
    longitude           FLOAT,
    speed_knots         FLOAT,
    course              FLOAT,
    heading             FLOAT,
    destination         VARCHAR,
    navigation_status   INTEGER,
    message_type        INTEGER,
    received_at         TIMESTAMP_NTZ,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TRANSIENT TABLE IF NOT EXISTS RAW.OCEAN_WEATHER (
    latitude            FLOAT,
    longitude           FLOAT,
    forecast_time       TIMESTAMP_NTZ,
    wave_height_m       FLOAT,
    wave_direction_deg  FLOAT,
    wave_period_s       FLOAT,
    wind_speed_ms       FLOAT,
    wind_direction_deg  FLOAT,
    water_temperature_c FLOAT,
    current_speed_ms    FLOAT,
    current_direction_deg FLOAT,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Grant usage to default role
USE DATABASE MARITIME_ANALYTICS;
GRANT USAGE ON DATABASE MARITIME_ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MARITIME_ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MARITIME_ANALYTICS.RAW TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MARITIME_ANALYTICS.STAGING TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MARITIME_ANALYTICS.MARTS TO ROLE SYSADMIN;
