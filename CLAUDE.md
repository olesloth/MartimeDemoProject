# Maritime Vessel Analytics Platform — Claude Code Guide

## Project Overview

Data engineering demo for **Ulstein Group** job application. Ingests real maritime data (vessel emissions, AIS tracking, ocean weather) into a Snowflake data warehouse, transforms it with dbt, and serves analytics via a FastAPI REST API. Demonstrates automated data pipelines for vessel operations monitoring and maritime decarbonization analytics.

**Domain:** Maritime industry — vessels in operations, shipbuilding, ship design, decarbonization.

**To agent:** If you encounter something surprising, fix a bug, or learn a new constraint, update this CLAUDE.md file with that information to improve future performance.

## Architecture

```
[EU MRV CSV] → load_eu_mrv.py ──────┐
[MET Norway API] → load_ocean_weather.py ──┤
[AISStream.io WS] → load_ais_positions.py ─┤→ Snowflake RAW → dbt STAGING → dbt MARTS → FastAPI
[Seed CSV] → load_vessel_registry.py ──────┘
```

1. **Ingestion** (`ingestion/`) — Python scripts that pull data from 3 external sources + 1 seed file into Snowflake RAW layer.
2. **Transformation** (`dbt_maritime/`) — dbt Core project: staging models (clean/type) → mart models (analytics-ready).
3. **API** (`api/`) — FastAPI serving vessel efficiency, fleet comparisons, and weather data from Snowflake marts.
4. **Orchestration** (`ingestion/orchestrate.py`) — Runs full pipeline: ingest → dbt build.

## Data Sources

| Source | Type | Auth | Notes |
|--------|------|------|-------|
| EU MRV/THETIS | CSV download | None | ~50K rows/year, real vessel emissions with IMO numbers |
| MET Norway | REST API | User-Agent header only | Ocean forecast, no API key needed |
| AISStream.io | WebSocket | Free API key (GitHub signup) | Real-time vessel positions, bounding box filter |
| Ulstein vessel registry | Seed CSV | None | Curated list of Ulstein-designed vessels |

## Tech Stack

- **Python 3.11+:** ingestion scripts, orchestration, FastAPI
- **SQL:** dbt models, Snowflake DDL
- **dbt Core:** transformation layer (staging + marts)
- **Snowflake:** data warehouse (free 30-day trial)
- **FastAPI:** REST API with auto-generated Swagger docs

## IMPORTANT: Free Tier Constraints

- **Snowflake:** 30-day trial with $400 credits. Use X-SMALL warehouse with 60s auto-suspend. Use transient tables for raw/staging to avoid time-travel storage costs.
- **AISStream.io:** Free API key. Capture bounded snapshots (10-15 min), don't run continuous streams.
- **MET Norway:** Fully free. Must include identifying `User-Agent` header per their TOS.
- **No paid services.** Do not add any dependency requiring payment or paid API keys.

## IMPORTANT: Snowflake Connection

All Snowflake connections use environment variables from `.env`:
```
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=MARITIME_WH
SNOWFLAKE_DATABASE=MARITIME_ANALYTICS
SNOWFLAKE_ROLE=SYSADMIN
```
Never hardcode credentials. Always use `ingestion/config.py` for connection params.

## IMPORTANT: dbt Profile

The dbt profile is configured in `dbt_maritime/profiles.yml` (checked into repo for demo purposes). In production, this would use `~/.dbt/profiles.yml`. The profile reads from environment variables.

## Project Structure

```
ingestion/
  config.py              Snowflake connection + API config from env vars
  snowflake_loader.py    Shared utility: DataFrame → Snowflake table
  load_eu_mrv.py         EU MRV emissions CSV → raw.eu_mrv_emissions
  load_ocean_weather.py  MET Norway API → raw.ocean_weather
  load_ais_positions.py  AIS WebSocket → raw.ais_positions
  load_vessel_registry.py  Seed CSV → raw.vessel_registry
  orchestrate.py         Pipeline runner: ingest all → dbt build
dbt_maritime/
  models/staging/        stg_* models (clean, type, dedup)
  models/marts/          mart_* models (analytics-ready)
  seeds/                 ulstein_vessel_registry.csv
  tests/                 Custom singular tests
  macros/                Reusable SQL macros
api/
  main.py                FastAPI app entry point
  database.py            Snowflake connection pool
  routers/               vessels.py, fleet.py, weather.py
  schemas.py             Pydantic response models
setup/
  snowflake_setup.sql    DDL to create database, schemas, warehouse, raw tables
tests/                   Python tests (ingestion + API)
```

## Key Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run full pipeline
python -m ingestion.orchestrate

# Run individual ingestion
python -m ingestion.load_eu_mrv
python -m ingestion.load_ocean_weather
python -m ingestion.load_ais_positions

# dbt
cd dbt_maritime && dbt build       # run + test
cd dbt_maritime && dbt docs generate && dbt docs serve

# API
uvicorn api.main:app --reload      # Swagger at http://localhost:8000/docs

# Tests
pytest tests/ -v
```

## Snowflake Schema Design

| Schema | Purpose | Tables |
|--------|---------|--------|
| RAW | Landed data, minimal transformation | ais_positions, eu_mrv_emissions, ocean_weather |
| STAGING | Cleaned, typed, deduplicated (dbt) | stg_ais_positions, stg_eu_mrv_emissions, stg_ocean_weather, stg_vessel_registry |
| MARTS | Business-ready analytics (dbt) | mart_vessel_efficiency, mart_fleet_overview, mart_vessel_voyages, mart_weather_conditions |

## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately

### 2. Verification Before Done
- Never mark a task complete without proving it works
- Run `dbt build` after model changes, `pytest` after Python changes
- Test API endpoints in browser after changes

### 3. Core Principles
- **Simplicity First**: Make every change as simple as possible
- **No Paid Services**: Everything must work on free tiers
- **Maritime Domain**: Keep naming and data relevant to Ulstein's business
