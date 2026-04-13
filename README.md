# Maritime Vessel Analytics Platform

A data engineering demo project showcasing automated data pipelines for the maritime industry. Built for Ulstein Group's Data Engineer position — demonstrates **Python, SQL, dbt, Snowflake, and FastAPI** skills applied to vessel operations monitoring and decarbonization analytics.

## Architecture

```
Data Sources                    Pipeline                      Serving
─────────────                   ────────                      ───────
EU MRV Emissions (CSV)  ──┐
MET Norway Weather (API) ─┤─→ Python Ingestion ─→ Snowflake RAW
AISStream.io Vessels (WS) ┤                          │
Ulstein Registry (Seed)  ──┘                     dbt Core
                                                     │
                                              STAGING (views)
                                                     │
                                              MARTS (tables) ─→ FastAPI REST API
                                                                     │
                                                              /docs (Swagger UI)
```

## What This Demonstrates

| Job Requirement | Implementation |
|---|---|
| Automated data pipelines | `ingestion/` scripts with `orchestrate.py` running parallel ingestion |
| Structured data storage | Snowflake RAW → STAGING → MARTS schema design via dbt |
| Efficient data retrieval | FastAPI endpoints with parameterized Snowflake queries |
| Data flows via API | AIS WebSocket ingestion, MET Norway REST API, FastAPI serving |
| Python & SQL | All ingestion/API in Python, all transformations in SQL (dbt) |
| dbt | Full project: sources, staging, marts, seeds, tests, docs, macros |
| Snowflake | Warehouse sizing, transient tables, schema design |
| Sensors & data acquisition | Real-time AIS vessel tracking data via WebSocket |

## Data Sources

- **EU MRV/THETIS** — Ship CO2 emissions, fuel consumption, and efficiency data for ~5,000+ vessels per year (public, free)
- **MET Norway** — Ocean weather forecasts along Norwegian shipping lanes (free, no API key)
- **AISStream.io** — Real-time vessel positions in the Norwegian Sea via WebSocket (free API key)
- **Ulstein Vessel Registry** — Curated seed data of Ulstein-designed vessels with X-BOW flags

## Quick Start

### Prerequisites

- Python 3.11+
- Snowflake account ([30-day free trial](https://signup.snowflake.com/))
- AISStream.io API key ([free registration](https://aisstream.io/))

### 1. Setup

```bash
git clone <repo-url> && cd DataEngineerDemoProject
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Copy and fill in your credentials
cp .env.example .env
```

### 2. Snowflake Setup

Run `setup/snowflake_setup.sql` in Snowflake Worksheets to create the database, schemas, warehouse, and raw tables.

### 3. Install dbt Packages

```bash
cd dbt_maritime && dbt deps && cd ..
```

### 4. Run the Pipeline

```bash
# Full pipeline: ingest all sources → dbt build
python -m ingestion.orchestrate

# Or run steps individually:
make ingest-mrv          # EU MRV emissions (~1 min)
make ingest-weather      # Ocean weather (~30 sec)
make ingest-ais          # AIS positions (~10 min capture)
make dbt-build           # dbt run + test
```

### 5. Start the API

```bash
uvicorn api.main:app --reload
# Open http://localhost:8000/docs for Swagger UI
```

### 6. Explore dbt Docs

```bash
cd dbt_maritime && dbt docs generate && dbt docs serve
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/v1/vessels` | List vessels with efficiency metrics |
| `GET /api/v1/vessels/{imo}/emissions` | Emissions time series for a vessel |
| `GET /api/v1/vessels/{imo}/positions` | Latest AIS positions |
| `GET /api/v1/fleet/efficiency` | Fleet-level efficiency stats |
| `GET /api/v1/fleet/comparison` | X-BOW vs conventional hull comparison |
| `GET /api/v1/weather/forecast` | Ocean conditions by region |
| `GET /api/v1/health` | API + database health check |

## Key Insight: X-BOW Hull Efficiency

The `mart_vessel_efficiency` model joins EU MRV emissions data with a curated registry of Ulstein-designed vessels, enabling direct comparison of X-BOW hull designs against conventional vessels on metrics like CO2 per nautical mile and fuel efficiency rankings.

## Project Structure

```
ingestion/              Python data ingestion scripts
  config.py             Connection settings from environment
  snowflake_loader.py   Shared DataFrame → Snowflake utility
  load_eu_mrv.py        EU MRV emissions CSV loader
  load_ocean_weather.py MET Norway API loader
  load_ais_positions.py AIS WebSocket snapshot loader
  orchestrate.py        Full pipeline orchestrator

dbt_maritime/           dbt transformation project
  models/staging/       Clean, type, and deduplicate raw data
  models/marts/         Business-ready analytics models
  seeds/                Ulstein vessel registry
  tests/                Custom data quality tests
  macros/               Reusable SQL (CO2 efficiency calcs)

api/                    FastAPI REST API
  routers/              Endpoint definitions
  schemas.py            Pydantic response models
  database.py           Snowflake connection management

setup/                  Snowflake DDL scripts
```

## Cost Management

Everything runs on free tiers:
- **Snowflake**: X-SMALL warehouse, 60s auto-suspend, transient tables
- **AISStream.io**: Free API key, bounded capture windows
- **MET Norway**: Fully free, no API key needed
- **dbt Core**: Open-source, free
