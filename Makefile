.PHONY: setup ingest dbt-build dbt-docs api test clean

# Setup virtual environment and install dependencies
setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	cd dbt_maritime && dbt deps

# Run full data ingestion pipeline
ingest:
	python -m ingestion.orchestrate

# Run individual ingestion scripts
ingest-mrv:
	python -m ingestion.load_eu_mrv

ingest-weather:
	python -m ingestion.load_ocean_weather

ingest-ais:
	python -m ingestion.load_ais_positions

ingest-registry:
	python -m ingestion.load_vessel_registry

# dbt commands
dbt-build:
	cd dbt_maritime && dbt build --profiles-dir .

dbt-run:
	cd dbt_maritime && dbt run --profiles-dir .

dbt-test:
	cd dbt_maritime && dbt test --profiles-dir .

dbt-docs:
	cd dbt_maritime && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

dbt-seed:
	cd dbt_maritime && dbt seed --profiles-dir .

# Start FastAPI development server
api:
	uvicorn api.main:app --reload

# Run Python tests
test:
	pytest tests/ -v

# Clean generated files
clean:
	rm -rf dbt_maritime/target dbt_maritime/dbt_packages dbt_maritime/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
