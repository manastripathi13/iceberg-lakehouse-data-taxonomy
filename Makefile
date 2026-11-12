.PHONY: setup up demo test dbt taxonomy down
setup:     ## install Python deps
	uv sync
up:        ## start object storage, Nessie and Trino
	docker compose up -d && uv run python scripts/create_bucket.py
demo:      ## full run from an empty lakehouse
	./scripts/run_demo.sh
test:      ## unit tests
	uv run pytest -q tests
dbt:       ## rebuild Silver + Gold
	cd dbt && uv run dbt build --profiles-dir .
taxonomy:  ## enforce taxonomy + regenerate docs/DATA_CATALOG.md
	uv run python governance/check_taxonomy.py --write-catalog
down:
	docker compose down -v
