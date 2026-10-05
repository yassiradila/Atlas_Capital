.PHONY: help up down ps logs test lint dbt-build dbt-test psql

help:
	@echo "Atlas Capital — Developer Commands"
	@echo "  make up          Start all Docker services in background"
	@echo "  make down        Stop all Docker services"
	@echo "  make ps          View container statuses"
	@echo "  make logs        Tail container logs"
	@echo "  make test        Run unit & DAG integrity tests (pytest)"
	@echo "  make lint        Run Python linter (ruff)"
	@echo "  make dbt-build   Run full dbt models and tests"
	@echo "  make dbt-test    Run dbt data quality tests only"
	@echo "  make psql        Connect to the warehouse database"

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	docker compose exec airflow-webserver pytest tests/

lint:
	docker compose exec airflow-webserver ruff check --select E,F,W,I --ignore E501 src/ tests/ dags/

dbt-build:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt build"

dbt-test:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test"

psql:
	docker compose exec postgres psql -U postgres -d warehouse
