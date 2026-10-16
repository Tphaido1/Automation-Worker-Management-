.PHONY: help install lint format mypy test up down demo bench

help:
	@echo "LeaseFlow Commands:"
	@echo "  make install  - Setup virtualenv and install dependencies"
	@echo "  make lint     - Check code with ruff"
	@echo "  make format   - Format code with ruff"
	@echo "  make mypy     - Run strict type check"
	@echo "  make test     - Run test suite"
	@echo "  make up       - Start Postgres database"
	@echo "  make down     - Stop database"
	@echo "  make demo     - Run chaos demo"
	@echo "  make bench    - Run performance benchmarks"

install:
	uv sync --all-extras --dev

lint:
	uv run ruff check .

format:
	uv run ruff format .

mypy:
	uv run mypy app

test:
	uv run pytest -v

up:
	docker compose up -d db

down:
	docker compose down

demo:
	uv run python scripts/demo.py

bench:
	uv run python scripts/bench.py
