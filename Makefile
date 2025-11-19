.PHONY: help setup install install-dev clean lint format test test-cov run-classification run-etl docker-up docker-down

PYTHON := python3
VENV := venv
BIN := $(VENV)/bin

help:
	@echo "Comandos disponibles:"
	@echo "  make setup          - Crear entorno virtual e instalar dependencias"
	@echo "  make install        - Instalar dependencias de producción"
	@echo "  make install-dev    - Instalar dependencias de desarrollo"
	@echo "  make clean          - Limpiar archivos temporales y cache"
	@echo "  make lint           - Ejecutar linting (pylint + flake8)"
	@echo "  make format         - Formatear código con black"
	@echo "  make test           - Ejecutar tests con pytest"
	@echo "  make test-cov       - Ejecutar tests con cobertura"
	@echo "  make run-classification - Ejecutar sistema de clasificación"
	@echo "  make run-etl        - Ejecutar sistema ETL"
	@echo "  make docker-up      - Levantar servicios (Hive) con Docker"
	@echo "  make docker-down    - Detener servicios Docker"

setup:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(MAKE) install
	$(MAKE) install-dev
	@echo "Entorno virtual creado. Actívalo con: source $(VENV)/bin/activate"

install:
	$(BIN)/pip install -r requirements.txt

install-dev:
	$(BIN)/pip install -r requirements-dev.txt

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ htmlcov/

lint:
	$(BIN)/pylint classification_system/ etl_system/ scripts/
	$(BIN)/flake8 classification_system/ etl_system/ scripts/ --max-line-length=100

format:
	$(BIN)/black classification_system/ etl_system/ scripts/ tests/
	$(BIN)/isort classification_system/ etl_system/ scripts/ tests/

test:
	$(BIN)/pytest tests/ -v

test-cov:
	$(BIN)/pytest tests/ -v --cov=classification_system --cov=etl_system --cov-report=html --cov-report=term

run-classification:
	$(BIN)/python scripts/run_classification.py

run-etl:
	$(BIN)/python scripts/run_etl.py

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down