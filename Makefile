# The virtual environment path.
VENV := .venv

.DEFAULT: help

help:
	@echo "make init"
	@echo "       prepare development environment, install dev packages and create venv"
	@echo "make test"
	@echo "       run tests"
	@echo "make lint"
	@echo "       run flake8 linting"
	@echo "make format"
	@echo "       run black code formatter"
	@echo "make sort"
	@echo "       run isort to sort imports"

.PHONY: init
init:
	@echo "Initializing virtual environment..."
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements-dev.txt

.PHONY: test
test:
	@echo "Running tests..."
	$(VENV)/bin/pytest

.PHONY: lint
lint:
	@echo "Running flake8 linting..."
	$(VENV)/bin/flake8

.PHONY: format
format:
	@echo "Running black code formatter..."
	$(VENV)/bin/black plugins dags tests
