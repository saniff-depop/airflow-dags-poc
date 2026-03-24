###################
# LOCAL VARIABLES #
###################
# Repo root = directory containing this Makefile (not $(CURDIR), so `make -f path/Makefile` still works).
REPO_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
export AIRFLOW_COMPOSE_CONTEXT := $(REPO_ROOT)

POETRY_VERSION ?= 2.1.3
POETRY_HOME ?= $(REPO_ROOT)/.poetry
POETRY_BINARY ?= $(shell command -v poetry 2>/dev/null || echo $(POETRY_HOME)/bin/poetry)
POETRY_RUN := cd $(REPO_ROOT) && $(POETRY_BINARY) run
POETRY_PROJECT_NAME ?= $(shell cd $(REPO_ROOT) && $(POETRY_BINARY) version | cut -d " " -f 1)
DAGS_DIR := $(REPO_ROOT)/dags
TEST_DIR := $(REPO_ROOT)/tests
COMPOSE_FILE := $(REPO_ROOT)/docker/docker-compose.yaml
COMPOSE_ENV_FILE := $(REPO_ROOT)/docker/.env
COMPOSE_ENV_EXAMPLE := $(REPO_ROOT)/docker/.env.example
COMPOSE_WRAP := bash $(REPO_ROOT)/scripts/compose.sh
COMPOSE := $(COMPOSE_WRAP) -f $(COMPOSE_FILE) --project-directory $(AIRFLOW_COMPOSE_CONTEXT) --env-file $(COMPOSE_ENV_FILE)
COMPOSE_VALIDATE := $(COMPOSE_WRAP) -f $(COMPOSE_FILE) --project-directory $(AIRFLOW_COMPOSE_CONTEXT) --env-file $(COMPOSE_ENV_EXAMPLE)

#########################
# ENVIRONMENT VARIABLES #
#########################
export POETRY_HOME
export PATH := $(POETRY_HOME)/bin:$(PATH)
export POETRY_VIRTUALENVS_IN_PROJECT = "true"

#################
# PHONY TARGETS #
#################
.PHONY: help init build \
		lint_check lint_fix \
		unit_test \
		check_all \
		clean \
		up up_with_data stop down \
		init_data init_databases init_airflow_connections \
		compose_config \
		dbt_seed

########
# HELP #
########
help: ## Show help for available make targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk -F':.*?## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

init:
	@bash $(REPO_ROOT)/scripts/init-project.sh

build: $(REPO_ROOT)/pyproject.toml $(REPO_ROOT)/poetry.lock ## Install poetry + dependencies and check lockfile is in sync
	@command -v poetry >/dev/null || ( \
		echo "Installing Poetry $(POETRY_VERSION)..."; \
		curl -sSL https://install.python-poetry.org | python3 - --version $(POETRY_VERSION); \
	)
	@cd $(REPO_ROOT) && poetry --version
	@echo "Installing project dependencies for development..."
	@cd $(REPO_ROOT) && poetry install --with dev

lint_check: build
	@echo "Running Ruff linter (non-destructive)"
	@$(POETRY_RUN) ruff check $(DAGS_DIR) $(TEST_DIR)
	@echo "Check ruff formatting (non-destructive)"
	@$(POETRY_RUN) ruff format --check $(DAGS_DIR) $(TEST_DIR)

lint_fix: build ## Format code and auto-fix basic lint issues
	@echo "Running Ruff formatter..."
	@$(POETRY_RUN) ruff format $(DAGS_DIR) $(TEST_DIR)
	@echo "Running Ruff linter with auto-fix..."
	@$(POETRY_RUN) ruff check --fix --unsafe-fixes $(DAGS_DIR) $(TEST_DIR)
	@echo "Code formatted and lint issues auto-fixed."

unit_test: build  ## Run unit tests
	@$(POETRY_RUN) pytest -s -vv

check_all: lint_check lint_fix unit_test ## Run all checks (lint, format, test)

up: ## Start Airflow stack (Docker Compose, detached)
	@$(COMPOSE) up -d

up_with_data: up init_data ## Start Airflow stack and initialize databases + connections

stop: ## Stop Airflow stack containers (do not remove)
	@$(COMPOSE) stop

down: ## Shut down stack and remove named volumes (DB + logs reset; bind mounts unchanged)
	@$(COMPOSE) down --volumes

init_data: init_databases init_airflow_connections ## Initialize all databases and Airflow connections

init_databases: ## Create database schemas and seed with Premier League data
	@bash $(REPO_ROOT)/scripts/init-databases.sh

init_airflow_connections: ## Create Airflow connections for source and warehouse
	@bash $(REPO_ROOT)/scripts/init-airflow-connections.sh

compose_config: ## Validate docker-compose.yaml using docker/.env.example (no docker/.env required)
	@echo "Validating Compose file with $(COMPOSE_ENV_EXAMPLE)..."
	@$(COMPOSE_VALIDATE) config >/dev/null
	@echo "Compose config OK."

dbt_seed: build ## Load dbt seeds into the warehouse (requires Postgres per profiles.yml)
	@$(POETRY_RUN) dbt seed --project-dir $(REPO_ROOT)/dbt/football_modelling --profiles-dir $(REPO_ROOT)/dbt/football_modelling --target local

clean_all: ## Clean project for virtualenv and cache directories
	@echo "Cleaning project for virtualenv and cache directories..."
	@rm -rf $(REPO_ROOT)/.venv $(REPO_ROOT)/.pytest_cache $(REPO_ROOT)/.ruff_cache $(REPO_ROOT)/.mypy_cache
	@echo "Project cleaned successfully!"
