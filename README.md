# airflow-dags-poc

Local development workspace for **Apache Airflow DAGs**. The stack is intended to run via **Docker Compose** with **MySQL** for Airflow metadata and **two PostgreSQL** instances (source system and data warehouse) for pipeline-style workflows. This repository holds DAG code under `dags/`, Docker assets under `docker/`, and tooling (Poetry, Ruff, pytest) for day-to-day maintenance.

## Prerequisites

- **Python** — version **3.14 or newer** (see `requires-python` in [`pyproject.toml`](pyproject.toml)).
- **Docker** — [Docker Desktop](https://docs.docker.com/desktop/) or Docker Engine with the **Compose V2** plugin (`docker compose`), for when the local Airflow stack is wired up (phases 04–05).
- **Poetry** — [install Poetry](https://python-poetry.org/docs/#installation) for managing the dev virtualenv and tools.

## 1. Install Poetry and project dependencies

1. Install Poetry using the [official installer](https://python-poetry.org/docs/#installation).
2. Confirm it is on your `PATH`:

   ```bash
   poetry --version
   ```

3. Clone this repository and `cd` into the project root (if you have not already).
4. Install dependencies and create the local virtualenv:

   ```bash
   make build
   ```

   Alternatively:

   ```bash
   poetry install --with dev
   ```

5. **Optional:** run the full lint + test gate:

   ```bash
   make check_all
   ```

## 2. Run the local stack (Docker Compose)

The stack is defined in [`docker/docker-compose.yaml`](docker/docker-compose.yaml): **MySQL** (Airflow metadata), **two Postgres** services (source + warehouse), **Airflow** webserver + scheduler (**LocalExecutor**), built from [`docker/Dockerfile.airflow`](docker/Dockerfile.airflow) (Apache Airflow **2.10.5** + PyMySQL). See [`specs/initial-setup/phases/04-docker-compose-database-tier.md`](specs/initial-setup/phases/04-docker-compose-database-tier.md) and [`specs/initial-setup/phases/05-docker-compose-airflow-integration.md`](specs/initial-setup/phases/05-docker-compose-airflow-integration.md).

**Workflow**

1. Copy [`docker/.env.example`](docker/.env.example) to **`docker/.env`** and edit secrets. At minimum, keep **`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`** in sync with **`MYSQL_USER`** / **`MYSQL_PASSWORD`**. **`make up` requires `docker/.env`** (the Makefile passes `--env-file docker/.env`).
2. **Optional:** validate Compose without a personal `docker/.env`:

   ```bash
   make compose_config
   ```

   Requires the [Docker Compose V2 plugin](https://docs.docker.com/compose/install/) (`docker compose version` should succeed).
3. Start everything (builds the Airflow image on first run), then seed the databases and create the Airflow connections:

   ```bash
   make up_with_data
   ```

   This runs **`make up`** followed by **`make init_data`**. `init_data` runs two steps, which you can also run on their own:

   | Target | What it does |
   |--------|--------------|
   | `make init_databases` | Creates the source `public` tables and loads the Premier League seed data, then creates the warehouse `bronze`, `silver_stg`, `silver` and audit tables ([`scripts/init-databases.sh`](scripts/init-databases.sh)). |
   | `make init_airflow_connections` | Creates the **`football_pg_source`** and **`football_pg_warehouse`** Airflow connections ([`scripts/init-airflow-connections.sh`](scripts/init-airflow-connections.sh)). |

   **`make up` on its own starts empty databases with no connections.** Use it only when the volumes already hold data, for example after `make stop`.

   Prefer **`make up`** over calling `docker compose` yourself: the Makefile pins the repo root, exports **`AIRFLOW_COMPOSE_CONTEXT`** for the Airflow image build, and uses **`--project-directory`** so `./dags` mounts resolve correctly. The Airflow image name is **local-only** (`pull_policy: never`); Docker must **build** it, not pull it from Docker Hub.

4. Open the **Airflow UI**: **http://localhost:8080** (or the host port in **`AIRFLOW_WEBSERVER_PORT`** in `docker/.env`). Sign in with **`AIRFLOW_ADMIN_USERNAME`** / **`AIRFLOW_ADMIN_PASSWORD`** from `docker/.env` (defaults in `.env.example`: `airflow` / `airflow`).

   If `airflow-webserver` stays in **`Created`**, another process already holds port 8080. Run `docker ps --format '{{.Names}}\t{{.Ports}}' | grep 8080` to find it. Then either stop that container or set a free port, for example `AIRFLOW_WEBSERVER_PORT=8090`, in `docker/.env` and run `make up` again.
5. Pause containers without removing them:

   ```bash
   make stop
   ```

6. Tear down the stack, remove the project network, and **delete named volumes** (MySQL/Postgres data and **`airflow_logs`** reset; repo bind mounts **`dags`**, **`soda`**, **`plugins`** are unchanged):

   ```bash
   make down
   ```

   The next start needs **`make up_with_data`**, because the seed data and Airflow connections were deleted with the volumes.

## Troubleshooting `football_ingest`

| Error | Cause | Fix |
|-------|-------|-----|
| ``The conn_id `football_pg_warehouse` isn't defined`` | Airflow connections were never created, or were wiped by `make down` | `make init_airflow_connections` |
| `relation "public.venue" does not exist` (Soda `pre_checks`) | The source database isn't seeded | `make init_databases` |
| `Fixture kickoff staleness` check fails in `pre_checks` | The seed data is static, and freshness is measured against the current date | Raise `BRONZE_SOURCE_*_FRESHNESS_MAX_AGE` in [`soda_contract_template.py`](dags/ingestion/football_ingest/include/soda_contract_template.py) (currently `3650d`) |

After applying a fix, clear the failed task in the Airflow UI and re-run it.

`scripts/init-airflow-connections.sh` hardcodes the Postgres credentials from `.env.example`. If you change `POSTGRES_*` passwords in `docker/.env`, update the script to match.

## 3. Run your first DAG

The bootstrap DAG **`example_hello`** lives in [`dags/example_hello.py`](dags/example_hello.py) (`dag_id`: **`example_hello`**).

1. With the stack up (section 2), open the UI and sign in.
2. Find **`example_hello`** in the DAG list (it may start **paused**).
3. **Unpause** the DAG, open it, and **Trigger** a run.
4. Confirm both tasks finish successfully in the **Graph** or **Grid** view.

## Further reading

- [`specs/initial-setup/README.md`](specs/initial-setup/README.md) — architecture, databases, DAG conventions, Makefile Compose shortcuts.
- [`specs/initial-setup/phases/`](specs/initial-setup/phases/) — ordered implementation phases (01–05) with verification steps.
- [`specs/dag-fan-out/README.md`](specs/dag-fan-out/README.md) — **football_ingest** parallel bronze/staging fan-out: phased implementation (**01–09**) and design pointers.

**Warehouse note:** Bronze and silver tables in this repo do **not** use foreign keys on **`postgres_warehouse`** (referential integrity is on the **source** DB only). If you reuse an old warehouse volume and hit FK errors when loading, see [`dags/football_ingest/README.md`](dags/football_ingest/README.md) (section *Warehouse referential integrity* and script **`007_drop_warehouse_foreign_keys.sql`**).
