# Football ingest DAG

**`dag_id`:** `football_ingest` (`football_ingest_dag.py`)

Medallion pipeline: **postgres_source.public** → warehouse **bronze** → Soda → **`silver_stg`** → Soda → **silver** (WAP). Specs: [`specs/soda/README.md`](../../specs/soda/README.md). Fan-out / per-table loads (design + phased rollout): [`specs/dag-fan-out/README.md`](../../specs/dag-fan-out/README.md).

## Prerequisites (local / Compose)

1. **Warehouse DDL** through **`006_warehouse_audit.sql`** (including **`audit.pipeline_run`**). Without **`audit.pipeline_run`**, **`silver_done`** fails and **rolls back** its audit transaction (silver may already reflect partial per-table loads from **`publish_silver_tables`**).
2. **Source seed** (phase 04) applied to **`postgres_source`** so bronze / **`silver_stg`** / silver have expected rows.
3. **Compose volumes:** **`./dags`**, **`./soda`**, **`./plugins`** mounted into Airflow (see `docker/docker-compose.yaml`) so Soda virtualenv tasks can read **`/opt/airflow/soda/...`**.

### Warehouse referential integrity (bronze / silver_stg / silver)

- **Source** (`public.*`): **foreign keys remain** — see [`scripts/sql/ddl/001_source_public.sql`](../../scripts/sql/ddl/001_source_public.sql).
- **Warehouse** (`bronze`, `silver_stg`, `silver`): DDL **does not** declare **`REFERENCES`** / **`FOREIGN KEY`** between tables — see **`003`**, **`004`**, **`005`**. That supports **parallel** bronze and **`silver_stg`** loads.
- **`ensure_schemas`** (first DAG task) runs [`scripts/sql/ensure_warehouse_schemas.sql`](scripts/sql/ensure_warehouse_schemas.sql), which **creates schemas** and **drops every `FOREIGN KEY` constraint** on **`bronze`**, **`silver_stg`**, and **`silver`** (idempotent). Legacy volumes that still had FKs are corrected on each run.
- **Manual / CI-only:** [`007_drop_warehouse_foreign_keys.sql`](../../scripts/sql/ddl/007_drop_warehouse_foreign_keys.sql) applies the same FK-drop loop (no schema creates).

**Typical DDL order (new warehouse):** `002_warehouse_schemas.sql` → **`003`** → **`004`** → **`005`** → **`006`**. Apply **`001`** on **`postgres_source`**.

## Airflow connections

Create **two** Postgres connections in the Airflow UI (**Admin → Connections**). Use these **Connection Id** values (also exported as constants in `football_ingest_dag.py`):

| Connection Id | Conn type | Extras | Use |
|-----------------|-------------|--------|-----|
| `football_pg_source` | Postgres | — | Read **source** OLTP (`public`) |
| `football_pg_warehouse` | Postgres | — | Read/write **warehouse** (`bronze`, `silver_stg`, `silver`) |

### Fields (Docker Compose)

Airflow containers share the Compose network: use **service DNS names** and internal port **5432** (not the host-mapped ports from `docker/.env`).

| Field | `football_pg_source` | `football_pg_warehouse` |
|--------|----------------------|-------------------------|
| **Host** | `postgres_source` | `postgres_warehouse` |
| **Schema** | `public` | `public` (qualify `bronze` / `silver_stg` / `silver` in SQL) |
| **Login** | `source_user` | `warehouse_user` |
| **Password** | `changeme_source` (or your `docker/.env`) | `changeme_warehouse` |
| **Database** | `source_db` | `warehouse_db` |
| **Port** | `5432` | `5432` |

Align values with [`docker/.env.example`](../../docker/.env.example).

## Host Python / Poetry

**`apache-airflow` is not installed in the Poetry dev environment** here: the repo targets **Python 3.14** on the host, while Airflow **2.10.x** supports **Python &lt; 3.13**. DAGs are validated by **Ruff** and executed in **`docker/Dockerfile.airflow`** (Airflow **2.10.5**).

## Task graph

`ensure_schemas` → **`load_bronze_tables`** (**parallel** nested group per table: `truncate_<table>` → `load_<table>` → **`post_checks`** — SodaCL includes source vs bronze **`row_count same as`**) → **`bronze_done`** → **`load_silver_stg_tables`** (per table nested `load_silver_stg_tables.<table>`: `load_<table>` → **`post_checks`**) → **`silver_stg_done`** → **`publish_silver_tables`** (per table nested `publish_silver_tables.<table>`: **`truncate_<table>`** → **`load_<table>`** → **`post_checks`**, branches parallel) → **`silver_done`** (audit + dataset outlet).

- **Phase 06 (live):** `ensure_schemas` runs **`ensure_warehouse_schemas.sql`** (schemas + drop warehouse FKs on **`bronze` / `silver_stg` / `silver`**).
- **DAG fan-out phase 08 (bronze):** **`load_bronze_tables`** runs **parallel** nested groups (one per bronze table): **`truncate_<table>`** (single-table `TRUNCATE`) → **`load_<table>`** (`run_bronze_table_replace` with **`truncate_table=False`**) → **`post_checks`** (**Soda Core 3** venv: Jinja → SodaCL, including **`row_count same as public.<table> in football_source`**). **`truncate_all_bronze_tables`** remains available for ops (**not** on the DAG spine). Requires **no `FOREIGN KEY` constraints** on `bronze.*` (enforced by **`ensure_schemas`**). **`./soda`** mounted at **`/opt/airflow/soda`**; scans use **`configuration.compose.yml`** (both Postgres data sources).
- **DAG fan-out phase 09 (silver_stg):** **`TaskGroup("load_silver_stg_tables")`** — for each table in **`SILVER_STG_TABLE_LOAD_ORDER`** (`include/silver_stg_from_bronze.py`, same entities as **`_SILVER_STG_INSERT_SQL`**), nested **`load_silver_stg_tables.<table>`**: **`load_<table>`** calls **`run_silver_stg_table_replace`** with **`batch_id` = `run_id`** (parallel connections, one transaction per task); **`post_checks`** runs **`run_soda_contract_for_table`** with **`layer="silver_stg"`** and **`silver_stg_pre_silver_template_context(table)`** (SodaCL: schema + duplicate / row_count / missing-name checks). **`silver_stg_done`** fans in before **`publish_silver_tables`**. There is no monolithic **`load_silver_stg`** task.
- **Silver publish (parallel per table):** **`TaskGroup("publish_silver_tables")`** — for each table in **`BRONZE_TABLE_LOAD_ORDER`**, nested **`publish_silver_tables.<table>`**: **`truncate_<table>`** (`run_truncate_one_silver_table`), **`load_<table>`** (`run_load_silver_table_from_stg_only`), **`post_checks`** (row-count parity, no zero rows). Branches run in parallel; each step uses its **own** DB transaction. **`silver_done`** runs **`run_finalize_silver_publish_audit`** (row-count snapshot + **`audit.pipeline_run`**) and emits the **`WAREHOUSE_SILVER_FOOTBALL_DATASET`** event. While a run is in progress, readers may see **partial** `silver.*` updates until **`silver_done`** succeeds.
- **Full silver_stg snapshot API:** **`run_silver_stg_snapshot_from_bronze`** still loads all **`silver_stg`** tables in **one** DB transaction (used by tests / ops); the DAG uses per-table **`run_silver_stg_table_replace`** for parallelism.
- **Legacy single-transaction publish:** **`run_publish_silver_from_silver_stg`** remains in code for tests / tooling — **full replace in one transaction** with per-table DQ inside that txn. The **DAG** uses **`publish_silver_tables`** + **`silver_done`** instead.

See [`soda/checks/pre_silver/README.md`](../../soda/checks/pre_silver/README.md) for the pre-silver DQ matrix. Tune worker **`parallelism`** / pools if many **`PythonVirtualenvOperator`** tasks queue.

`max_active_runs=1` keeps snapshot semantics predictable (concurrent runs are not supported for this full-replace design).

## Troubleshooting

| Symptom | Things to check |
|--------|------------------|
| **`load_bronze_tables.<table>.post_checks` / `load_silver_stg_tables.<table>.post_checks` fail** building the venv | Scheduler/worker logs; network to PyPI; disk space under Airflow home. Per-table Soda tasks need **`soda-core-postgres`**, **`jinja2`**, and **`setuptools`** (`VENV_REQUIREMENTS_SODA_AND_JINJA` in `include/soda_runner.py`). `OTEL_SDK_DISABLED=true` reduces Soda telemetry noise. |
| **`load_bronze_tables.<table>.post_checks` fails** on row count | SodaCL cross-check: source **`public.<table>`** vs **`bronze.<table>`** — fix load or source data; other tables may already have loaded in parallel. |
| **A specific `load_silver_stg_tables.<table>.post_checks` fails** | Logs include **`Soda pre-silver (`** and the table name; check Jinja-rendered checks for that entity (`silver_stg_pre_silver_template_context` in `include/soda_contract_template.py`). |
| **`ModuleNotFoundError: football_ingest`** in Soda venv tasks | The shim prepends `AIRFLOW__CORE__DAGS_FOLDER` (or `/opt/airflow/dags`) to `sys.path`. If your DAGs live elsewhere, set **`AIRFLOW__CORE__DAGS_FOLDER`** on the worker or mount DAGs at the default path. |
| **Soda “file not found”** | **`./soda` → `/opt/airflow/soda`** volume present; scans use **`configuration.compose.yml`** (and Jinja templates under **`checks/_templates/`**). |
| **`silver_done` fails on `audit.pipeline_run`** | Apply **`scripts/sql/ddl/006_warehouse_audit.sql`** on **`postgres_warehouse`** (after `002` creates **`audit`** schema). |
| **`load_<table>` (under bronze) fails on FK** | Confirm **`ensure_schemas`** succeeded (drops warehouse FKs). Run **`007_drop_warehouse_foreign_keys.sql`** on **`postgres_warehouse`** if needed. **`silver_stg`** parallel loads also require no FKs on **`silver_stg.*`**. |
| **Postgres connection errors** | Airflow connections use Compose DNS (**`postgres_source`** / **`postgres_warehouse`**) and port **5432**, not host-mapped ports. |
| **Broken link to a bronze DQ task** (bookmarks, Slack, metrics) | Use **`load_bronze_tables.<table>.post_checks`** (SodaCL includes row-count cross-check). Older ids such as **`post_checks.schema_check`**, **`row_count_check`**, **`load_bronze_<table>`**, or **`parity_bronze_<table>`** are no longer valid. |

### Quick CLI connection add (optional)

Inside `airflow-scheduler`:

```bash
airflow connections add football_pg_source \
  --conn-uri "postgresql://source_user:changeme_source@postgres_source:5432/source_db"
airflow connections add football_pg_warehouse \
  --conn-uri "postgresql://warehouse_user:changeme_warehouse@postgres_warehouse:5432/warehouse_db"
```

(Adjust passwords if your `docker/.env` differs.)
