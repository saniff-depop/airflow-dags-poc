# Soda (data quality)

This directory holds **Soda Core 3** configuration, **SodaCL** check layouts, and legacy **per-file** Postgres definitions for the football medallion pipeline.

## Layout

- **Multi–data-source config (Soda Core 3):** [`configuration.compose.yml`](configuration.compose.yml) — `football_warehouse` + `football_source` for cross-checks. Copy to gitignored [`configuration.yml`](configuration.yml.example) for local secrets (see [`.gitignore`](../.gitignore)).
- **Reference data sources:** [`data_sources/`](data_sources/) — one YAML per Postgres role (Compose vs host filenames). See [`data_sources/README.md`](data_sources/README.md).
- **Checks:** [`checks/post_bronze/`](checks/post_bronze/), [`checks/pre_silver/`](checks/pre_silver/), [`checks/spike/`](checks/spike/) — SodaCL; bronze / `silver_stg` checks are rendered from [`checks/_templates/`](checks/_templates/).
- **Integration (optional / CLI):** [`checks/pre_silver/integration/`](checks/pre_silver/integration/) — example cross-table SodaCL on `silver_stg.*`; not wired on the **`football_ingest`** DAG (run with `soda scan` locally if needed).

## Poetry

```bash
poetry install --with dev
```

Dev stack includes **`soda-core-postgres`**, **`jinja2`**, **`setuptools`**, **`psycopg`**.

## Test connections (host → published ports)

With Docker Compose running (`make up`) and defaults from `docker/.env.example`:

```bash
poetry run soda test-connection -d soda/data_sources/football_source.yml
poetry run soda test-connection -d soda/data_sources/football_warehouse.yml
```

(Exact CLI verbs may vary slightly by `soda-core` version; use `soda --help` if a subcommand differs.)

## Scan (host)

Run a **SodaCL** scan with the committed Compose configuration and the phase-04 spike example (requires both databases up and seeded):

```bash
poetry run soda scan -c soda/configuration.compose.yml -s soda/checks/spike/minimal_cross_check_example.yml
```

Adjust `-c` / `-s` flags to match your installed `soda` CLI (`soda scan --help`).

**Airflow** uses the same [`configuration.compose.yml`](configuration.compose.yml) path mounted under `/opt/airflow/soda/` and `PythonVirtualenvOperator` with pins from `VENV_REQUIREMENTS_SODA_AND_JINJA` in `soda_runner.py`.

## Pre-silver reference YAML

Hand-written SodaCL under [`checks/pre_silver/`](checks/pre_silver/) mirrors the Jinja output for smoke tests; the DAG renders templates per table. Files under [`checks/pre_silver/integration/`](checks/pre_silver/integration/) are for manual or CI `soda scan` runs only.

## Local overrides

- **`soda/configuration.yml`** is **gitignored** for secrets; start from [`configuration.yml.example`](configuration.yml.example).
- Optional: untracked `soda/data_sources/*.local.yml`.

## Telemetry

The Soda CLI may log if telemetry is blocked; scans can still succeed. Airflow tasks set `OTEL_SDK_DISABLED=true`.
