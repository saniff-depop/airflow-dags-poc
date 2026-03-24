# Soda data sources (Soda Library 4.x)

This project uses **Soda Core 4** (`soda-postgres` in Poetry). Each Postgres role is a **separate YAML file** (`type`, `name`, `connection` block). There is **no** `soda_cloud` section (standalone / local POC).

| File | Role | Default connection |
|------|------|-------------------|
| `football_source.yml` | `postgres_source` / `public` | `localhost:5433` (host port from `docker/.env.example`) |
| `football_warehouse.yml` | `postgres_warehouse` (bronze / silver_stg / silver) | `localhost:5434` |
| `football_source.compose.yml` | Same as source | `postgres_source:5432` (Airflow container → Compose network) |
| `football_warehouse.compose.yml` | Same as warehouse | `postgres_warehouse:5432` |

## Test from the repository root (host)

With Compose **up** and ports published:

```bash
poetry run soda data-source test -ds soda/data_sources/football_source.yml
poetry run soda data-source test -ds soda/data_sources/football_warehouse.yml
```

(`-ds` is the **path** to the YAML file, despite the flag name.)

## Airflow / in-container

The repo includes **`*.compose.yml`** files for the Compose service DNS names and port **5432**. The Airflow image mounts **`./soda` → `/opt/airflow/soda`** (see `docker/docker-compose.yaml`); phase 07 tasks use those paths.

For ad-hoc copies with secrets, see **`*.docker.yml.example`**.

## Bronze / silver_stg in checks

Warehouse checks should qualify schemas (e.g. `bronze.competition`) in SodaCL or contracts when you add them in phases 07 / 09.
