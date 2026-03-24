# SQL scripts

SQL assets for the local football / medallion POC.

| Directory | Phase | Contents |
|-----------|-------|----------|
| `ddl/` | [`specs/soda/phases/02-ddl-source-and-warehouse-schemas.md`](../../specs/soda/phases/02-ddl-source-and-warehouse-schemas.md) | `CREATE TABLE` / schemas for `postgres_source` and `postgres_warehouse` |
| `seed/` | [`specs/soda/phases/04-deterministic-source-seed-sql.md`](../../specs/soda/phases/04-deterministic-source-seed-sql.md) | Deterministic seed data for source (manual apply; not run by `make check_all`) |

## Idempotence

DDL uses **`CREATE TABLE IF NOT EXISTS`** (and **`CREATE SCHEMA IF NOT EXISTS`**). Re-running the same file is safe for additive objects; **column changes** are not migrations—alter tables in a new numbered script if the model evolves.

## Naming

| Layer | Database | Schema | Tables |
|-------|----------|--------|--------|
| Source OLTP | `postgres_source` (`source_db`) | `public` | `competition`, `season`, `venue`, `team`, `player`, `player_contract`, `matchday_round`, `fixture`, `match_result` |
| Bronze | `postgres_warehouse` (`warehouse_db`) | `bronze` | **Same names** as `public`, columns mirror source (PK/FK preserved) for simple row-count checks |
| Ingestion pre-silver | warehouse | `silver_stg` | Same entity names; business columns + **`_ingested_at`**, **`_batch_id`**, **`_source_system`** |
| Silver | warehouse | `silver` | Same entity names; business columns + **`_batch_id`**, **`_published_at`**; PK/FK for consumer queries |
| Audit | warehouse | `audit` | `pipeline_run` (optional metrics / Soda summaries) |

`player_contract` intentionally has **no UNIQUE** on `(player_id, team_id, valid_from)` so phase `04` can seed duplicate rows for dedupe tests.

## Apply order

Run **001** on **source** only; **002–006** on **warehouse** only.

| Order | File | Target |
|-------|------|--------|
| 1 | `ddl/001_source_public.sql` | `postgres_source` |
| 2 | `ddl/002_warehouse_schemas.sql` | `postgres_warehouse` |
| 3 | `ddl/003_warehouse_bronze_tables.sql` | `postgres_warehouse` |
| 4 | `ddl/004_warehouse_silver_stg_tables.sql` | `postgres_warehouse` |
| 5 | `ddl/005_warehouse_silver_tables.sql` | `postgres_warehouse` |
| 6 | `ddl/006_warehouse_audit.sql` | `postgres_warehouse` |

Airflow **`silver_done`** inserts into **`audit.pipeline_run`** after per-table **`publish_silver_tables`** loads (separate transactions per truncate/load/DQ). If **`006`** has not been applied, **`silver_done`** fails and its audit insert rolls back; **`silver.*`** may already reflect partial updates from the same run.

## Examples (Docker Compose)

From the **repository root**, with the stack up (`make up`) and credentials matching `docker/.env`.

`docker compose` in this repo expects **`AIRFLOW_COMPOSE_CONTEXT`** (see `Makefile`). When not using `make`, export it first:

```bash
export AIRFLOW_COMPOSE_CONTEXT="$(pwd)"
```

**Source**

```bash
docker compose --env-file docker/.env -f docker/docker-compose.yaml --project-directory "$(pwd)" exec -T \
  postgres_source psql -U "${POSTGRES_SOURCE_USER:-source_user}" -d "${POSTGRES_SOURCE_DB:-source_db}" \
  < scripts/sql/ddl/001_source_public.sql
```

**Warehouse (pipe all warehouse scripts in order)**

```bash
for f in scripts/sql/ddl/002_warehouse_schemas.sql \
         scripts/sql/ddl/003_warehouse_bronze_tables.sql \
         scripts/sql/ddl/004_warehouse_silver_stg_tables.sql \
         scripts/sql/ddl/005_warehouse_silver_tables.sql \
         scripts/sql/ddl/006_warehouse_audit.sql; do
  docker compose --env-file docker/.env -f docker/docker-compose.yaml --project-directory "$(pwd)" exec -T \
    postgres_warehouse psql -U "${POSTGRES_WAREHOUSE_USER:-warehouse_user}" -d "${POSTGRES_WAREHOUSE_DB:-warehouse_db}" \
    < "$f"
done
```

Host-only clients (DBeaver, `psql` on the host) should use **mapped ports** from `docker/.env` (`POSTGRES_SOURCE_PORT`, `POSTGRES_WAREHOUSE_PORT`), not `5432`.

---

## Source seed (`scripts/sql/seed/`) — phase 04

**Seeding is not run by `make check_all`.** Apply manually after DDL on **`postgres_source`** only.

| Order | File | Purpose |
|-------|------|---------|
| 1 | `seed/001_truncate_source_public.sql` | Truncate all `public` pipeline tables and **restart identity** (use before re-seed if data exists). |
| 2 | `seed/002_seed_dimensions.sql` | Competition, venues, season, teams, players, contracts (includes **duplicate** contracts), rounds. |
| 3 | `seed/003_seed_facts.sql` | Fixtures (`scheduled` + `finished`) and `match_result` for the finished fixture. |

### Expected row counts (after 001 → 002 → 003)

| Table | Rows |
|-------|------|
| `competition` | 1 |
| `venue` | 3 |
| `season` | 1 |
| `team` | 6 |
| `player` | 8 |
| `player_contract` | 9 |
| `matchday_round` | 3 |
| `fixture` | 3 |
| `match_result` | 1 |

**Dedupe fixture:** `player_contract` rows **`103`** and **`104`** share the same **`(player_id, team_id, valid_from)`** = `(3, 2, 2024-07-01)` (different `shirt_number` / `player_contract_id`). Use for phase `08` **`DISTINCT ON`** / window dedupe.

**Quality-check fixtures:** `player_id = 3` has **`nationality` NULL**; `player_id = 7` has **`preferred_foot` NULL**.

### Apply seed (Docker Compose)

From repo root, with `AIRFLOW_COMPOSE_CONTEXT` set (see above) and stack up:

```bash
export AIRFLOW_COMPOSE_CONTEXT="$(pwd)"
for f in scripts/sql/seed/001_truncate_source_public.sql \
         scripts/sql/seed/002_seed_dimensions.sql \
         scripts/sql/seed/003_seed_facts.sql; do
  docker compose --env-file docker/.env -f docker/docker-compose.yaml --project-directory "$(pwd)" exec -T \
    postgres_source psql -U "${POSTGRES_SOURCE_USER:-source_user}" -d "${POSTGRES_SOURCE_DB:-source_db}" \
    < "$f"
done
```

### Verify counts and duplicates

```sql
SELECT relname AS table, n_live_tup::bigint AS approx
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY relname;
```

Or exact:

```sql
SELECT 'competition' AS t, COUNT(*) FROM public.competition
UNION ALL SELECT 'venue', COUNT(*) FROM public.venue
UNION ALL SELECT 'season', COUNT(*) FROM public.season
UNION ALL SELECT 'team', COUNT(*) FROM public.team
UNION ALL SELECT 'player', COUNT(*) FROM public.player
UNION ALL SELECT 'player_contract', COUNT(*) FROM public.player_contract
UNION ALL SELECT 'matchday_round', COUNT(*) FROM public.matchday_round
UNION ALL SELECT 'fixture', COUNT(*) FROM public.fixture
UNION ALL SELECT 'match_result', COUNT(*) FROM public.match_result;
```

Duplicate logical keys on contracts:

```sql
SELECT player_id, team_id, valid_from, COUNT(*) AS cnt
FROM public.player_contract
GROUP BY player_id, team_id, valid_from
HAVING COUNT(*) > 1;
```

### After phase 09 (`load_silver_stg_tables` / per-table `load_<table>` on warehouse)

Bronze mirrors source row counts (`player_contract` still has **9** rows). **`silver_stg`** should have **one row per natural key**; after Airflow loads **`silver_stg`**:

```sql
SELECT player_id, team_id, valid_from, COUNT(*) AS cnt
FROM silver_stg.player_contract
GROUP BY player_id, team_id, valid_from
HAVING COUNT(*) > 1;
```

Expect **no rows**. The winning duplicate is the row with the higher **`player_contract_id`** (**104**). Confirm **`_batch_id`** and **`_ingested_at`** are set on `silver_stg.player_contract` (and other **`silver_stg`** tables).
