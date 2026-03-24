# Quick Start Guide

## Fresh Start (Automated Setup)

### Start Everything with Data

```bash
make up_with_data
```

This single command will:
1. ✅ Start all Docker services (Airflow, Postgres source, Postgres warehouse, MySQL)
2. ✅ Wait for databases to be healthy
3. ✅ Create source database schema
4. ✅ Seed source with **real 2024-25 Premier League data** (20 teams, 300 players, 200 matches)
5. ✅ Create warehouse schemas (bronze, silver_stg, silver, audit)
6. ✅ Create all warehouse tables
7. ✅ Drop foreign keys for parallel loading
8. ✅ Create Airflow connections (`football_pg_source`, `football_pg_warehouse`)

**Access Airflow:** http://localhost:8080 (admin/admin)

### Clean Slate

```bash
make down
```

This will:
- Stop all containers
- Remove all volumes (databases, logs)
- Give you a fresh start for next `make up_with_data`

## Data Summary

After `make up_with_data`, your source database contains:

| Table | Count | Description |
|-------|-------|-------------|
| `competition` | 1 | Premier League |
| `venue` | 20 | Real stadiums (Emirates, Anfield, etc.) |
| `team` | 20 | All 2024-25 Premier League clubs |
| `player` | 300 | Real players (15 per team) |
| `player_contract` | 302 | Contracts with 2 intentional duplicates for testing |
| `matchday_round` | 20 | Matchweeks 1-20 |
| `fixture` | 200 | Completed matches (Aug 2024 - Jan 2025) |
| `match_result` | 200 | Results with scores and xG |

## Running the DAG

1. Go to http://localhost:8080
2. Enable the `football_ingest` DAG
3. Click "Trigger DAG"
4. Watch it process through:
   - `ensure_schemas` → Verify warehouse setup
   - `load_bronze_tables` → Nested group per table: `truncate_<table>` → `load_<table>` → `post_checks` TaskGroup with `schema_check` and `row_count_check` in parallel (9 branches in parallel)
   - `bronze_done` → Bronze phase marker (`EmptyOperator`)
   - `load_silver_stg_tables` → Per table nested group: `load_<table>` → `post_check`
   - `silver_stg_done` → Fan-in after pre-silver per-table checks
   - `publish_silver_tables` → Per table nested group: `truncate_<table>` → `load_<table>` → `post_check` (parallel branches)
   - `silver_done` → Audit row + dataset for downstream dbt

## Manual Database Operations (Optional)

If you only want to initialize databases (without starting services):
```bash
make init_databases
```

If you only want to create Airflow connections:
```bash
make init_airflow_connections
```

## Workflow Comparison

### Old Way (Manual)
```bash
make up
# Wait for services...
# Run multiple psql commands manually
# Create connections in Airflow UI
# Hope everything works
```

### New Way (Automated)
```bash
make down           # Clean slate
make up_with_data   # Everything ready in 1 command
```

## Troubleshooting

### Services not healthy
- Wait 30-60 seconds after `make up_with_data`
- Check logs: `docker compose logs -f airflow-scheduler`

### Connection errors in DAG
- Verify connections: `docker compose exec airflow-scheduler airflow connections list`
- Should see `football_pg_source` and `football_pg_warehouse`

### Data not seeded
- Check if init scripts ran: Look for "✅ Database initialization complete!" in output
- Manually run: `make init_databases`

## Development Workflow

```bash
# Make code changes...

make check_all      # Run linting + tests

# Test in Airflow:
make down
make up_with_data   # Fresh environment every time
# Trigger DAG, verify changes
```
