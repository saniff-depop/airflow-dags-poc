# Pre-silver Soda checks (Library 4)

Data contracts gate **`publish_silver_tables`** / **`silver_done`** (silver load). Per-table checks use **Jinja** (`checks/_templates/silver_stg_table_contract.yml.jinja`) at runtime.

## Airflow (phase 09)

- **`load_silver_stg_tables`:** for each table in **`SILVER_STG_TABLE_LOAD_ORDER`**, nested **`load_silver_stg_tables.<table>`**: **`load_<table>`** → **`post_checks`** (`PythonVirtualenvOperator` → **`run_soda_contract_for_table`**, `layer="silver_stg"`, context from **`silver_stg_pre_silver_template_context`**).
- **`scan_label`:** `football_ingest_pre_silver_<table>_v1` per branch.

## DQ coverage (silver_stg entities)

| Table | Contract source | Checks |
|-------|-----------------|--------|
| All in **`SILVER_STG_TABLE_LOAD_ORDER`** | Jinja template | **`schema: {}`** minimum |
| **`player`** | Jinja + context | **`duplicate`** on `player_id`; **`missing`** on `given_name`, `family_name` |
| **`player_contract`** | Jinja + context | **`duplicate`** on `(player_id, team_id, valid_from)`; **`row_count`** must_be **8** |
| **`fixture`** | Jinja + context | **`schema: {}`** (reference hand YAML omits row-level checks) |

## Hand-written YAML in this directory

Files such as **`silver_stg_player_contract.yml`**, **`silver_stg_player.yml`**, **`silver_stg_fixture.yml`** are **reference / CLI examples** only; the DAG does **not** load them (avoids duplicate Soda work). For local CLI verification of those shapes, see root **`soda/README.md`**.
