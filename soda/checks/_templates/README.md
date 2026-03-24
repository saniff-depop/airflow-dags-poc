# Jinja templates → SodaCL (Soda Core 3)

These files are **not** standalone check YAML. They are rendered at **runtime** with **Jinja2** (`StrictUndefined`) via **`dags/ingestion/football_ingest/include/soda_contract_template.py`**, written to a temporary `.yml` path, and passed to **`soda.scan.Scan`** together with **`soda/configuration.compose.yml`** (or a local override).
