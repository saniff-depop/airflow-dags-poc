# Post-bronze Soda checks (Soda Core 3)

Per-table **bronze** checks are **rendered from Jinja** at runtime (`checks/_templates/bronze_table_contract.yml.jinja`) and executed via `soda.scan.Scan` using [`configuration.compose.yml`](../../configuration.compose.yml).

Each rendered file includes **schema**, **missing_count** on required columns, **`row_count same as public.<table> in football_source`**, and optional duplicate-group **`failed rows`** checks.

See root **`soda/README.md`** and **`specs/improvements/soda-v3-cross-checks-migration/`** for migration notes.
