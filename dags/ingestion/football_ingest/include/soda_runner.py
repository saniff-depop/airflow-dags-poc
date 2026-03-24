"""Soda Core 3 SodaCL scans for Airflow ``PythonVirtualenvOperator`` tasks.

``run_*`` shims add the DAGs folder to ``sys.path``, import this module, then call
``_*_impl`` (venv ``script.py`` embeds only the shim; venv subprocess has no
``PYTHONPATH``).

**PythonVirtualenvOperator** ``requirements`` should match ``pyproject.toml`` dev pins
(``soda-core-postgres``, ``jinja2``, ``setuptools``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, Literal

from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER

logger = logging.getLogger(__name__)

# Keep in sync with ``pyproject.toml`` dev / Airflow venv installs.
VENV_REQUIREMENTS_SODA_AND_JINJA: Final[tuple[str, ...]] = (
    "soda-core-postgres==3.5.6",
    "jinja2==3.1.6",
    "setuptools==82.0.1",
)

# Scan data source names must match ``soda/configuration*.yml``. Each warehouse entry
# sets Postgres ``schema`` / ``search_path``; SodaCL uses bare ``checks for <table>:``.
WAREHOUSE_BRONZE_DATA_SOURCE: Final[str] = "football_warehouse"
WAREHOUSE_SILVER_STG_DATA_SOURCE: Final[str] = "football_warehouse_silver_stg"
SOURCE_OLTP_DATA_SOURCE: Final[str] = "football_source"


def _postgres_sql_quoted_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _count_rows_via_hook(*, postgres_conn_id: str, schema: str, table: str) -> int:
    """Row count via :class:`~airflow.providers.postgres.hooks.postgres.PostgresHook`.

    Uses the same client stack as bronze load tasks (psycopg2 from the Postgres
    provider), not ``psycopg`` v3, so TaskFlow tasks work on a stock Airflow image.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    q_schema = _postgres_sql_quoted_ident(schema)
    q_table = _postgres_sql_quoted_ident(table)
    sql = f"SELECT COUNT(*) FROM {q_schema}.{q_table}"
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    records = hook.get_records(sql)
    if not records:
        return 0
    return int(records[0][0])


def _count_rows_via_psycopg(*, pg_uri: str, schema: str, table: str) -> int:
    """Row count over a libpq URI (``psycopg`` v3), e.g. isolated Soda venv tasks."""
    import psycopg
    from psycopg import sql

    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(table),
    )
    with psycopg.connect(pg_uri) as conn, conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def assert_source_bronze_row_parity(
    *,
    tables: tuple[str, ...],
    source_conn_id: str | None = None,
    warehouse_conn_id: str | None = None,
    source_pg_uri: str | None = None,
    warehouse_pg_uri: str | None = None,
) -> None:
    """Fail if source public and warehouse bronze row counts differ for any table.

    Pass either Airflow connection ids (worker TaskFlow) or Postgres URIs (e.g. Soda
    venv entrypoints with no Airflow import).
    """
    for table in tables:
        assert_source_bronze_row_parity_one_table(
            table=table,
            source_conn_id=source_conn_id,
            warehouse_conn_id=warehouse_conn_id,
            source_pg_uri=source_pg_uri,
            warehouse_pg_uri=warehouse_pg_uri,
        )


def assert_source_bronze_row_parity_one_table(
    *,
    table: str,
    source_conn_id: str | None = None,
    warehouse_conn_id: str | None = None,
    source_pg_uri: str | None = None,
    warehouse_pg_uri: str | None = None,
) -> None:
    """Fail if ``public.<table>`` and ``bronze.<table>`` row counts differ."""
    if table not in BRONZE_TABLE_LOAD_ORDER:
        msg = f"Parity check unsupported for table {table!r}"
        raise ValueError(msg)
    hook_mode = source_conn_id is not None and warehouse_conn_id is not None
    uri_mode = source_pg_uri is not None and warehouse_pg_uri is not None
    if hook_mode == uri_mode:
        msg = (
            "Provide exactly one of: (source_conn_id, warehouse_conn_id) or "
            "(source_pg_uri, warehouse_pg_uri)"
        )
        raise ValueError(msg)
    if hook_mode:
        public_n = _count_rows_via_hook(
            postgres_conn_id=source_conn_id, schema="public", table=table
        )
        bronze_n = _count_rows_via_hook(
            postgres_conn_id=warehouse_conn_id, schema="bronze", table=table
        )
    else:
        public_n = _count_rows_via_psycopg(
            pg_uri=source_pg_uri, schema="public", table=table
        )
        bronze_n = _count_rows_via_psycopg(
            pg_uri=warehouse_pg_uri, schema="bronze", table=table
        )
    if public_n != bronze_n:
        msg = (
            f"Row count mismatch for {table}: "
            f"source public.{table}={public_n} "
            f"vs warehouse bronze.{table}={bronze_n}"
        )
        raise RuntimeError(msg)


def verify_sodacl_checks_at_path(
    *,
    configuration_yaml: str,
    checks_file_path: str,
    scan_label: str,
    data_source_name: str = WAREHOUSE_BRONZE_DATA_SOURCE,
    variables: Mapping[str, str] | None = None,
) -> None:
    """Run a Soda Core 3 ``Scan`` over one SodaCL checks file; raise on failure."""
    from pathlib import Path as _Path

    from soda.scan import Scan

    print("=" * 80)
    print(f"{scan_label}: Soda Core 3 scan")
    print(f"Configuration: {configuration_yaml}")
    print(f"Checks file: {checks_file_path}")
    try:
        print("\nChecks YAML:\n" + _Path(checks_file_path).read_text(encoding="utf-8"))
    except OSError as e:
        print(f"Warning: could not read checks file for logging: {e}")
    print("-" * 80)

    scan = Scan()
    scan.set_scan_definition_name(scan_label)
    scan.add_configuration_yaml_file(configuration_yaml)
    scan.set_data_source_name(data_source_name)
    if variables:
        scan.add_variables(dict(variables))
    scan.add_sodacl_yaml_file(checks_file_path)
    scan.set_verbose(False)
    scan.execute()

    logs = scan.get_logs_text()
    if logs:
        print(logs)
    logger.info("%s: checks_file=%s", scan_label, checks_file_path)

    if scan.has_check_fails() or scan.has_error_or_warning_logs():
        detail = (logs or scan.get_error_or_warning_logs_text() or "").strip()
        raise RuntimeError(detail or "Soda scan failed")


def _run_soda_contract_for_table_impl(
    *,
    project_root: str,
    layer: Literal["bronze", "silver_stg"],
    table: str,
    configuration_yaml: str,
    scan_label: str,
    template_context: Mapping[str, Any] | None,
    duplicate_columns: list[str] | None,
) -> None:
    """Render Jinja SodaCL to a temp file, run ``Scan``, then delete the file."""
    soda_root = Path(project_root).resolve()
    context: dict[str, Any] = {**(dict(template_context) if template_context else {})}
    context["table"] = table
    if duplicate_columns is not None:
        context["duplicate_columns"] = duplicate_columns

    from ingestion.football_ingest.include.soda_contract_template import (
        bronze_contract_template_context,
        render_contract_yaml_for_layer,
        silver_stg_contract_template_context,
        write_rendered_contract_to_tempfile,
    )

    if layer == "bronze":
        for key, value in bronze_contract_template_context(table=table).items():
            context.setdefault(key, value)
    else:
        for key, value in silver_stg_contract_template_context(table=table).items():
            context.setdefault(key, value)

    print("=" * 80)
    print(f"{scan_label}: Rendering templated SodaCL for table '{table}'")
    print(f"Layer: {layer}")
    print(f"Template context keys: {list(context.keys())}")
    print()

    rendered = render_contract_yaml_for_layer(
        layer=layer,
        context=context,
        soda_root=soda_root,
    )

    print("-" * 80)
    print(f"Rendered SodaCL for {layer}.{table}:")
    print(rendered)
    print("=" * 80)
    print()

    warehouse_ds = (
        WAREHOUSE_SILVER_STG_DATA_SOURCE
        if layer == "silver_stg"
        else WAREHOUSE_BRONZE_DATA_SOURCE
    )
    tmp_path = write_rendered_contract_to_tempfile(rendered)
    try:
        verify_sodacl_checks_at_path(
            configuration_yaml=configuration_yaml,
            checks_file_path=str(tmp_path),
            scan_label=scan_label,
            data_source_name=warehouse_ds,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def run_soda_contract_for_table(
    *,
    project_root: str,
    layer: Literal["bronze", "silver_stg"],
    table: str,
    configuration_yaml: str,
    scan_label: str,
    template_context: Mapping[str, Any] | None = None,
    duplicate_columns: list[str] | None = None,
) -> None:
    """Venv-safe entry: render templated SodaCL for one table and run a scan."""
    import os as _os
    import sys as _sys

    dags_dir = (_os.environ.get("AIRFLOW__CORE__DAGS_FOLDER") or "").strip()
    if not dags_dir:
        dags_dir = "/opt/airflow/dags"
    if dags_dir not in _sys.path:
        _sys.path.insert(0, dags_dir)

    import ingestion.football_ingest.include.soda_runner as impl

    impl._run_soda_contract_for_table_impl(
        project_root=project_root,
        layer=layer,
        table=table,
        configuration_yaml=configuration_yaml,
        scan_label=scan_label,
        template_context=template_context,
        duplicate_columns=duplicate_columns,
    )


def _run_soda_bronze_source_prechecks_impl(
    *,
    project_root: str,
    table: str,
    configuration_yaml: str,
    scan_label: str,
) -> None:
    """Render source pre-bronze SodaCL, run ``Scan`` on ``football_source``."""
    soda_root = Path(project_root).resolve()
    from ingestion.football_ingest.include.soda_contract_template import (
        render_bronze_source_prechecks_yaml,
        write_rendered_contract_to_tempfile,
    )

    rendered = render_bronze_source_prechecks_yaml(table=table, soda_root=soda_root)
    print("=" * 80)
    print(f"{scan_label}: pre-bronze source checks for '{table}'")
    print(rendered)
    print("=" * 80)

    tmp_path = write_rendered_contract_to_tempfile(rendered)
    try:
        verify_sodacl_checks_at_path(
            configuration_yaml=configuration_yaml,
            checks_file_path=str(tmp_path),
            scan_label=scan_label,
            data_source_name=SOURCE_OLTP_DATA_SOURCE,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def run_soda_bronze_source_prechecks_for_table(
    *,
    project_root: str,
    table: str,
    configuration_yaml: str,
    scan_label: str,
) -> None:
    """Venv-safe entry: source freshness / presence before bronze truncate."""
    import os as _os
    import sys as _sys

    dags_dir = (_os.environ.get("AIRFLOW__CORE__DAGS_FOLDER") or "").strip()
    if not dags_dir:
        dags_dir = "/opt/airflow/dags"
    if dags_dir not in _sys.path:
        _sys.path.insert(0, dags_dir)

    import ingestion.football_ingest.include.soda_runner as impl

    impl._run_soda_bronze_source_prechecks_impl(
        project_root=project_root,
        table=table,
        configuration_yaml=configuration_yaml,
        scan_label=scan_label,
    )


def run_post_bronze_soda_checks(
    *,
    project_root: str,
    scan_definition_name: str,
    checks_subpath: str,
    configuration_path: str,
    configuration_yaml: str,
    source_pg_uri: str,
    warehouse_pg_uri: str,
) -> None:
    """Venv-safe entry: load full module from disk, then run implementation."""
    import os as _os
    import sys as _sys

    dags_dir = (_os.environ.get("AIRFLOW__CORE__DAGS_FOLDER") or "").strip()
    if not dags_dir:
        dags_dir = "/opt/airflow/dags"
    if dags_dir not in _sys.path:
        _sys.path.insert(0, dags_dir)

    import ingestion.football_ingest.include.soda_runner as impl

    impl._run_post_bronze_soda_checks_impl(
        project_root=project_root,
        scan_definition_name=scan_definition_name,
        checks_subpath=checks_subpath,
        configuration_path=configuration_path,
        configuration_yaml=configuration_yaml,
        source_pg_uri=source_pg_uri,
        warehouse_pg_uri=warehouse_pg_uri,
    )


def _run_post_bronze_soda_checks_impl(
    *,
    project_root: str,
    scan_definition_name: str,
    checks_subpath: str,
    configuration_path: str,
    configuration_yaml: str,
    source_pg_uri: str,
    warehouse_pg_uri: str,
) -> None:
    """Post-bronze batch: row parity for all tables and SodaCL scan for competition."""
    print("=" * 80)
    print(f"Soda post-bronze: scan_definition_name={scan_definition_name}")
    print(f"  checks_subpath={checks_subpath}")
    print(f"  configuration_path={configuration_path}")
    print()

    logger.info(
        "Soda post-bronze: scan_definition_name=%s checks_subpath=%s "
        "configuration_path=%s",
        scan_definition_name,
        checks_subpath,
        configuration_path,
    )

    root = Path(project_root).resolve()
    table_count = len(BRONZE_TABLE_LOAD_ORDER)
    print(f"Verifying row parity between source and bronze for {table_count} tables...")
    logger.info(
        "Verifying row parity between source and bronze for %d tables...",
        table_count,
    )

    assert_source_bronze_row_parity(
        tables=BRONZE_TABLE_LOAD_ORDER,
        source_pg_uri=source_pg_uri,
        warehouse_pg_uri=warehouse_pg_uri,
    )

    print("✓ Row parity verification passed for all tables")
    logger.info("✓ Row parity verification passed for all tables")

    from ingestion.football_ingest.include.soda_contract_template import (
        render_bronze_contract_yaml,
        write_rendered_contract_to_tempfile,
    )

    rendered = render_bronze_contract_yaml(table="competition", soda_root=root)
    tmp_path = write_rendered_contract_to_tempfile(rendered)
    try:
        verify_sodacl_checks_at_path(
            configuration_yaml=configuration_yaml,
            checks_file_path=str(tmp_path),
            scan_label=f"Soda post-bronze ({scan_definition_name})",
            data_source_name=WAREHOUSE_BRONZE_DATA_SOURCE,
        )
    finally:
        tmp_path.unlink(missing_ok=True)
