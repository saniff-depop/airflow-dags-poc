"""Render **Soda Core 3** SodaCL check YAML from Jinja templates.

Templates live under ``<soda_root>/checks/_templates/``. In Compose, ``soda_root``
is typically ``/opt/airflow/soda``; in unit tests, pass the repo's ``soda``
directory.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Final, Literal

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER

_TEMPLATE_BRONZE: Final[str] = "bronze_table_contract.yml.jinja"
_TEMPLATE_BRONZE_SOURCE_PRECHECKS: Final[str] = "bronze_source_prechecks.yml.jinja"
_TEMPLATE_SILVER_STG: Final[str] = "silver_stg_table_contract.yml.jinja"

# Pre-bronze freshness: Soda compares scan time to MAX(kickoff_utc). Wide defaults
# keep the bundled historical seed valid; use e.g. ``3d`` / ``8d`` for live OLTP.
BRONZE_SOURCE_FIXTURE_FRESHNESS_MAX_AGE: Final[str] = "3650d"
BRONZE_SOURCE_MATCH_RESULT_PROXY_FRESHNESS_MAX_AGE: Final[str] = "3650d"


def soda_postgres_type_for_contract(data_type: str) -> str:
    """Map contract/DDL-style types to Postgres types for SodaCL ``schema`` checks."""
    raw = data_type.strip()
    upper = raw.upper()
    known: dict[str, str] = {
        "BIGINT": "bigint",
        "TEXT": "text",
        "CHAR": "character",
        "INTEGER": "integer",
        "SMALLINT": "smallint",
        "BOOLEAN": "boolean",
        "DATE": "date",
        "TIMESTAMP_TZ": "timestamp with time zone",
    }
    if upper in known:
        return known[upper]
    if upper.startswith("NUMERIC"):
        tail = raw.split("NUMERIC", 1)[1].strip().lower().replace(" ", "")
        return f"numeric{tail}"
    return raw.lower()


def _columns_with_pg_types(
    cols: tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cols:
        row = dict(c)
        row["pg_type"] = soda_postgres_type_for_contract(str(row["data_type"]))
        out.append(row)
    return out


def _column(name: str, data_type: str, *, optional: bool = False) -> dict[str, Any]:
    col: dict[str, Any] = {"name": name, "data_type": data_type}
    if optional:
        col["optional"] = True
    return col


# Aligned with ``scripts/sql/ddl/003_warehouse_bronze_tables.sql``.
BRONZE_CONTRACT_COLUMNS: Final[dict[str, tuple[dict[str, Any], ...]]] = {
    "competition": (
        _column("competition_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("country_code", "CHAR", optional=True),
    ),
    "venue": (
        _column("venue_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("city", "TEXT", optional=True),
        _column("capacity", "INTEGER", optional=True),
    ),
    "season": (
        _column("season_id", "BIGINT"),
        _column("competition_id", "BIGINT"),
        _column("label", "TEXT"),
        _column("start_date", "DATE"),
        _column("end_date", "DATE"),
    ),
    "team": (
        _column("team_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("short_name", "TEXT"),
        _column("founded_year", "SMALLINT", optional=True),
        _column("venue_id", "BIGINT", optional=True),
    ),
    "player": (
        _column("player_id", "BIGINT"),
        _column("given_name", "TEXT"),
        _column("family_name", "TEXT"),
        _column("date_of_birth", "DATE", optional=True),
        _column("nationality", "TEXT", optional=True),
        _column("preferred_foot", "TEXT", optional=True),
    ),
    "player_contract": (
        _column("player_contract_id", "BIGINT"),
        _column("player_id", "BIGINT"),
        _column("team_id", "BIGINT"),
        _column("valid_from", "DATE"),
        _column("valid_to", "DATE", optional=True),
        _column("shirt_number", "SMALLINT", optional=True),
        _column("is_loan", "BOOLEAN"),
    ),
    "matchday_round": (
        _column("round_id", "BIGINT"),
        _column("season_id", "BIGINT"),
        _column("round_number", "SMALLINT"),
        _column("label", "TEXT"),
    ),
    "fixture": (
        _column("fixture_id", "BIGINT"),
        _column("season_id", "BIGINT"),
        _column("round_id", "BIGINT"),
        _column("home_team_id", "BIGINT"),
        _column("away_team_id", "BIGINT"),
        _column("venue_id", "BIGINT"),
        _column("kickoff_utc", "TIMESTAMP_TZ"),
        _column("status", "TEXT"),
    ),
    "match_result": (
        _column("fixture_id", "BIGINT"),
        _column("home_goals", "SMALLINT"),
        _column("away_goals", "SMALLINT"),
        # DDL NUMERIC(5,3); PG metadata reports plain ``numeric`` for Soda.
        _column("home_xg", "NUMERIC", optional=True),
        _column("away_xg", "NUMERIC", optional=True),
    ),
}


# Matches ``silver_stg`` DDL in ``004_warehouse_silver_stg_tables.sql`` (+ lineage).
SILVER_STG_CONTRACT_COLUMNS: Final[dict[str, tuple[dict[str, Any], ...]]] = {
    "competition": (
        _column("competition_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("country_code", "CHAR", optional=True),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "venue": (
        _column("venue_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("city", "TEXT", optional=True),
        _column("capacity", "INTEGER", optional=True),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "season": (
        _column("season_id", "BIGINT"),
        _column("competition_id", "BIGINT"),
        _column("label", "TEXT"),
        _column("start_date", "DATE"),
        _column("end_date", "DATE"),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "team": (
        _column("team_id", "BIGINT"),
        _column("name", "TEXT"),
        _column("short_name", "TEXT"),
        _column("founded_year", "SMALLINT", optional=True),
        _column("venue_id", "BIGINT", optional=True),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "player": (
        _column("player_id", "BIGINT"),
        _column("given_name", "TEXT"),
        _column("family_name", "TEXT"),
        _column("date_of_birth", "DATE", optional=True),
        _column("nationality", "TEXT", optional=True),
        _column("preferred_foot", "TEXT", optional=True),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "player_contract": (
        _column("player_contract_id", "BIGINT"),
        _column("player_id", "BIGINT"),
        _column("team_id", "BIGINT"),
        _column("valid_from", "DATE"),
        _column("valid_to", "DATE", optional=True),
        _column("shirt_number", "SMALLINT", optional=True),
        _column("is_loan", "BOOLEAN"),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "matchday_round": (
        _column("round_id", "BIGINT"),
        _column("season_id", "BIGINT"),
        _column("round_number", "SMALLINT"),
        _column("label", "TEXT"),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "fixture": (
        _column("fixture_id", "BIGINT"),
        _column("season_id", "BIGINT"),
        _column("round_id", "BIGINT"),
        _column("home_team_id", "BIGINT"),
        _column("away_team_id", "BIGINT"),
        _column("venue_id", "BIGINT"),
        _column("kickoff_utc", "TIMESTAMP_TZ"),
        _column("status", "TEXT"),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
    "match_result": (
        _column("fixture_id", "BIGINT"),
        _column("home_goals", "SMALLINT"),
        _column("away_goals", "SMALLINT"),
        _column("home_xg", "NUMERIC", optional=True),
        _column("away_xg", "NUMERIC", optional=True),
        _column("_ingested_at", "TIMESTAMP_TZ"),
        _column("_batch_id", "TEXT"),
        _column("_source_system", "TEXT"),
    ),
}


def soda_checks_templates_dir(*, soda_root: Path) -> Path:
    """Return ``<soda_root>/checks/_templates`` as an absolute path."""
    return (soda_root.resolve() / "checks" / "_templates").resolve()


def _jinja_environment(*, soda_root: Path) -> Environment:
    templates_dir = soda_checks_templates_dir(soda_root=soda_root)
    return Environment(
        loader=FileSystemLoader(str(templates_dir)),
        undefined=StrictUndefined,
        autoescape=False,
    )


def bronze_contract_template_context(*, table: str) -> dict[str, Any]:
    """Column list for bronze Jinja SodaCL (includes ``pg_type`` for schema checks)."""
    cols = BRONZE_CONTRACT_COLUMNS.get(table)
    if cols is None:
        msg = f"No bronze contract columns defined for table {table!r}"
        raise ValueError(msg)
    return {"columns": _columns_with_pg_types(cols)}


def bronze_source_prechecks_template_context(*, table: str) -> dict[str, Any]:
    """Jinja context for ``bronze_source_prechecks.yml.jinja`` (OLTP, pre-truncate)."""
    if table not in BRONZE_TABLE_LOAD_ORDER:
        msg = f"No bronze source precheck defined for table {table!r}"
        raise ValueError(msg)
    return {
        **bronze_contract_template_context(table=table),
        "table": table,
        "fixture_freshness_max_age": BRONZE_SOURCE_FIXTURE_FRESHNESS_MAX_AGE,
        "match_result_proxy_freshness_max_age": (
            BRONZE_SOURCE_MATCH_RESULT_PROXY_FRESHNESS_MAX_AGE
        ),
    }


def silver_stg_contract_template_context(*, table: str) -> dict[str, Any]:
    """Column list for silver_stg Jinja SodaCL (includes ``pg_type``)."""
    cols = SILVER_STG_CONTRACT_COLUMNS.get(table)
    if cols is None:
        msg = f"No silver_stg contract columns defined for table {table!r}"
        raise ValueError(msg)
    return {"columns": _columns_with_pg_types(cols)}


def silver_stg_pre_silver_template_context(*, table: str) -> dict[str, Any]:
    """Jinja context for per-table pre-silver silver_stg contracts (DAG fan-out).

    Always includes full ``columns`` for the table. Maps known tables to duplicate /
    row-count / missing-name guards; others get schema-only checks via the template.
    """
    ctx: dict[str, Any] = {
        **silver_stg_contract_template_context(table=table),
        "table": table,
    }
    if table == "player_contract":
        ctx["duplicate_columns"] = ["player_id", "team_id", "valid_from"]
        ctx["row_count_must_be"] = 300
    elif table == "player":
        ctx["duplicate_columns"] = ["player_id"]
        ctx["missing_column_names"] = ["given_name", "family_name"]
    return ctx


def render_contract_yaml_for_layer(
    *,
    layer: Literal["bronze", "silver_stg"],
    context: Mapping[str, Any],
    soda_root: Path,
) -> str:
    """Render bronze or silver_stg ``*_table_contract.yml.jinja`` as **SodaCL** YAML.

    **Bronze** post-load template omits schema (validated on source in pre_checks);
    **silver_stg** still embeds schema in the template. Context needs ``table`` and
    ``columns`` via :func:`bronze_contract_template_context`,
    :func:`silver_stg_contract_template_context`, or
    :func:`silver_stg_pre_silver_template_context`. Optional ``duplicate_columns``
    enables duplicate ``failed rows`` on bronze or silver_stg. Silver_stg may use
    ``row_count_must_be`` and ``missing_column_names``. With
    :class:`StrictUndefined`, omit optional keys you do not need.
    """
    name = _TEMPLATE_BRONZE if layer == "bronze" else _TEMPLATE_SILVER_STG
    env = _jinja_environment(soda_root=soda_root)
    template = env.get_template(name)
    rendered: str = template.render(dict(context))
    return rendered


def render_bronze_contract_yaml(
    *,
    table: str,
    soda_root: Path,
    duplicate_columns: list[str] | None = None,
) -> str:
    """Render the bronze per-table contract template."""
    ctx: dict[str, Any] = {
        **bronze_contract_template_context(table=table),
        "table": table,
    }
    if duplicate_columns is not None:
        ctx["duplicate_columns"] = duplicate_columns
    return render_contract_yaml_for_layer(
        layer="bronze",
        context=ctx,
        soda_root=soda_root,
    )


def render_bronze_source_prechecks_yaml(*, table: str, soda_root: Path) -> str:
    """Render SodaCL for source schema, freshness, and presence before bronze load."""
    env = _jinja_environment(soda_root=soda_root)
    template = env.get_template(_TEMPLATE_BRONZE_SOURCE_PRECHECKS)
    ctx = bronze_source_prechecks_template_context(table=table)
    rendered: str = template.render(ctx)
    return rendered


def render_silver_stg_contract_yaml(
    *,
    table: str,
    soda_root: Path,
    duplicate_columns: list[str] | None = None,
) -> str:
    """Render the silver_stg per-table contract template."""
    ctx: dict[str, Any] = {
        **silver_stg_contract_template_context(table=table),
        "table": table,
    }
    if duplicate_columns is not None:
        ctx["duplicate_columns"] = duplicate_columns
    return render_contract_yaml_for_layer(
        layer="silver_stg",
        context=ctx,
        soda_root=soda_root,
    )


def write_rendered_contract_to_tempfile(rendered_yaml: str) -> Path:
    """Persist UTF-8 YAML to a temporary ``.yml`` file and return its path.

    The file is not auto-deleted. Callers should remove the path when finished
    (e.g. ``path.unlink(missing_ok=True)`` in a ``finally`` block).
    """
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yml",
        delete=False,
    ) as tmp:
        tmp.write(rendered_yaml)
        return Path(tmp.name)
