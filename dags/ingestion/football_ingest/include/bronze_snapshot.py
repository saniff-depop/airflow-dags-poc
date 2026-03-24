"""Bronze layer: public (source) → bronze (warehouse).

**Per-table replace** — :func:`run_bronze_table_replace` truncates a single
``bronze.<table>`` then copies ``public.<table>`` into it. Use one Airflow task
per table name for parallel loads (warehouse has no FKs on bronze).

**Full snapshot** — :func:`run_full_bronze_snapshot` loops
:data:`BRONZE_TABLE_LOAD_ORDER` and calls :func:`run_bronze_table_replace` per
table (no single global ``TRUNCATE`` before the loop).

**Reset** — :func:`truncate_all_bronze_tables` runs the legacy multi-table
``TRUNCATE … CASCADE`` for operational emptying without reload.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

# FK-safe insert order (parents before children).
BRONZE_TABLE_LOAD_ORDER: Final[tuple[str, ...]] = (
    "competition",
    "venue",
    "season",
    "team",
    "player",
    "player_contract",
    "matchday_round",
    "fixture",
    "match_result",
)

# TRUNCATE children before parents (same order as source seed 001_truncate).
_TRUNCATE_BRONZE_SQL: Final[str] = """
TRUNCATE TABLE
    bronze.match_result,
    bronze.fixture,
    bronze.matchday_round,
    bronze.player_contract,
    bronze.player,
    bronze.team,
    bronze.season,
    bronze.venue,
    bronze.competition
CASCADE;
"""


def _ensure_schemas_sql_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / "sql"
        / "ensure_warehouse_schemas.sql"
    )


def ensure_warehouse_schemas(warehouse_conn_id: str) -> None:
    """Run ``ensure_warehouse_schemas.sql`` on the warehouse.

    Creates schemas and drops FKs on ``bronze``, ``silver_stg``, and ``silver`` only
    (same policy as ``007_drop_warehouse_foreign_keys.sql``).
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    path = _ensure_schemas_sql_path()
    sql = path.read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=warehouse_conn_id)
    hook.run(sql, autocommit=True)


def _list_public_columns(*, source_conn_id: str, table: str) -> list[str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=source_conn_id)
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """
    return [row[0] for row in hook.get_records(sql, parameters=(table,))]


def _quoted_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _truncate_bronze_table(*, warehouse_conn_id: str, table: str) -> None:
    """``TRUNCATE`` only ``bronze.<table>`` (quoted identifiers)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    sql = f"TRUNCATE TABLE bronze.{_quoted_ident(table)}"
    hook = PostgresHook(postgres_conn_id=warehouse_conn_id)
    hook.run(sql, autocommit=True)


def run_bronze_table_replace(
    *,
    table: str,
    source_conn_id: str,
    warehouse_conn_id: str,
    truncate_table: bool = True,
) -> int:
    """Truncate ``bronze.<table>`` (optional), then copy ``public.<table>`` → bronze.

    When ``truncate_table`` is ``False``, the caller must already have cleared
    bronze (e.g. :func:`truncate_all_bronze_tables` in one shot). Use that mode
    for parallel Airflow tasks so a single child-first ``TRUNCATE … CASCADE``
    runs once; per-table ``TRUNCATE`` fails if other bronze tables still hold
    foreign keys pointing at this table (legacy warehouse DDL).

    ``table`` must be a name in :data:`BRONZE_TABLE_LOAD_ORDER`.
    """
    if table not in BRONZE_TABLE_LOAD_ORDER:
        msg = (
            f"Unknown bronze table {table!r}; "
            f"expected one of {BRONZE_TABLE_LOAD_ORDER!r}"
        )
        raise ValueError(msg)

    if truncate_table:
        _truncate_bronze_table(warehouse_conn_id=warehouse_conn_id, table=table)
    n = _copy_one_table(
        source_conn_id=source_conn_id,
        warehouse_conn_id=warehouse_conn_id,
        table=table,
    )
    logger.info("Bronze replace: public.%s → bronze.%s (%s rows)", table, table, n)
    return n


def truncate_all_bronze_tables(*, warehouse_conn_id: str) -> None:
    """Truncate all v1 bronze tables in dependency-safe order (``CASCADE``)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    wh = PostgresHook(postgres_conn_id=warehouse_conn_id)
    wh.run(_TRUNCATE_BRONZE_SQL, autocommit=True)


def _copy_one_table(
    *,
    source_conn_id: str,
    warehouse_conn_id: str,
    table: str,
) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    columns = _list_public_columns(source_conn_id=source_conn_id, table=table)
    if not columns:
        msg = f"public.{table}: no columns found (missing table?)"
        raise ValueError(msg)

    col_list_sql = ", ".join(_quoted_ident(c) for c in columns)
    select_sql = f"SELECT {col_list_sql} FROM public.{_quoted_ident(table)}"
    insert_sql = (
        f"INSERT INTO bronze.{_quoted_ident(table)} ({col_list_sql}) "
        f"VALUES ({', '.join(['%s'] * len(columns))})"
    )

    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    warehouse_hook = PostgresHook(postgres_conn_id=warehouse_conn_id)

    with source_hook.get_conn() as src_conn, src_conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()

    if not rows:
        return 0

    with warehouse_hook.get_conn() as wh_conn, wh_conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
        wh_conn.commit()

    return len(rows)


def run_full_bronze_snapshot(*, source_conn_id: str, warehouse_conn_id: str) -> None:
    """
    Replace every bronze v1 table: one :func:`truncate_all_bronze_tables` (child-first
    ``CASCADE``), then copy each table in :data:`BRONZE_TABLE_LOAD_ORDER` without
    per-table ``TRUNCATE``. Safe when warehouse bronze still has foreign keys.

    Small-table POC: loads full result sets into memory per table.
    """
    truncate_all_bronze_tables(warehouse_conn_id=warehouse_conn_id)
    for table in BRONZE_TABLE_LOAD_ORDER:
        run_bronze_table_replace(
            table=table,
            source_conn_id=source_conn_id,
            warehouse_conn_id=warehouse_conn_id,
            truncate_table=False,
        )
