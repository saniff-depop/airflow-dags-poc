"""Bronze → ``silver_stg`` with lineage and DISTINCT ON dedupe (phase 08).

**Per-table replace** — :func:`run_silver_stg_table_replace` truncates one
``silver_stg.<table>`` then runs the matching ``INSERT … SELECT`` from bronze.
Pass a shared ``conn`` to participate in a caller-owned transaction (as
:func:`run_silver_stg_snapshot_from_bronze` does); otherwise it opens a connection,
commits, and closes.

**Full snapshot** — :func:`run_silver_stg_snapshot_from_bronze` runs all tables in
:data:`BRONZE_TABLE_LOAD_ORDER` inside **one** database transaction (no upfront
global ``TRUNCATE``).

**Reset** — :func:`truncate_all_silver_stg_tables` runs the legacy multi-table
``TRUNCATE … CASCADE`` for ops-only emptying.

``_ingested_at`` is ``NOW()`` per statement. ``_batch_id`` is typically the Airflow
``run_id``.

``bronze.player_contract`` can contain multiple rows per natural key; we keep the
row with the greatest ``player_contract_id``.

:data:`SILVER_STG_TABLE_LOAD_ORDER` lists ``silver_stg`` tables in the same order as
:data:`~ingestion.football_ingest.include.bronze_snapshot.BRONZE_TABLE_LOAD_ORDER`
(keys of :data:`_SILVER_STG_INSERT_SQL` only), for deterministic DAG fan-out.
"""

from __future__ import annotations

import logging
from typing import Final

from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER

logger = logging.getLogger(__name__)


def _postgres_hook_class() -> type:
    """Return :class:`PostgresHook` (lazy import for Airflow-free unit tests)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook


def _quoted_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


_TRUNCATE_SILVER_STG_SQL: Final[str] = """
TRUNCATE TABLE
    silver_stg.match_result,
    silver_stg.fixture,
    silver_stg.matchday_round,
    silver_stg.player_contract,
    silver_stg.player,
    silver_stg.team,
    silver_stg.season,
    silver_stg.venue,
    silver_stg.competition
CASCADE;
"""

# One ``%s`` placeholder per statement: ``_batch_id`` (Airflow ``run_id``).
_SILVER_STG_INSERT_SQL: Final[dict[str, str]] = {
    "competition": """
INSERT INTO silver_stg.competition (
    competition_id, name, country_code,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (competition_id)
    competition_id,
    TRIM(BOTH FROM name),
    country_code,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.competition
ORDER BY competition_id, competition_id DESC;
""",
    "venue": """
INSERT INTO silver_stg.venue (
    venue_id, name, city, capacity,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (venue_id)
    venue_id,
    TRIM(BOTH FROM name),
    TRIM(BOTH FROM city) AS city,
    capacity,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.venue
ORDER BY venue_id, venue_id DESC;
""",
    "season": """
INSERT INTO silver_stg.season (
    season_id, competition_id, label, start_date, end_date,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (season_id)
    season_id,
    competition_id,
    TRIM(BOTH FROM label),
    start_date,
    end_date,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.season
ORDER BY season_id, season_id DESC;
""",
    "team": """
INSERT INTO silver_stg.team (
    team_id, name, short_name, founded_year, venue_id,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (team_id)
    team_id,
    TRIM(BOTH FROM name),
    TRIM(BOTH FROM short_name),
    founded_year,
    venue_id,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.team
ORDER BY team_id, team_id DESC;
""",
    "player": """
INSERT INTO silver_stg.player (
    player_id, given_name, family_name, date_of_birth, nationality, preferred_foot,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (player_id)
    player_id,
    TRIM(BOTH FROM given_name),
    TRIM(BOTH FROM family_name),
    date_of_birth,
    CASE WHEN nationality IS NULL THEN NULL ELSE TRIM(BOTH FROM nationality) END,
    CASE WHEN preferred_foot IS NULL THEN NULL ELSE TRIM(BOTH FROM preferred_foot) END,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.player
ORDER BY player_id, player_id DESC;
""",
    "player_contract": """
INSERT INTO silver_stg.player_contract (
    player_contract_id, player_id, team_id, valid_from, valid_to,
    shirt_number, is_loan,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (player_id, team_id, valid_from)
    player_contract_id,
    player_id,
    team_id,
    valid_from,
    valid_to,
    shirt_number,
    is_loan,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.player_contract
ORDER BY player_id, team_id, valid_from, player_contract_id DESC;
""",
    "matchday_round": """
INSERT INTO silver_stg.matchday_round (
    round_id, season_id, round_number, label,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (round_id)
    round_id,
    season_id,
    round_number,
    TRIM(BOTH FROM label),
    NOW(),
    %s,
    'postgres_source'
FROM bronze.matchday_round
ORDER BY round_id, round_id DESC;
""",
    "fixture": """
INSERT INTO silver_stg.fixture (
    fixture_id, season_id, round_id, home_team_id, away_team_id, venue_id,
    kickoff_utc, status,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (fixture_id)
    fixture_id,
    season_id,
    round_id,
    home_team_id,
    away_team_id,
    venue_id,
    kickoff_utc,
    TRIM(BOTH FROM status),
    NOW(),
    %s,
    'postgres_source'
FROM bronze.fixture
ORDER BY fixture_id, fixture_id DESC;
""",
    "match_result": """
INSERT INTO silver_stg.match_result (
    fixture_id, home_goals, away_goals, home_xg, away_xg,
    _ingested_at, _batch_id, _source_system
)
SELECT DISTINCT ON (fixture_id)
    fixture_id,
    home_goals,
    away_goals,
    home_xg,
    away_xg,
    NOW(),
    %s,
    'postgres_source'
FROM bronze.match_result
ORDER BY fixture_id, fixture_id DESC;
""",
}

SILVER_STG_TABLE_LOAD_ORDER: Final[tuple[str, ...]] = tuple(
    t for t in BRONZE_TABLE_LOAD_ORDER if t in _SILVER_STG_INSERT_SQL
)


def run_silver_stg_table_replace(
    *,
    table: str,
    warehouse_conn_id: str,
    batch_id: str,
    conn: object | None = None,
) -> int:
    """Truncate ``silver_stg.<table>`` and reload from bronze for that table.

    If ``conn`` is ``None``, opens a connection from ``warehouse_conn_id``,
    commits (or rolls back on error), and closes.

    If ``conn`` is provided, uses it without committing or closing (caller owns
    the transaction). Parallel Airflow tasks should use **distinct** ``table``
    values and **separate** connections (``conn=None``).

    ``table`` must be a key in :data:`_SILVER_STG_INSERT_SQL`.
    """
    if table not in _SILVER_STG_INSERT_SQL:
        keys = tuple(_SILVER_STG_INSERT_SQL)
        msg = f"Unknown silver_stg table {table!r}; expected one of {keys!r}"
        raise ValueError(msg)

    insert_sql = _SILVER_STG_INSERT_SQL[table]
    truncate_sql = f"TRUNCATE TABLE silver_stg.{_quoted_ident(table)}"

    close_conn = False
    if conn is None:
        hook = _postgres_hook_class()(postgres_conn_id=warehouse_conn_id)
        conn = hook.get_conn()
        conn.autocommit = False
        close_conn = True

    try:
        with conn.cursor() as cur:
            cur.execute(truncate_sql)
            cur.execute(insert_sql, (batch_id,))
            n = cur.rowcount if cur.rowcount is not None else 0
        if close_conn:
            conn.commit()
        logger.info(
            "Silver_stg replace: bronze.%s → silver_stg.%s (%s rows)",
            table,
            table,
            n,
        )
        return int(n)
    except Exception:
        if close_conn:
            conn.rollback()
        raise
    finally:
        if close_conn:
            conn.close()


def truncate_all_silver_stg_tables(*, warehouse_conn_id: str) -> None:
    """Truncate all v1 ``silver_stg`` tables in one ``CASCADE`` statement."""
    hook = _postgres_hook_class()(postgres_conn_id=warehouse_conn_id)
    hook.run(_TRUNCATE_SILVER_STG_SQL, autocommit=True)


def run_silver_stg_snapshot_from_bronze(
    *,
    warehouse_conn_id: str,
    batch_id: str,
) -> None:
    """Load all ``silver_stg`` tables in one transaction (per-table TRUNCATE)."""
    missing = [t for t in BRONZE_TABLE_LOAD_ORDER if t not in _SILVER_STG_INSERT_SQL]
    if missing:
        msg = f"Missing silver_stg INSERT SQL for tables: {missing}"
        raise RuntimeError(msg)

    hook = _postgres_hook_class()(postgres_conn_id=warehouse_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    try:
        for table in SILVER_STG_TABLE_LOAD_ORDER:
            run_silver_stg_table_replace(
                table=table,
                warehouse_conn_id=warehouse_conn_id,
                batch_id=batch_id,
                conn=conn,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
