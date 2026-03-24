"""WAP publish: silver_stg → silver in one transaction (phase 10, pattern A).

Full replace: ``TRUNCATE silver.*`` (child-first, ``CASCADE``), then
``INSERT INTO silver … SELECT … FROM silver_stg`` (parent-first).
``_batch_id`` comes from silver_stg; ``_published_at`` is ``NOW()`` in-transaction.

Per-table **minimal DQ** (same transaction): each table must have matching,
strictly positive row counts in ``silver_stg`` and ``silver`` after load.

On error, **rollback** keeps the prior silver snapshot.
``audit.pipeline_run`` is written in the **same** transaction (requires DDL ``006``).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Final, Protocol

from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER

logger = logging.getLogger(__name__)


def _postgres_hook_cls() -> type:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook


class _SilverPublishCursor(Protocol):
    rowcount: int

    def execute(self, query: str, vars: object | None = None) -> object: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


_TRUNCATE_SILVER_SQL: Final[str] = """
TRUNCATE TABLE
    silver.match_result,
    silver.fixture,
    silver.matchday_round,
    silver.player_contract,
    silver.player,
    silver.team,
    silver.season,
    silver.venue,
    silver.competition
CASCADE;
"""

_SILVER_INSERT_SQL: Final[dict[str, str]] = {
    "competition": """
INSERT INTO silver.competition (
    competition_id, name, country_code, _batch_id, _published_at
)
SELECT competition_id, name, country_code, _batch_id, NOW()
FROM silver_stg.competition;
""",
    "venue": """
INSERT INTO silver.venue (
    venue_id, name, city, capacity, _batch_id, _published_at
)
SELECT venue_id, name, city, capacity, _batch_id, NOW()
FROM silver_stg.venue;
""",
    "season": """
INSERT INTO silver.season (
    season_id, competition_id, label, start_date, end_date,
    _batch_id, _published_at
)
SELECT
    season_id, competition_id, label, start_date, end_date,
    _batch_id, NOW()
FROM silver_stg.season;
""",
    "team": """
INSERT INTO silver.team (
    team_id, name, short_name, founded_year, venue_id,
    _batch_id, _published_at
)
SELECT
    team_id, name, short_name, founded_year, venue_id,
    _batch_id, NOW()
FROM silver_stg.team;
""",
    "player": """
INSERT INTO silver.player (
    player_id, given_name, family_name, date_of_birth, nationality, preferred_foot,
    _batch_id, _published_at
)
SELECT
    player_id, given_name, family_name, date_of_birth, nationality, preferred_foot,
    _batch_id, NOW()
FROM silver_stg.player;
""",
    "player_contract": """
INSERT INTO silver.player_contract (
    player_contract_id, player_id, team_id, valid_from, valid_to,
    shirt_number, is_loan, _batch_id, _published_at
)
SELECT
    player_contract_id, player_id, team_id, valid_from, valid_to,
    shirt_number, is_loan, _batch_id, NOW()
FROM silver_stg.player_contract;
""",
    "matchday_round": """
INSERT INTO silver.matchday_round (
    round_id, season_id, round_number, label, _batch_id, _published_at
)
SELECT round_id, season_id, round_number, label, _batch_id, NOW()
FROM silver_stg.matchday_round;
""",
    "fixture": """
INSERT INTO silver.fixture (
    fixture_id, season_id, round_id, home_team_id, away_team_id, venue_id,
    kickoff_utc, status, _batch_id, _published_at
)
SELECT
    fixture_id, season_id, round_id, home_team_id, away_team_id, venue_id,
    kickoff_utc, status, _batch_id, NOW()
FROM silver_stg.fixture;
""",
    "match_result": """
INSERT INTO silver.match_result (
    fixture_id, home_goals, away_goals, home_xg, away_xg,
    _batch_id, _published_at
)
SELECT
    fixture_id, home_goals, away_goals, home_xg, away_xg,
    _batch_id, NOW()
FROM silver_stg.match_result;
""",
}


def _truncate_all_silver_tables(cur: _SilverPublishCursor) -> None:
    cur.execute(_TRUNCATE_SILVER_SQL)


def _load_silver_table_from_stg(cur: _SilverPublishCursor, table: str) -> None:
    cur.execute(_SILVER_INSERT_SQL[table])


def _assert_silver_table_minimal_dq(cur: _SilverPublishCursor, table: str) -> None:
    cur.execute(f"SELECT COUNT(*) FROM silver_stg.{table}")
    stg_row = cur.fetchone()
    stg_n = int(stg_row[0]) if stg_row else 0
    cur.execute(f"SELECT COUNT(*) FROM silver.{table}")
    sil_row = cur.fetchone()
    sil_n = int(sil_row[0]) if sil_row else 0
    if stg_n < 1:
        msg = (
            f"Silver publish DQ: silver_stg.{table} must have at least one row "
            f"(got {stg_n})"
        )
        raise RuntimeError(msg)
    if sil_n < 1:
        msg = (
            f"Silver publish DQ: silver.{table} must have at least one row "
            f"after load (got {sil_n})"
        )
        raise RuntimeError(msg)
    if stg_n != sil_n:
        msg = (
            f"Silver publish DQ: row count mismatch for {table}: "
            f"silver_stg={stg_n} silver={sil_n}"
        )
        raise RuntimeError(msg)


def _collect_row_counts_for_audit(
    cur: _SilverPublishCursor,
) -> tuple[dict[str, int], dict[str, int]]:
    silver_stg_counts: dict[str, int] = {}
    silver_counts: dict[str, int] = {}
    for table in BRONZE_TABLE_LOAD_ORDER:
        cur.execute(f"SELECT COUNT(*) FROM silver_stg.{table}")
        row = cur.fetchone()
        silver_stg_counts[table] = int(row[0]) if row else 0
        cur.execute(f"SELECT COUNT(*) FROM silver.{table}")
        row = cur.fetchone()
        silver_counts[table] = int(row[0]) if row else 0
    return silver_stg_counts, silver_counts


def _insert_pipeline_run_audit_row(
    cur: _SilverPublishCursor,
    *,
    dag_id: str,
    dag_run_id: str,
    logical_date: datetime | None,
    check_suite: str,
    outcome: dict[str, str | bool],
    row_payload: dict[str, dict[str, int]],
) -> None:
    cur.execute(
        """
        INSERT INTO audit.pipeline_run (
            dag_id,
            dag_run_id,
            logical_date,
            check_suite,
            outcome_json,
            row_counts_json
        )
        VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
        """,
        (
            dag_id,
            dag_run_id,
            logical_date,
            check_suite,
            json.dumps(outcome),
            json.dumps(row_payload),
        ),
    )


def run_publish_silver_from_silver_stg(
    *,
    warehouse_conn_id: str,
    dag_id: str,
    dag_run_id: str,
    logical_date: datetime | None,
) -> None:
    """Truncate silver, copy from silver_stg, DQ per table, audit — one transaction."""
    missing = [t for t in BRONZE_TABLE_LOAD_ORDER if t not in _SILVER_INSERT_SQL]
    if missing:
        msg = f"Missing silver INSERT SQL for tables: {missing}"
        raise RuntimeError(msg)

    hook = _postgres_hook_cls()(postgres_conn_id=warehouse_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            _truncate_all_silver_tables(cur)
            for table in BRONZE_TABLE_LOAD_ORDER:
                _load_silver_table_from_stg(cur, table)
                logger.info(
                    "Silver publish: silver_stg.%s → silver.%s (%s rows)",
                    table,
                    table,
                    cur.rowcount,
                )
                _assert_silver_table_minimal_dq(cur, table)

            silver_stg_counts, silver_counts = _collect_row_counts_for_audit(cur)

            outcome: dict[str, str | bool] = {
                "status": "success",
                "publish_pattern": "full_replace_truncate_insert",
            }
            row_payload = {
                "silver_stg": silver_stg_counts,
                "silver": silver_counts,
            }
            _insert_pipeline_run_audit_row(
                cur,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                logical_date=logical_date,
                check_suite="football_ingest_pre_silver_gate_then_publish",
                outcome=outcome,
                row_payload=row_payload,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _quoted_silver_table_ident(table: str) -> str:
    if table not in BRONZE_TABLE_LOAD_ORDER:
        msg = (
            f"Unknown silver table {table!r}; "
            f"expected one of {BRONZE_TABLE_LOAD_ORDER!r}"
        )
        raise ValueError(msg)
    escaped = table.replace('"', '""')
    return f'"{escaped}"'


def run_truncate_one_silver_table(*, warehouse_conn_id: str, table: str) -> None:
    """``TRUNCATE`` only ``silver.<table>`` (one autocommit statement)."""
    sql = f"TRUNCATE TABLE silver.{_quoted_silver_table_ident(table)}"
    hook = _postgres_hook_cls()(postgres_conn_id=warehouse_conn_id)
    hook.run(sql, autocommit=True)


def run_load_silver_table_from_stg_only(*, warehouse_conn_id: str, table: str) -> None:
    """``INSERT INTO silver.<t> SELECT … FROM silver_stg.<t>`` (one autocommit)."""
    if table not in _SILVER_INSERT_SQL:
        msg = f"No silver INSERT SQL for table {table!r}"
        raise ValueError(msg)
    hook = _postgres_hook_cls()(postgres_conn_id=warehouse_conn_id)
    hook.run(_SILVER_INSERT_SQL[table], autocommit=True)
    logger.info("Silver per-task load: silver_stg.%s → silver.%s", table, table)


def run_dq_silver_table_minimal(*, warehouse_conn_id: str, table: str) -> None:
    """Row-count parity and strictly positive counts for one table (read-only)."""
    hook = _postgres_hook_cls()(postgres_conn_id=warehouse_conn_id)
    conn = hook.get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _assert_silver_table_minimal_dq(cur, table)
    finally:
        conn.close()


FINALIZE_SILVER_PUBLISH_CHECK_SUITE: Final[str] = (
    "football_ingest_pre_silver_gate_per_table_publish_finalize"
)


def run_finalize_silver_publish_audit(
    *,
    warehouse_conn_id: str,
    dag_id: str,
    dag_run_id: str,
    logical_date: datetime | None,
) -> None:
    """Collect row counts and insert ``audit.pipeline_run`` (single transaction)."""
    hook = _postgres_hook_cls()(postgres_conn_id=warehouse_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            silver_stg_counts, silver_counts = _collect_row_counts_for_audit(cur)
            outcome: dict[str, str | bool] = {
                "status": "success",
                "publish_pattern": "per_table_parallel_truncate_load_dq",
            }
            row_payload = {
                "silver_stg": silver_stg_counts,
                "silver": silver_counts,
            }
            _insert_pipeline_run_audit_row(
                cur,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                logical_date=logical_date,
                check_suite=FINALIZE_SILVER_PUBLISH_CHECK_SUITE,
                outcome=outcome,
                row_payload=row_payload,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
