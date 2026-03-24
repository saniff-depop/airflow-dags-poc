from unittest.mock import MagicMock, patch

import pytest
from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER
from ingestion.football_ingest.include.publish_silver_from_silver_stg import (
    _SILVER_INSERT_SQL,
    run_publish_silver_from_silver_stg,
)


def test_silver_insert_sql__covers_bronze_load_order() -> None:
    assert tuple(_SILVER_INSERT_SQL) == BRONZE_TABLE_LOAD_ORDER


def test_silver_insert_sql__selects_from_silver_stg() -> None:
    for table, sql in _SILVER_INSERT_SQL.items():
        assert f"FROM silver_stg.{table}" in sql.replace("\n", " ")


@patch(
    "ingestion.football_ingest.include.publish_silver_from_silver_stg._postgres_hook_cls",
)
def test_run_publish_silver_from_silver_stg__dq_zero_silver_stg__rolls_back(
    mock_hook_factory: MagicMock,
) -> None:
    mock_hook_cls = MagicMock()
    mock_hook_factory.return_value = mock_hook_cls
    mock_hook = MagicMock()
    mock_hook_cls.return_value = mock_hook
    mock_conn = MagicMock()
    mock_hook.get_conn.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_conn.cursor.return_value.__exit__.return_value = None
    mock_cur.rowcount = 1
    mock_cur.fetchone.return_value = (0,)

    with pytest.raises(RuntimeError, match="silver_stg"):
        run_publish_silver_from_silver_stg(
            warehouse_conn_id="football_pg_warehouse",
            dag_id="football_ingest",
            dag_run_id="run1",
            logical_date=None,
        )

    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()


@patch(
    "ingestion.football_ingest.include.publish_silver_from_silver_stg._postgres_hook_cls",
)
def test_run_publish_silver_from_silver_stg__happy_path__commits(
    mock_hook_factory: MagicMock,
) -> None:
    mock_hook_cls = MagicMock()
    mock_hook_factory.return_value = mock_hook_cls
    mock_hook = MagicMock()
    mock_hook_cls.return_value = mock_hook
    mock_conn = MagicMock()
    mock_hook.get_conn.return_value = mock_conn
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_conn.cursor.return_value.__exit__.return_value = None
    mock_cur.rowcount = 3
    n_tables = len(BRONZE_TABLE_LOAD_ORDER)
    dq_and_audit_fetches = 2 * n_tables + 2 * n_tables
    mock_cur.fetchone.side_effect = [(3,)] * dq_and_audit_fetches

    run_publish_silver_from_silver_stg(
        warehouse_conn_id="football_pg_warehouse",
        dag_id="football_ingest",
        dag_run_id="run1",
        logical_date=None,
    )

    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()
