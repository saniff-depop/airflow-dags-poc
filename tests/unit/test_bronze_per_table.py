"""Tests for per-table bronze load (:func:`run_bronze_table_replace`)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from ingestion.football_ingest.include.bronze_snapshot import (
    BRONZE_TABLE_LOAD_ORDER,
    run_bronze_table_replace,
    run_full_bronze_snapshot,
)


def test_run_bronze_table_replace__unknown_table_raises() -> None:
    with pytest.raises(ValueError, match="Unknown bronze table"):
        run_bronze_table_replace(
            table="not_a_real_table",
            source_conn_id="src",
            warehouse_conn_id="wh",
        )


def test_run_bronze_table_replace__truncate_then_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, str]]] = []

    def _fake_truncate(*, warehouse_conn_id: str, table: str) -> None:
        calls.append(
            ("truncate", {"warehouse_conn_id": warehouse_conn_id, "table": table}),
        )

    def _fake_copy(
        *,
        source_conn_id: str,
        warehouse_conn_id: str,
        table: str,
    ) -> int:
        calls.append(
            (
                "copy",
                {
                    "source_conn_id": source_conn_id,
                    "warehouse_conn_id": warehouse_conn_id,
                    "table": table,
                },
            ),
        )
        return 7

    monkeypatch.setattr(
        "ingestion.football_ingest.include.bronze_snapshot._truncate_bronze_table",
        _fake_truncate,
    )
    monkeypatch.setattr(
        "ingestion.football_ingest.include.bronze_snapshot._copy_one_table",
        _fake_copy,
    )

    n = run_bronze_table_replace(
        table="competition",
        source_conn_id="src",
        warehouse_conn_id="wh",
    )

    assert n == 7
    assert [c[0] for c in calls] == ["truncate", "copy"]
    assert calls[0][1] == {"warehouse_conn_id": "wh", "table": "competition"}
    assert calls[1][1]["table"] == "competition"


def test_run_bronze_table_replace__skip_truncate_when_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truncates: list[str] = []

    def _fake_truncate(*, warehouse_conn_id: str, table: str) -> None:
        truncates.append(table)

    def _fake_copy(
        *,
        source_conn_id: str,
        warehouse_conn_id: str,
        table: str,
    ) -> int:
        return 1

    monkeypatch.setattr(
        "ingestion.football_ingest.include.bronze_snapshot._truncate_bronze_table",
        _fake_truncate,
    )
    monkeypatch.setattr(
        "ingestion.football_ingest.include.bronze_snapshot._copy_one_table",
        _fake_copy,
    )

    run_bronze_table_replace(
        table="venue",
        source_conn_id="src",
        warehouse_conn_id="wh",
        truncate_table=False,
    )
    assert truncates == []


@patch("ingestion.football_ingest.include.bronze_snapshot.run_bronze_table_replace")
@patch("ingestion.football_ingest.include.bronze_snapshot.truncate_all_bronze_tables")
def test_run_full_bronze_snapshot__truncates_once_then_copies(
    mock_truncate_all: MagicMock,
    mock_replace: MagicMock,
) -> None:
    run_full_bronze_snapshot(source_conn_id="src", warehouse_conn_id="wh")
    mock_truncate_all.assert_called_once_with(warehouse_conn_id="wh")
    assert mock_replace.call_count == len(BRONZE_TABLE_LOAD_ORDER)
    seen = {c.kwargs["table"] for c in mock_replace.call_args_list}
    assert seen == set(BRONZE_TABLE_LOAD_ORDER)
    for c in mock_replace.call_args_list:
        assert c.kwargs["truncate_table"] is False
