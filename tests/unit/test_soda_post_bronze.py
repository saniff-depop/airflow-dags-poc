from pathlib import Path

import pytest
from ingestion.football_ingest.include.soda_runner import (
    assert_source_bronze_row_parity,
    assert_source_bronze_row_parity_one_table,
)


def test_assert_source_bronze_row_parity__passes_when_counts_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_count(*, pg_uri: str, schema: str, table: str) -> int:
        _ = (pg_uri, schema, table)
        return 5

    monkeypatch.setattr(
        "ingestion.football_ingest.include.soda_runner._count_rows_via_psycopg",
        fake_count,
    )
    assert_source_bronze_row_parity(
        source_pg_uri="postgresql://x",
        warehouse_pg_uri="postgresql://y",
        tables=("competition", "venue"),
    )


def test_assert_source_bronze_row_parity__raises_when_counts_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_count(*, pg_uri: str, schema: str, table: str) -> int:
        if table == "venue":
            return 1 if schema == "public" else 2
        return 10

    monkeypatch.setattr(
        "ingestion.football_ingest.include.soda_runner._count_rows_via_psycopg",
        fake_count,
    )
    with pytest.raises(RuntimeError, match="Row count mismatch for venue"):
        assert_source_bronze_row_parity(
            source_pg_uri="postgresql://x",
            warehouse_pg_uri="postgresql://y",
            tables=("competition", "venue"),
        )


def test_assert_source_bronze_row_parity_one_table__passes_when_counts_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_count(*, pg_uri: str, schema: str, table: str) -> int:
        _ = (pg_uri, schema, table)
        return 3

    monkeypatch.setattr(
        "ingestion.football_ingest.include.soda_runner._count_rows_via_psycopg",
        fake_count,
    )
    assert_source_bronze_row_parity_one_table(
        source_pg_uri="postgresql://x",
        warehouse_pg_uri="postgresql://y",
        table="fixture",
    )


def test_assert_source_bronze_row_parity_one_table__raises_when_counts_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_count(*, pg_uri: str, schema: str, table: str) -> int:
        if schema == "public":
            return 2
        return 5

    monkeypatch.setattr(
        "ingestion.football_ingest.include.soda_runner._count_rows_via_psycopg",
        fake_count,
    )
    with pytest.raises(RuntimeError, match="Row count mismatch for player"):
        assert_source_bronze_row_parity_one_table(
            source_pg_uri="postgresql://x",
            warehouse_pg_uri="postgresql://y",
            table="player",
        )


def test_assert_source_bronze_row_parity_one_table__taskflow_uses_hook_not_psycopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_hook_count(*, postgres_conn_id: str, schema: str, table: str) -> int:
        _ = postgres_conn_id
        return 4 if schema == "public" else 4

    monkeypatch.setattr(
        "ingestion.football_ingest.include.soda_runner._count_rows_via_hook",
        fake_hook_count,
    )
    assert_source_bronze_row_parity_one_table(
        source_conn_id="football_pg_source",
        warehouse_conn_id="football_pg_warehouse",
        table="venue",
    )


def test_assert_source_bronze_row_parity_one_table__rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        assert_source_bronze_row_parity_one_table(
            source_conn_id="a",
            warehouse_conn_id="b",
            table="not_a_bronze_table",
        )


def test_assert_source_bronze_row_parity_one_table__rejects_ambiguous_connection_args() -> (
    None
):
    with pytest.raises(ValueError, match="exactly one"):
        assert_source_bronze_row_parity_one_table(
            source_conn_id="a",
            warehouse_conn_id="b",
            source_pg_uri="postgresql://x",
            warehouse_pg_uri="postgresql://y",
            table="venue",
        )


def test_soda_v3_compose_configuration_and_spike_assets__exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = repo_root / "soda/configuration.compose.yml"
    spike = repo_root / "soda/checks/spike/minimal_cross_check_example.yml"
    assert cfg.is_file()
    assert spike.is_file()
