"""Tests for per-table silver_stg load (:func:`run_silver_stg_table_replace`)."""

from __future__ import annotations

import pytest
from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER
from ingestion.football_ingest.include.silver_stg_from_bronze import (
    run_silver_stg_snapshot_from_bronze,
    run_silver_stg_table_replace,
)


class _FakeCursor:
    def __init__(self) -> None:
        self.executes: list[tuple[str, tuple[object, ...] | None]] = []
        self._rowcount = 4

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.executes.append((sql, params))

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj


class _RecordingConn:
    def __init__(self) -> None:
        self.autocommit = False
        self.committed = False
        self.closed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def test_run_silver_stg_table_replace__unknown_table_raises() -> None:
    with pytest.raises(ValueError, match="Unknown silver_stg table"):
        run_silver_stg_table_replace(
            table="nope",
            warehouse_conn_id="wh",
            batch_id="run-1",
            conn=_FakeConn(),
        )


def test_run_silver_stg_table_replace__truncate_then_insert_on_shared_conn() -> None:
    conn = _FakeConn()
    n = run_silver_stg_table_replace(
        table="competition",
        warehouse_conn_id="wh",
        batch_id="manual__abc",
        conn=conn,
    )
    assert n == 4
    assert len(conn.cursor_obj.executes) == 2
    trunc_sql, trunc_params = conn.cursor_obj.executes[0]
    assert 'TRUNCATE TABLE silver_stg."competition"' in trunc_sql
    assert trunc_params is None
    insert_sql, insert_params = conn.cursor_obj.executes[1]
    assert "INSERT INTO silver_stg.competition" in insert_sql
    assert insert_params == ("manual__abc",)


def test_run_silver_stg_snapshot_from_bronze__delegates_per_table_in_one_tx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = _RecordingConn()
    tables_seen: list[str] = []

    def _fake_replace(
        *,
        table: str,
        warehouse_conn_id: str,
        batch_id: str,
        conn: object | None,
    ) -> int:
        tables_seen.append(table)
        assert warehouse_conn_id == "wh"
        assert batch_id == "rid"
        assert conn is shared
        return 0

    class _FakeHook:
        def __init__(self, postgres_conn_id: str) -> None:
            self.postgres_conn_id = postgres_conn_id

        def get_conn(self) -> _RecordingConn:
            return shared

    monkeypatch.setattr(
        "ingestion.football_ingest.include.silver_stg_from_bronze.run_silver_stg_table_replace",
        _fake_replace,
    )
    monkeypatch.setattr(
        "ingestion.football_ingest.include.silver_stg_from_bronze._postgres_hook_class",
        lambda: _FakeHook,
    )

    run_silver_stg_snapshot_from_bronze(warehouse_conn_id="wh", batch_id="rid")

    assert tables_seen == list(BRONZE_TABLE_LOAD_ORDER)
    assert shared.committed
    assert shared.closed
