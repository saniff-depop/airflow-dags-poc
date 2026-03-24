"""Structural checks for :mod:`ingestion.football_ingest.football_ingest_dag` task ids."""

from __future__ import annotations

from ingestion.football_ingest.football_ingest_dag import dag as football_ingest_dag
from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER
from ingestion.football_ingest.include.silver_stg_from_bronze import (
    SILVER_STG_TABLE_LOAD_ORDER,
)


def test_football_ingest_dag__bronze_phase__exposes_expected_task_ids() -> None:
    task_ids = set(football_ingest_dag.task_dict.keys())

    assert "ensure_schemas" in task_ids
    assert "bronze_done" in task_ids
    assert "truncate_bronze" not in task_ids
    assert not any("bronze_fan_out" in tid for tid in task_ids)
    assert not any("soda_post_bronze" in tid for tid in task_ids)

    for table in BRONZE_TABLE_LOAD_ORDER:
        prefix = f"load_bronze_tables.{table}"
        assert f"{prefix}.pre_checks" in task_ids
        assert f"{prefix}.truncate_{table}" in task_ids
        assert f"{prefix}.load_{table}" in task_ids
        assert f"{prefix}.post_checks" in task_ids


def test_football_ingest_dag__silver_stg_phase__exposes_expected_task_ids() -> None:
    task_ids = set(football_ingest_dag.task_dict.keys())

    assert "silver_stg_done" in task_ids
    assert "silver_stg_fan_out" not in task_ids
    for table in SILVER_STG_TABLE_LOAD_ORDER:
        prefix = f"load_silver_stg_tables.{table}"
        assert f"{prefix}.load_{table}" in task_ids
        assert f"{prefix}.post_checks" in task_ids


def test_football_ingest_dag__publish_silver_phase__exposes_expected_task_ids() -> None:
    task_ids = set(football_ingest_dag.task_dict.keys())

    assert "publish_silver" not in task_ids
    assert "silver_done" in task_ids
    for table in BRONZE_TABLE_LOAD_ORDER:
        prefix = f"publish_silver_tables.{table}"
        assert f"{prefix}.truncate_{table}" in task_ids
        assert f"{prefix}.load_{table}" in task_ids
        assert f"{prefix}.post_checks" in task_ids
