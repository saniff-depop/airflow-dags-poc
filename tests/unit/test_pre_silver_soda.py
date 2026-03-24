from pathlib import Path

import pytest
from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER
from ingestion.football_ingest.include.silver_stg_from_bronze import (
    _SILVER_STG_INSERT_SQL,
    SILVER_STG_TABLE_LOAD_ORDER,
)
from ingestion.football_ingest.include.soda_contract_template import (
    render_contract_yaml_for_layer,
    silver_stg_pre_silver_template_context,
)


def test_silver_stg_table_load_order__deterministic_bronze_subset() -> None:
    sql_keys = frozenset(_SILVER_STG_INSERT_SQL)
    expected = tuple(t for t in BRONZE_TABLE_LOAD_ORDER if t in sql_keys)
    assert expected == SILVER_STG_TABLE_LOAD_ORDER
    assert frozenset(SILVER_STG_TABLE_LOAD_ORDER) == sql_keys


def test_silver_stg_pre_silver_template_context__player_contract() -> None:
    ctx = silver_stg_pre_silver_template_context(table="player_contract")
    assert ctx["duplicate_columns"] == ["player_id", "team_id", "valid_from"]
    assert ctx["row_count_must_be"] == 300


def test_silver_stg_pre_silver_template_context__player() -> None:
    ctx = silver_stg_pre_silver_template_context(table="player")
    assert ctx["duplicate_columns"] == ["player_id"]
    assert ctx["missing_column_names"] == ["given_name", "family_name"]
    assert any(c["name"] == "given_name" for c in ctx["columns"])


def test_silver_stg_pre_silver_template_context__fixture_schema_only() -> None:
    ctx = silver_stg_pre_silver_template_context(table="fixture")
    assert ctx["table"] == "fixture"
    assert "columns" in ctx
    assert any(c["name"] == "fixture_id" for c in ctx["columns"])


@pytest.mark.parametrize("table", SILVER_STG_TABLE_LOAD_ORDER)
def test_silver_stg_pre_silver_rendered_yaml__parses_for_each_table(table: str) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    soda_root = repo_root / "soda"
    ctx = silver_stg_pre_silver_template_context(table=table)
    out = render_contract_yaml_for_layer(
        layer="silver_stg",
        context=ctx,
        soda_root=soda_root,
    )
    assert f"checks for {table}:" in out
    assert "- schema:" in out


def test_legacy_pre_silver_hand_yaml__kept_as_reference() -> None:
    """Hand-written YAML under ``checks/pre_silver/`` is not used by the DAG (Jinja)."""
    repo_root = Path(__file__).resolve().parents[2]
    base = repo_root / "soda" / "checks" / "pre_silver"
    for name in (
        "silver_stg_player_contract.yml",
        "silver_stg_player.yml",
        "silver_stg_fixture.yml",
    ):
        assert (base / name).is_file(), f"missing reference {name}"


def test_silver_stg_fixture_yml__reference_contract__has_no_failed_rows_block() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    text = (repo_root / "soda/checks/pre_silver/silver_stg_fixture.yml").read_text(
        encoding="utf-8",
    )
    assert "failed_rows:" not in text
