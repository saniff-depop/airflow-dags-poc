"""Tests for ``soda_contract_template`` (Jinja → SodaCL YAML)."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pytest
from ingestion.football_ingest.include.soda_contract_template import (
    render_bronze_contract_yaml,
    render_bronze_source_prechecks_yaml,
    render_contract_yaml_for_layer,
    render_silver_stg_contract_yaml,
    silver_stg_pre_silver_template_context,
    write_rendered_contract_to_tempfile,
)
from jinja2.exceptions import UndefinedError
from ruamel.yaml import YAML


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _soda_root() -> Path:
    return _repo_root() / "soda"


def _assert_parses_as_sodacl(*, text: str, expected_checks_for: str) -> None:
    yaml = YAML(typ="safe")
    data = yaml.load(StringIO(text))
    assert isinstance(data, dict)
    assert expected_checks_for in data
    assert isinstance(data[expected_checks_for], list)
    assert data[expected_checks_for]


def test_render_bronze_contract_yaml__includes_checks_for_and_parses() -> None:
    out = render_bronze_contract_yaml(table="competition", soda_root=_soda_root())
    assert "checks for competition:" in out
    assert "row_count same as competition in football_source" in out
    assert "missing_count(competition_id)" in out
    assert "when required column missing" not in out
    _assert_parses_as_sodacl(
        text=out,
        expected_checks_for="checks for competition",
    )


def test_render_bronze_source_prechecks_yaml__includes_schema_and_parses() -> None:
    out = render_bronze_source_prechecks_yaml(table="venue", soda_root=_soda_root())
    assert "checks for venue:" in out
    assert "when required column missing" in out
    assert "row_count > 0" in out
    _assert_parses_as_sodacl(
        text=out,
        expected_checks_for="checks for venue",
    )


def test_render_bronze_source_prechecks_yaml__fixture_includes_row_count() -> None:
    out = render_bronze_source_prechecks_yaml(table="fixture", soda_root=_soda_root())
    assert "checks for fixture:" in out
    assert "Source fixture is not empty" in out
    assert "row_count > 0" in out
    assert "freshness(kickoff_utc)" in out
    _assert_parses_as_sodacl(
        text=out,
        expected_checks_for="checks for fixture",
    )


def test_render_silver_stg_contract_yaml__includes_checks_for_and_parses() -> None:
    out = render_silver_stg_contract_yaml(table="player", soda_root=_soda_root())
    assert "checks for player:" in out
    _assert_parses_as_sodacl(
        text=out,
        expected_checks_for="checks for player",
    )


def test_render_silver_stg_pre_silver_context__player_includes_missing_and_duplicate() -> (
    None
):
    ctx = silver_stg_pre_silver_template_context(table="player")
    out = render_contract_yaml_for_layer(
        layer="silver_stg",
        context=ctx,
        soda_root=_soda_root(),
    )
    assert "given_name" in out
    assert "missing_count(given_name)" in out
    assert "failed rows:" in out
    assert "player_id" in out


def test_render_silver_stg_pre_silver_context__player_contract_includes_row_count() -> (
    None
):
    ctx = silver_stg_pre_silver_template_context(table="player_contract")
    out = render_contract_yaml_for_layer(
        layer="silver_stg",
        context=ctx,
        soda_root=_soda_root(),
    )
    assert "row_count = 300" in out
    assert "valid_from" in out


def test_render_bronze_contract_yaml__duplicate_columns_block() -> None:
    out = render_bronze_contract_yaml(
        table="player",
        soda_root=_soda_root(),
        duplicate_columns=["player_id"],
    )
    assert "failed rows:" in out
    assert "player_id" in out
    assert "GROUP BY player_id" in out


def test_render_contract_yaml_for_layer__missing_table_raises() -> None:
    with pytest.raises(UndefinedError):
        render_contract_yaml_for_layer(
            layer="bronze",
            context={
                "columns": [{"name": "x", "data_type": "TEXT", "pg_type": "text"}]
            },
            soda_root=_soda_root(),
        )


def test_write_rendered_contract_to_tempfile__writes_utf8_roundtrip() -> None:
    text = "checks for x:\n  - row_count > 0\n"
    path = write_rendered_contract_to_tempfile(text)
    try:
        assert path.suffix == ".yml"
        read_back = path.read_text(encoding="utf-8")
        assert read_back == text
    finally:
        path.unlink(missing_ok=True)
