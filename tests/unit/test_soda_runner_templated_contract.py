"""Tests for SodaCL scan entrypoints (mocked Soda Core 3 ``Scan``)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from ingestion.football_ingest.include.soda_contract_template import (
    BRONZE_SOURCE_MATCH_RESULT_PROXY_FRESHNESS_MAX_AGE,
)
from ingestion.football_ingest.include.soda_runner import (
    SOURCE_OLTP_DATA_SOURCE,
    WAREHOUSE_BRONZE_DATA_SOURCE,
    WAREHOUSE_SILVER_STG_DATA_SOURCE,
    _run_soda_bronze_source_prechecks_impl,
    _run_soda_contract_for_table_impl,
    verify_sodacl_checks_at_path,
)


def _soda_root() -> Path:
    return Path(__file__).resolve().parents[2] / "soda"


def _fake_scan_instance() -> MagicMock:
    s = MagicMock()
    s.has_check_fails.return_value = False
    s.has_error_or_warning_logs.return_value = False
    s.get_logs_text.return_value = ""
    s.get_error_or_warning_logs_text.return_value = ""
    return s


@patch("soda.scan.Scan")
def test_verify_sodacl_checks_at_path__configures_scan_and_executes(
    mock_scan_cls: MagicMock,
) -> None:
    instance = _fake_scan_instance()
    mock_scan_cls.return_value = instance
    verify_sodacl_checks_at_path(
        configuration_yaml="/opt/airflow/soda/configuration.compose.yml",
        checks_file_path="/tmp/checks.yml",
        scan_label="unit",
    )
    mock_scan_cls.assert_called_once()
    instance.set_scan_definition_name.assert_called_once_with("unit")
    instance.add_configuration_yaml_file.assert_called_once_with(
        "/opt/airflow/soda/configuration.compose.yml",
    )
    instance.set_data_source_name.assert_called_once_with(WAREHOUSE_BRONZE_DATA_SOURCE)
    instance.add_sodacl_yaml_file.assert_called_once_with("/tmp/checks.yml")
    instance.execute.assert_called_once()


@patch("soda.scan.Scan")
def test_verify_sodacl_checks_at_path__with_variables__calls_add_variables(
    mock_scan_cls: MagicMock,
) -> None:
    instance = _fake_scan_instance()
    mock_scan_cls.return_value = instance
    verify_sodacl_checks_at_path(
        configuration_yaml="/opt/airflow/soda/configuration.compose.yml",
        checks_file_path="/tmp/checks.yml",
        scan_label="unit",
        variables={"NOW": "2024-08-20T00:00:00+00:00"},
    )
    instance.add_variables.assert_called_once_with({"NOW": "2024-08-20T00:00:00+00:00"})


@patch("soda.scan.Scan")
def test_run_soda_contract_for_table_impl__passes_rendered_yaml_path(
    mock_scan_cls: MagicMock,
) -> None:
    instance = _fake_scan_instance()
    mock_scan_cls.return_value = instance
    captured: dict[str, str] = {}

    def _read_checks_yaml(path: str) -> None:
        captured["yaml"] = Path(path).read_text(encoding="utf-8")

    instance.add_sodacl_yaml_file.side_effect = _read_checks_yaml
    soda_root = _soda_root()

    _run_soda_contract_for_table_impl(
        project_root=str(soda_root),
        layer="bronze",
        table="competition",
        configuration_yaml=str(soda_root / "configuration.compose.yml"),
        scan_label="Soda test (competition)",
        template_context=None,
        duplicate_columns=None,
    )

    instance.execute.assert_called_once()
    text = captured["yaml"]
    assert "checks for competition:" in text
    assert "row_count same as competition in football_source" in text
    assert "missing_count(competition_id)" in text
    assert "when required column missing" not in text
    instance.set_data_source_name.assert_called_once_with(WAREHOUSE_BRONZE_DATA_SOURCE)


@patch("soda.scan.Scan")
def test_run_soda_contract_for_table_impl__silver_stg_sets_silver_warehouse_data_source(
    mock_scan_cls: MagicMock,
) -> None:
    instance = _fake_scan_instance()
    mock_scan_cls.return_value = instance
    soda_root = _soda_root()

    _run_soda_contract_for_table_impl(
        project_root=str(soda_root),
        layer="silver_stg",
        table="competition",
        configuration_yaml=str(soda_root / "configuration.compose.yml"),
        scan_label="Soda test silver_stg (competition)",
        template_context=None,
        duplicate_columns=None,
    )

    instance.set_data_source_name.assert_called_once_with(
        WAREHOUSE_SILVER_STG_DATA_SOURCE
    )


@patch("soda.scan.Scan")
def test_run_soda_bronze_source_prechecks_impl__match_result__uses_modern_freshness_syntax(
    mock_scan_cls: MagicMock,
) -> None:
    instance = _fake_scan_instance()
    mock_scan_cls.return_value = instance
    captured: dict[str, str] = {}

    def _read_checks_yaml(path: str) -> None:
        captured["yaml"] = Path(path).read_text(encoding="utf-8")

    instance.add_sodacl_yaml_file.side_effect = _read_checks_yaml
    soda_root = _soda_root()

    _run_soda_bronze_source_prechecks_impl(
        project_root=str(soda_root),
        table="match_result",
        configuration_yaml=str(soda_root / "configuration.compose.yml"),
        scan_label="Soda test pre-bronze (match_result)",
    )

    instance.execute.assert_called_once()
    instance.add_variables.assert_not_called()
    instance.set_data_source_name.assert_called_once_with(SOURCE_OLTP_DATA_SOURCE)
    text = captured["yaml"]
    assert "checks for fixture:" in text
    assert (
        f"freshness(kickoff_utc) < {BRONZE_SOURCE_MATCH_RESULT_PROXY_FRESHNESS_MAX_AGE}"
        in text
    )
    assert "checks for match_result:" in text
    assert "when required column missing" in text
    assert "fixture_id" in text
