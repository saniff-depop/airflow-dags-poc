from ingestion.football_ingest.include.bronze_snapshot import BRONZE_TABLE_LOAD_ORDER
from ingestion.football_ingest.include.silver_stg_from_bronze import (
    _SILVER_STG_INSERT_SQL,
)


def test_silver_stg_insert_sql__covers_bronze_load_order() -> None:
    assert tuple(_SILVER_STG_INSERT_SQL) == BRONZE_TABLE_LOAD_ORDER


def test_player_contract_sql__uses_distinct_on_natural_key() -> None:
    sql = _SILVER_STG_INSERT_SQL["player_contract"]
    assert "DISTINCT ON (player_id, team_id, valid_from)" in sql.replace("\n", " ")
    assert "player_contract_id DESC" in sql
