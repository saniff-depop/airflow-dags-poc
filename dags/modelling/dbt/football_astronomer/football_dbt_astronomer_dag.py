"""Gold dbt pipeline via Cosmos: seeds, snapshots, models, and tests.

Snapshot nodes are in the rendered graph and run before models that ref them
(e.g. dim_team, dim_player, dim_venue).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from common.datasets import WAREHOUSE_SILVER_FOOTBALL_DATASET
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_DIR = Path(__file__).resolve().parents[4] / "dbt" / "football_modelling"

profile_config = ProfileConfig(
    profile_name="football_modelling",
    target_name="local",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="football_pg_warehouse",
        profile_args={"schema": "gold"},
    ),
)

project_config = ProjectConfig(
    dbt_project_path=str(DBT_PROJECT_DIR),
)

render_config = RenderConfig(
    test_behavior=TestBehavior.AFTER_EACH,
    should_detach_multiple_parents_tests=True,
)

default_args = {
    "owner": "football_modelling",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="football_dbt_astronomer",
    schedule=[WAREHOUSE_SILVER_FOOTBALL_DATASET],
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["football", "dbt", "gold", "modelling", "cosmos"],
    default_args=default_args,
)
def football_dbt_astronomer() -> None:
    DbtTaskGroup(
        group_id="gold_modelling",
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        operator_args={"install_deps": True},
    )


dag = football_dbt_astronomer()
