from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = str(
    Path(__file__).resolve().parents[4] / "dbt" / "football_modelling"
)
DBT_PROFILES_DIR = DBT_PROJECT_DIR

default_args = {
    "owner": "football_modelling",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="football_dbt_simple",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["football", "dbt", "gold", "modelling"],
    default_args=default_args,
)
def football_dbt_simple() -> None:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}"
            f" && dbt deps --profiles-dir {DBT_PROFILES_DIR}"
            f" && dbt seed --profiles-dir {DBT_PROFILES_DIR} --target local"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}"
            f" && dbt deps --profiles-dir {DBT_PROFILES_DIR}"
            f" && dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --target local"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}"
            f" && dbt deps --profiles-dir {DBT_PROFILES_DIR}"
            f" && dbt run --profiles-dir {DBT_PROFILES_DIR} --target local"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}"
            f" && dbt test --profiles-dir {DBT_PROFILES_DIR} --target local"
        ),
    )

    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test


dag = football_dbt_simple()
