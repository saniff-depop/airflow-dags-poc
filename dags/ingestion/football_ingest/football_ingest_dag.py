from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.task_group import TaskGroup
from common.datasets import WAREHOUSE_SILVER_FOOTBALL_DATASET

from ingestion.football_ingest.include.bronze_snapshot import (
    BRONZE_TABLE_LOAD_ORDER,
    _truncate_bronze_table,
    ensure_warehouse_schemas,
    run_bronze_table_replace,
)
from ingestion.football_ingest.include.publish_silver_from_silver_stg import (
    run_dq_silver_table_minimal,
    run_finalize_silver_publish_audit,
    run_load_silver_table_from_stg_only,
    run_truncate_one_silver_table,
)
from ingestion.football_ingest.include.silver_stg_from_bronze import (
    SILVER_STG_TABLE_LOAD_ORDER,
    run_silver_stg_table_replace,
)
from ingestion.football_ingest.include.soda_contract_template import (
    silver_stg_pre_silver_template_context,
)
from ingestion.football_ingest.include.soda_runner import (
    VENV_REQUIREMENTS_SODA_AND_JINJA,
    run_soda_bronze_source_prechecks_for_table,
    run_soda_contract_for_table,
)

POSTGRES_SOURCE_CONN_ID = "football_pg_source"
POSTGRES_WAREHOUSE_CONN_ID = "football_pg_warehouse"

SODA_POST_BRONZE_SCAN_PREFIX = "football_ingest_post_bronze"
SODA_PRE_BRONZE_SOURCE_SCAN_PREFIX = "football_ingest_bronze_source_pre"
SODA_PRE_SILVER_SCAN_PREFIX = "football_ingest_pre_silver"

default_args = {
    "owner": "football_ingest",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "email_on_failure": False,
}


def _truncate_bronze_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id=f"truncate_{table}")
    def _truncate_bronze_table_branch() -> None:
        _truncate_bronze_table(
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            table=table,
        )

    return _truncate_bronze_table_branch


def _load_bronze_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id=f"load_{table}")
    def _load_bronze_table() -> None:
        run_bronze_table_replace(
            table=table,
            source_conn_id=POSTGRES_SOURCE_CONN_ID,
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            truncate_table=False,
        )

    return _load_bronze_table


def _load_silver_stg_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id=f"load_{table}")
    def _load_silver_stg_table() -> None:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        batch_id = str(ctx["run_id"])
        run_silver_stg_table_replace(
            table=table,
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            batch_id=batch_id,
        )

    return _load_silver_stg_table


def _truncate_silver_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id=f"truncate_{table}")
    def _truncate_silver_table() -> None:
        run_truncate_one_silver_table(
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            table=table,
        )

    return _truncate_silver_table


def _load_silver_publish_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id=f"load_{table}")
    def _load_silver_table() -> None:
        run_load_silver_table_from_stg_only(
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            table=table,
        )

    return _load_silver_table


def _dq_silver_table_task_factory(table: str) -> Callable[[], Any]:
    @task(task_id="post_checks")
    def _dq_silver_table() -> None:
        run_dq_silver_table_minimal(
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            table=table,
        )

    return _dq_silver_table


@dag(
    dag_id="football_ingest",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["football", "ingest", "soda", "postgres"],
    default_args=default_args,
    doc_md=f"""
    ## football_ingest

    Source → bronze (parallel per table) → ``silver_stg`` (parallel per table) → Soda →
    silver (WAP). Implementation: **`specs/soda/`**, **`specs/dag-fan-out/`**.

    **Connections:** `{POSTGRES_SOURCE_CONN_ID}` (source),
    `{POSTGRES_WAREHOUSE_CONN_ID}` (warehouse).
    Configure in the Airflow UI; see **`dags/ingestion/football_ingest/README.md`**.

    **Schedule:** `@daily`. Produces the warehouse silver dataset
    (``postgres://postgres_warehouse:5432/warehouse_db.silver``) on success,
    triggering downstream dbt modelling.

    **Bronze (phase 08):** ``ensure_schemas`` → ``load_bronze_tables`` (nested group per
    table: ``pre_checks`` (Soda on ``football_source``: schema, freshness, non-empty) →
    ``truncate_<table>`` → ``load_<table>`` → ``post_checks`` (SodaCL incl.
    source vs bronze ``row_count same as``), all tables parallel) → ``bronze_done``.
    ``ensure_schemas`` drops warehouse FKs so parallel per-table ``TRUNCATE`` is safe.

    **Silver_stg (phase 09):** ``load_silver_stg_tables`` — per table nested group
    ``load_silver_stg_tables.<table>``: ``load_<table>`` → ``post_checks`` (SodaCL) →
    ``silver_stg_done`` → ``publish_silver_tables`` (per table: ``truncate_<t>`` →
    ``load_<t>`` →
    ``post_checks``) → ``silver_done``.
    """,
)
def football_ingest() -> None:
    @task(task_id="ensure_schemas")
    def ensure_schemas() -> None:
        ensure_warehouse_schemas(POSTGRES_WAREHOUSE_CONN_ID)

    soda_root = Path(__file__).resolve().parents[3] / "soda"
    soda_configuration_yaml = str(soda_root / "configuration.compose.yml")
    venv_reqs = list(VENV_REQUIREMENTS_SODA_AND_JINJA)

    t_ensure = ensure_schemas()
    bronze_done = EmptyOperator(task_id="bronze_done")
    silver_stg_done = EmptyOperator(task_id="silver_stg_done")

    with TaskGroup(group_id="load_bronze_tables") as load_bronze_tables:
        for table in BRONZE_TABLE_LOAD_ORDER:
            with TaskGroup(group_id=table):
                pre_checks = PythonVirtualenvOperator(
                    task_id="pre_checks",
                    python_callable=run_soda_bronze_source_prechecks_for_table,
                    requirements=venv_reqs,
                    system_site_packages=False,
                    op_kwargs={
                        "project_root": str(soda_root),
                        "table": table,
                        "configuration_yaml": soda_configuration_yaml,
                        "scan_label": (
                            f"Soda pre-bronze source ({table}) "
                            f"({SODA_PRE_BRONZE_SOURCE_SCAN_PREFIX}_{table}_v1)"
                        ),
                    },
                    env_vars={"OTEL_SDK_DISABLED": "true"},
                )
                t_trunc = _truncate_bronze_table_task_factory(table)()
                t_load = _load_bronze_table_task_factory(table)()
                post_checks = PythonVirtualenvOperator(
                    task_id="post_checks",
                    python_callable=run_soda_contract_for_table,
                    requirements=venv_reqs,
                    system_site_packages=False,
                    op_kwargs={
                        "project_root": str(soda_root),
                        "layer": "bronze",
                        "table": table,
                        "configuration_yaml": soda_configuration_yaml,
                        "scan_label": (
                            f"Soda post-bronze ({table}) "
                            f"({SODA_POST_BRONZE_SCAN_PREFIX}_{table}_v1)"
                        ),
                    },
                    env_vars={"OTEL_SDK_DISABLED": "true"},
                )
                chain(pre_checks, t_trunc, t_load)
                t_load >> post_checks

    with TaskGroup(group_id="load_silver_stg_tables") as load_silver_stg_tables:
        for table in SILVER_STG_TABLE_LOAD_ORDER:
            with TaskGroup(group_id=table):
                t_silver_stg_load = _load_silver_stg_table_task_factory(table)()
                post_checks = PythonVirtualenvOperator(
                    task_id="post_checks",
                    python_callable=run_soda_contract_for_table,
                    requirements=venv_reqs,
                    system_site_packages=False,
                    op_kwargs={
                        "project_root": str(soda_root),
                        "layer": "silver_stg",
                        "table": table,
                        "configuration_yaml": soda_configuration_yaml,
                        "scan_label": (
                            f"Soda pre-silver ({table}) "
                            f"({SODA_PRE_SILVER_SCAN_PREFIX}_{table}_v1)"
                        ),
                        "template_context": silver_stg_pre_silver_template_context(
                            table=table,
                        ),
                    },
                    env_vars={"OTEL_SDK_DISABLED": "true"},
                )
                chain(t_silver_stg_load, post_checks)

    with TaskGroup(group_id="publish_silver_tables") as publish_silver_tables:
        for table in BRONZE_TABLE_LOAD_ORDER:
            with TaskGroup(group_id=table):
                t_trunc_silver = _truncate_silver_table_task_factory(table)()
                t_load_silver = _load_silver_publish_table_task_factory(table)()
                t_dq_silver = _dq_silver_table_task_factory(table)()
                chain(t_trunc_silver, t_load_silver, t_dq_silver)

    @task(
        task_id="silver_done",
        outlets=[WAREHOUSE_SILVER_FOOTBALL_DATASET],
    )
    def silver_done() -> None:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        dag = ctx["dag"]
        dag_run = ctx["dag_run"]
        run_finalize_silver_publish_audit(
            warehouse_conn_id=POSTGRES_WAREHOUSE_CONN_ID,
            dag_id=dag.dag_id,
            dag_run_id=str(dag_run.run_id),
            logical_date=dag_run.logical_date,
        )

    t_silver_done = silver_done()
    chain(
        t_ensure,
        load_bronze_tables,
        bronze_done,
        load_silver_stg_tables,
        silver_stg_done,
        publish_silver_tables,
        t_silver_done,
    )


dag = football_ingest()
