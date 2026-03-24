from __future__ import annotations

from airflow.datasets import Dataset

WAREHOUSE_SILVER_FOOTBALL_DATASET = Dataset(
    "postgres://postgres_warehouse:5432/warehouse_db.silver",
)
