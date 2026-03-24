"""Build Postgres connection URIs from Airflow Connections (worker runtime only)."""

from __future__ import annotations

from urllib.parse import quote_plus


def postgres_connection_uri(*, conn_id: str) -> str:
    """Return a libpq/psycopg URI for the given Airflow Postgres connection id."""
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    user = quote_plus(conn.login or "")
    password = quote_plus(conn.password or "")
    host = conn.host or "localhost"
    port = conn.port or 5432
    database = quote_plus(conn.schema or "")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
