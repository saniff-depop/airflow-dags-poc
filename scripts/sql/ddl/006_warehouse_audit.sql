-- Optional audit trail for DAG / Soda outcomes (phase 10).

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id TEXT NOT NULL,
    dag_run_id TEXT NOT NULL,
    logical_date TIMESTAMPTZ,
    check_suite TEXT,
    outcome_json JSONB,
    row_counts_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
