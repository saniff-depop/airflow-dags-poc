-- Remove FOREIGN KEY constraints from warehouse medallion layers (bronze, silver_stg, silver).
-- Policy: medallion schemas only — not audit or other app schemas (parallel publish / loads).
-- Source database (public.*) keeps FKs; see 001_source_public.sql.
-- Run once on existing DBs, or rely on dags/football_ingest/scripts/sql/ensure_warehouse_schemas.sql
-- (first DAG task) which applies the same logic every run.
-- Fresh installs from current 003/004/005: typically no FK rows to drop.

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.conname AS constraint_name,
               n.nspname AS schema_name,
               t.relname AS table_name
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE c.contype = 'f'
          AND n.nspname IN ('bronze', 'silver_stg', 'silver')
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I DROP CONSTRAINT %I',
            r.schema_name,
            r.table_name,
            r.constraint_name
        );
    END LOOP;
END $$;
