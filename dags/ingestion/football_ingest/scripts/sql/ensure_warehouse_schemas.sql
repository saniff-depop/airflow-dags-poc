-- Idempotent warehouse schemas for medallion pipeline (run on postgres_warehouse only).
-- Warehouse medallion layers intentionally have NO foreign keys (parallel bronze /
-- silver_stg / silver loads). This file creates schemas and drops any legacy FOREIGN KEY
-- constraints on bronze / silver_stg / silver only (not audit or other schemas).

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver_stg;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS audit;

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
