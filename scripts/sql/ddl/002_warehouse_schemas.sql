-- Warehouse medallion + audit schemas (postgres_warehouse).
-- Apply after source DDL if you create cross-db links later; order is independent for isolated DBs.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver_stg;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS audit;
