#!/usr/bin/env bash
# Initialize source and warehouse databases with DDL and seed data
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_COMPOSE_CONTEXT="${REPO_ROOT}"

COMPOSE="docker compose --env-file ${REPO_ROOT}/docker/.env -f ${REPO_ROOT}/docker/docker-compose.yaml --project-directory ${REPO_ROOT}"

echo "🔧 Initializing databases..."

# Wait for postgres_source to be healthy
echo "⏳ Waiting for postgres_source to be ready..."
for i in {1..30}; do
    if ${COMPOSE} exec -T postgres_source pg_isready -U source_user -d source_db &>/dev/null; then
        echo "✓ postgres_source is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ postgres_source failed to become ready"
        exit 1
    fi
    sleep 1
done

# Wait for postgres_warehouse to be healthy
echo "⏳ Waiting for postgres_warehouse to be ready..."
for i in {1..30}; do
    if ${COMPOSE} exec -T postgres_warehouse pg_isready -U warehouse_user -d warehouse_db &>/dev/null; then
        echo "✓ postgres_warehouse is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ postgres_warehouse failed to become ready"
        exit 1
    fi
    sleep 1
done

echo ""
echo "📊 Creating source database schema..."
${COMPOSE} exec -T postgres_source psql -U source_user -d source_db < "${REPO_ROOT}/scripts/sql/ddl/001_source_public.sql"
echo "✓ Source schema created"

echo ""
echo "📦 Seeding source database with Premier League data..."
for f in "${REPO_ROOT}"/scripts/sql/seed/*.sql; do
    echo "  Executing $(basename "$f")..."
    ${COMPOSE} exec -T postgres_source psql -U source_user -d source_db < "$f"
done
echo "✓ Source database seeded"

echo ""
echo "🏗️  Creating warehouse database schemas and tables..."
for f in "${REPO_ROOT}"/scripts/sql/ddl/{002_warehouse_schemas,003_warehouse_bronze_tables,004_warehouse_silver_stg_tables,005_warehouse_silver_tables,006_warehouse_audit,007_drop_warehouse_foreign_keys}.sql; do
    echo "  Executing $(basename "$f")..."
    ${COMPOSE} exec -T postgres_warehouse psql -U warehouse_user -d warehouse_db < "$f"
done
echo "✓ Warehouse database initialized"

echo ""
echo "📋 Verifying data counts..."
${COMPOSE} exec -T postgres_source psql -U source_user -d source_db -c \
    "SELECT 'competition' AS table_name, COUNT(*) FROM public.competition
     UNION ALL SELECT 'venue', COUNT(*) FROM public.venue
     UNION ALL SELECT 'team', COUNT(*) FROM public.team
     UNION ALL SELECT 'player', COUNT(*) FROM public.player
     UNION ALL SELECT 'player_contract', COUNT(*) FROM public.player_contract
     UNION ALL SELECT 'fixture', COUNT(*) FROM public.fixture
     UNION ALL SELECT 'match_result', COUNT(*) FROM public.match_result
     ORDER BY table_name;"

echo ""
echo "✅ Database initialization complete!"
