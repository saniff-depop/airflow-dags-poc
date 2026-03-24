#!/usr/bin/env bash
# Create Airflow connections for source and warehouse databases
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_COMPOSE_CONTEXT="${REPO_ROOT}"

COMPOSE="docker compose --env-file ${REPO_ROOT}/docker/.env -f ${REPO_ROOT}/docker/docker-compose.yaml --project-directory ${REPO_ROOT}"

echo "🔗 Creating Airflow connections..."

# Wait for airflow-scheduler to be healthy
echo "⏳ Waiting for airflow-scheduler to be ready..."
for i in {1..60}; do
    if ${COMPOSE} exec -T airflow-scheduler airflow db check &>/dev/null; then
        echo "✓ Airflow scheduler is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "❌ Airflow scheduler failed to become ready"
        exit 1
    fi
    sleep 2
done

# Add football_pg_source connection
echo ""
echo "📌 Adding football_pg_source connection..."
if ${COMPOSE} exec -T airflow-scheduler airflow connections get football_pg_source &>/dev/null; then
    echo "  Connection already exists, deleting..."
    ${COMPOSE} exec -T airflow-scheduler airflow connections delete football_pg_source
fi
${COMPOSE} exec -T airflow-scheduler airflow connections add football_pg_source \
    --conn-uri "postgresql://source_user:changeme_source@postgres_source:5432/source_db"
echo "✓ football_pg_source connection created"

# Add football_pg_warehouse connection
echo ""
echo "📌 Adding football_pg_warehouse connection..."
if ${COMPOSE} exec -T airflow-scheduler airflow connections get football_pg_warehouse &>/dev/null; then
    echo "  Connection already exists, deleting..."
    ${COMPOSE} exec -T airflow-scheduler airflow connections delete football_pg_warehouse
fi
${COMPOSE} exec -T airflow-scheduler airflow connections add football_pg_warehouse \
    --conn-uri "postgresql://warehouse_user:changeme_warehouse@postgres_warehouse:5432/warehouse_db"
echo "✓ football_pg_warehouse connection created"

echo ""
echo "📋 Verifying connections..."
${COMPOSE} exec -T airflow-scheduler airflow connections list | grep football || true

echo ""
echo "✅ Airflow connections initialized!"
