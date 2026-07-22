#!/bin/bash
set -euo pipefail
export PORT="${PORT:-8097}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-byz.gateway.access}"
export KAFKA_GROUP="${KAFKA_GROUP:-admin-gateway-sse}"
export IAM_JWKS_URL="${IAM_JWKS_URL:-http://127.0.0.1:8082/.well-known/jwks.json}"
# Full list here — supervisord splits commas in environment=
export CORS_ORIGINS="${CORS_ORIGINS:-http://localhost:4200,https://admin.byzantineapp.dev,https://sys.byzantineapp.dev}"
export HOST_METRICS_INTERVAL="${HOST_METRICS_INTERVAL:-2s}"

exec /opt/services/admin-gateway-sse/admin-gateway-sse
