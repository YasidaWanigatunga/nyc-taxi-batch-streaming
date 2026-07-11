#!/usr/bin/env bash
# setup.sh — Assignment 2 bootstrap: brings up the full streaming stack.
set -Eeuo pipefail

log() { printf '\033[0;36m[%s] %s\033[0m\n' "$(date '+%H:%M:%S')" "$*"; }
ok()  { printf '\033[0;32m[%s] OK %s\033[0m\n' "$(date '+%H:%M:%S')" "$*"; }
die() { printf '\033[0;31m[%s] FAIL %s\033[0m\n' "$(date '+%H:%M:%S')" "$*" >&2; exit 1; }
trap 'die "failed at line $LINENO"' ERR

command -v docker >/dev/null || die "docker not found"
docker compose version >/dev/null 2>&1 || die "docker compose v2 not found"
docker info >/dev/null 2>&1 || die "Docker daemon not running"

[ -s "data/yellow_tripdata_2023-01.parquet" ] || die "Missing data/yellow_tripdata_2023-01.parquet"
ok "Source parquet present."

log "Building images and starting the stack (first run pulls Kafka images)..."
docker compose up -d --build

log "Waiting for all three Kafka brokers to report healthy..."
for b in kafka1 kafka2 kafka3; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$b" 2>/dev/null)" = "healthy" ]; do
    sleep 3
  done
  ok "$b healthy."
done

log "Waiting for topic creation..."
until docker compose logs kafka-init 2>/dev/null | grep -q "Topic ready"; do sleep 2; done
ok "Topic taxi-trips-stream ready."

cat <<'BANNER'

============================================================
  Streaming stack is up.

  Kafka        localhost:9092, :9093, :9094
  Postgres     localhost:5433  db=taxi_stream user=taxi pw=taxi
  Consumer     http://localhost:8000/metrics
  Prometheus   http://localhost:9090
  Grafana      http://localhost:3000  (admin/admin)
               dashboard: "NYC Taxi Streaming - Observability"

  Watch it flow:
    docker compose logs -f producer consumer
============================================================
BANNER
