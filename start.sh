#!/bin/bash
set -e

echo "Starting Redpanda..."
/usr/bin/rpk redpanda start \
  --overprovisioned \
  --smp 1 \
  --memory 200M \
  --reserve-memory 0M \
  --node-id 0 \
  --check=false &

echo "Waiting for Redpanda to be ready..."
sleep 20

echo "Creating Kafka topic..."
/usr/bin/rpk topic create transactions --partitions 1 --replicas 1 2>/dev/null || echo "Topic already exists"

echo "Starting .NET backend..."
export ASPNETCORE_URLS="http://+:${PORT:-5218}"
exec dotnet /app/backend.dll
