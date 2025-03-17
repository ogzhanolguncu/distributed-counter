#!/bin/bash
set -e

# Defaults
NODE_START_PORT=${NODE_START_PORT:-9001}
NODE_COUNT=${NODE_COUNT:-3}
DISCOVERY_HOST=${DISCOVERY_HOST:-discovery-gateway}

echo "Starting $NODE_COUNT nodes beginning at port $NODE_START_PORT"
echo "Using discovery server at $DISCOVERY_HOST:8000"

# Get instance hostname for unique node identifiers
HOSTNAME=$(hostname)

# Start the specified number of nodes
for i in $(seq 0 $(($NODE_COUNT - 1))); do
  PORT=$(($NODE_START_PORT + $i))
  HTTP_PORT=$(($PORT - 9000 + 8010)) # Maps 9001->8011, 9002->8012, etc.

  echo "Starting node on port $PORT (HTTP port $HTTP_PORT)..."
  /app/counter -addr 0.0.0.0:$PORT -discovery $DISCOVERY_HOST:8000 -http-port $HTTP_PORT &
done

# Keep the container running
wait
