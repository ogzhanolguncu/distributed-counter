#!/bin/bash
set -e

# Get environment variables with defaults
NODE_START_PORT=${NODE_START_PORT:-9001}
NODE_COUNT=${NODE_COUNT:-3}
DISCOVERY_HOST=${DISCOVERY_HOST:-discovery-server}
DISCOVERY_PORT=${DISCOVERY_PORT:-8000}

echo "Starting $NODE_COUNT nodes beginning at port $NODE_START_PORT"
echo "Using discovery server at $DISCOVERY_HOST:$DISCOVERY_PORT"

# Start the nodes
for i in $(seq 0 $(($NODE_COUNT - 1))); do
  PORT=$(($NODE_START_PORT + $i))
  HTTP_PORT=$(($PORT - 9000 + 8010)) # Maps 9001->8011, 9002->8012, etc.

  echo "Starting node on port $PORT (HTTP port $HTTP_PORT)..."
  /app/counter -addr 0.0.0.0:$PORT -discovery $DISCOVERY_HOST:$DISCOVERY_PORT -http-port $HTTP_PORT &
done

# Keep container running
wait
