#!/bin/bash
set -e

# Start the discovery server
echo "Starting discovery server on port 8000..."
/app/discovery -port 8000 &

# Start the gateway
echo "Starting gateway on port 8080..."
/app/gateway -discovery localhost:8000

# Keep the container running
wait
