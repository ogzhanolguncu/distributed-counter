#!/bin/bash

# Default monitoring interval in seconds
INTERVAL=${1:-1}

clear

echo "===================================================="
echo "    DISTRIBUTED COUNTER CLUSTER MONITOR"
echo "===================================================="
echo "Press Ctrl+C to exit"
echo

while true; do
  # Clear screen before each update
  clear

  # Get timestamp
  timestamp=$(date "+%Y-%m-%d %H:%M:%S")

  # Prepare data
  node1_data=$(curl -s http://localhost:8010/counter 2>/dev/null || echo "ERROR")
  node2_data=$(curl -s http://localhost:8011/counter 2>/dev/null || echo "ERROR")
  node3_data=$(curl -s http://localhost:8012/counter 2>/dev/null || echo "ERROR")
  node4_data=$(curl -s http://localhost:8013/counter 2>/dev/null || echo "ERROR")
  node5_data=$(curl -s http://localhost:8014/counter 2>/dev/null || echo "ERROR")

  # Extract counter values and versions using more compatible grep patterns
  node1_counter=$(echo "$node1_data" | grep -o "Counter: [0-9]*" | cut -d' ' -f2 || echo "?")
  node1_version=$(echo "$node1_data" | grep -o "version [0-9]*" | cut -d' ' -f2 || echo "?")

  node2_counter=$(echo "$node2_data" | grep -o "Counter: [0-9]*" | cut -d' ' -f2 || echo "?")
  node2_version=$(echo "$node2_data" | grep -o "version [0-9]*" | cut -d' ' -f2 || echo "?")

  node3_counter=$(echo "$node3_data" | grep -o "Counter: [0-9]*" | cut -d' ' -f2 || echo "?")
  node3_version=$(echo "$node3_data" | grep -o "version [0-9]*" | cut -d' ' -f2 || echo "?")

  node4_counter=$(echo "$node4_data" | grep -o "Counter: [0-9]*" | cut -d' ' -f2 || echo "?")
  node4_version=$(echo "$node4_data" | grep -o "version [0-9]*" | cut -d' ' -f2 || echo "?")

  node5_counter=$(echo "$node5_data" | grep -o "Counter: [0-9]*" | cut -d' ' -f2 || echo "?")
  node5_version=$(echo "$node5_data" | grep -o "version [0-9]*" | cut -d' ' -f2 || echo "?")

  # Check for convergence
  if [[ "$node1_counter" == "$node2_counter" &&
    "$node2_counter" == "$node3_counter" &&
    "$node3_counter" == "$node4_counter" &&
    "$node4_counter" == "$node5_counter" &&
    "$node1_counter" != "?" ]]; then
    convergence="✓ CONVERGED"
  else
    convergence="× NOT CONVERGED"
  fi

  # Display data
  echo "===================================================="
  echo "    DISTRIBUTED COUNTER CLUSTER MONITOR"
  echo "===================================================="
  echo "Press Ctrl+C to exit"
  echo
  echo "Time: $timestamp"
  echo
  echo "Node | Counter | Version"
  echo "-----|---------|--------"
  echo "  1  |   $node1_counter   |   $node1_version"
  echo "  2  |   $node2_counter   |   $node2_version"
  echo "  3  |   $node3_counter   |   $node3_version"
  echo "  4  |   $node4_counter   |   $node4_version"
  echo "  5  |   $node5_counter   |   $node5_version"
  echo
  echo "Status: $convergence"
  echo
  echo "===================================================="

  # Wait for next update
  sleep $INTERVAL
done
