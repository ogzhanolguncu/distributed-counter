#!/bin/bash

# Variables
DISCOVERY_PORT=8000
BASE_NODE_PORT=9000
BASE_HTTP_PORT=8010
NUM_NODES=5
PART3_DIR="./part3"
LOG_DIR="./logs"

# Colors for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to display usage information
function show_usage {
  echo -e "${BLUE}=====================================${NC}"
  echo -e "${BLUE}  Distributed Counter Cluster Manager ${NC}"
  echo -e "${BLUE}=====================================${NC}"
  echo ""
  echo "Usage: ./run-cluster.sh [command]"
  echo ""
  echo "Commands:"
  echo "  start           Start discovery server and cluster nodes"
  echo "  stop            Stop all running cluster components"
  echo "  status          Check status of cluster components"
  echo "  increment [N]   Increment counter on node N (default: random node)"
  echo "  inc [N]         Shorthand for increment"
  echo "  decrement [N]   Decrement counter on node N (default: random node)"
  echo "  dec [N]         Shorthand for decrement"
  echo "  counter [N]     Get counter value from node N (default: all nodes)"
  echo "  cnt [N]         Shorthand for counter"
  echo "  peers [N]       Get peer information from node N (default: all nodes)"
  echo "  p [N]           Shorthand for peers"
  echo "  logs [N]        View logs (N=node number or 'discovery', empty for all)"
  echo "  consistency     Check counter consistency across all nodes"
  echo "  check           Run a short functionality test on the cluster"
  echo "  simulation [D] [I] [O]  Run a convergence simulation"
  echo "                   D: duration in seconds (default: 60)"
  echo "                   I: check interval in seconds (default: 2)"
  echo "                   O: operations per cycle (default: 3)"
  echo "  clean           Clean up logs and data directories"
  echo "  help            Show this help message"
  echo ""
  echo "Examples:"
  echo "  ./run-cluster.sh start         # Start a 5-node cluster"
  echo "  ./run-cluster.sh inc 2         # Increment counter on node 2"
  echo "  ./run-cluster.sh cnt           # Get counter from all nodes"
  echo "  ./run-cluster.sh p             # Get peer info from all nodes"
  echo "  ./run-cluster.sh consistency   # Check if counters are consistent"
  echo "  ./run-cluster.sh simulation 120 3 5  # Run 120s simulation, check every 3s, 5 ops per cycle"
  echo "  ./run-cluster.sh stop          # Stop the cluster and clear ports"
}

# Function to create directories
function create_dirs {
  mkdir -p $LOG_DIR
  mkdir -p data/wal
}

# Function to check if process is running
function is_running {
  local port=$1
  netstat -tuln 2>/dev/null | grep -q ":$port " || lsof -i ":$port" >/dev/null 2>&1
  return $?
}

# Function to check if a specific node is running
function is_node_running {
  local node_num=$1
  local node_port=$((BASE_NODE_PORT + node_num - 1))
  is_running $node_port
  return $?
}

# Function to check if HTTP server for a node is responding
function is_http_running {
  local node_num=$1
  local http_port=$((BASE_HTTP_PORT + node_num - 1))
  curl --output /dev/null --silent --head --fail "http://localhost:$http_port/counter" >/dev/null 2>&1
  return $?
}

# Function to check if file paths for the cluster exist
function check_paths {
  # Check if part3 directory exists
  if [ ! -d "$PART3_DIR" ]; then
    echo -e "${RED}ERROR: Directory $PART3_DIR does not exist${NC}"
    echo "This script should be run from the parent directory of part3"
    return 1
  fi

  # Check if discovery main.go exists
  if [ ! -f "$PART3_DIR/cmd/discovery/main.go" ]; then
    echo -e "${RED}ERROR: Discovery server code not found at $PART3_DIR/cmd/discovery/main.go${NC}"
    return 1
  fi

  # Check if counter main.go exists
  if [ ! -f "$PART3_DIR/cmd/counter/main.go" ]; then
    echo -e "${RED}ERROR: Counter code not found at $PART3_DIR/cmd/counter/main.go${NC}"
    return 1
  fi

  return 0
}

# Function to check Go mod structure
function check_go_mod_structure {
  # Try to detect if this is a Go module structure
  if [ ! -f "go.mod" ]; then
    echo -e "${YELLOW}Warning: go.mod file not found in current directory${NC}"
    echo -e "${YELLOW}The script assumes you're running from the root of your Go module${NC}"
  else
    echo -e "${GREEN}Found go.mod, valid Go module structure${NC}"
  fi
}

# Function to check for port availability
function check_ports {
  local ports_in_use=""

  # Check discovery port
  if is_running $DISCOVERY_PORT; then
    ports_in_use="$ports_in_use $DISCOVERY_PORT"
  fi

  # Check node ports
  for ((i = 1; i <= NUM_NODES; i++)); do
    local node_port=$((BASE_NODE_PORT + i - 1))
    if is_running $node_port; then
      ports_in_use="$ports_in_use $node_port"
    fi
  done

  if [ -n "$ports_in_use" ]; then
    echo -e "${YELLOW}WARNING: The following ports are already in use:$ports_in_use${NC}"
    echo -e "${YELLOW}This may cause conflicts with the cluster. Consider stopping existing services or changing ports.${NC}"
    return 1
  fi

  return 0
}

# Function to start discovery server
function start_discovery {
  echo "Starting discovery server on port $DISCOVERY_PORT..."
  if is_running $DISCOVERY_PORT; then
    echo -e "${YELLOW}Discovery server already running on port $DISCOVERY_PORT${NC}"
  else
    mkdir -p $LOG_DIR
    # Using full path for better process tracking
    go run $PART3_DIR/cmd/discovery/main.go --port=$DISCOVERY_PORT --cleanup=5s >$LOG_DIR/discovery.log 2>&1 &
    local pid=$!
    echo -e "${GREEN}Discovery server started with PID $pid${NC}"
    # Check if it's actually running
    sleep 2
    if ! is_running $DISCOVERY_PORT; then
      echo -e "${RED}ERROR: Discovery server failed to start. Check logs at $LOG_DIR/discovery.log${NC}"
      tail -n 10 $LOG_DIR/discovery.log
      return 1
    fi
    # Save PID for later use
    echo $pid >$LOG_DIR/discovery.pid
  fi
  return 0
}

# Function to start node
function start_node {
  local node_num=$1
  local node_port=$((BASE_NODE_PORT + node_num - 1))
  local http_port=$((BASE_HTTP_PORT + node_num - 1))

  # Set different parameters based on node number for diversity in testing
  local max_peers=2
  local sync_interval="2s"
  local wal_enabled=true
  local fsync_enabled=true

  case $node_num in
  2) # Node 2 - More aggressive sync
    max_peers=3
    sync_interval="1s"
    ;;
  3) # Node 3 - No WAL
    wal_enabled=false
    fsync_enabled=false
    ;;
  4) # Node 4 - WAL without fsync
    wal_enabled=true
    fsync_enabled=false
    ;;
  5) # Node 5 - Slower sync
    max_peers=1
    sync_interval="5s"
    ;;
  esac

  echo "Starting node $node_num on port $node_port (HTTP: $http_port)..."
  if is_running $node_port; then
    echo -e "${YELLOW}Node $node_num already running on port $node_port${NC}"
  else
    go run $PART3_DIR/cmd/counter/main.go \
      --addr="localhost:$node_port" \
      --discovery="localhost:$DISCOVERY_PORT" \
      --max-peers=$max_peers \
      --sync=$sync_interval \
      --wal=$wal_enabled \
      --wal-fsync=$fsync_enabled \
      >$LOG_DIR/node-$node_num.log 2>&1 &
    local pid=$!
    echo -e "${GREEN}Node $node_num started with PID $pid${NC}"
    # Check if it's actually running
    sleep 2
    if ! is_running $node_port; then
      echo -e "${RED}ERROR: Node $node_num failed to start. Check logs at $LOG_DIR/node-$node_num.log${NC}"
      tail -n 10 $LOG_DIR/node-$node_num.log
      return 1
    fi
    # Save PID for later use
    echo $pid >$LOG_DIR/node-$node_num.pid
  fi
  return 0
}

# Function to stop processes on port
function stop_on_port {
  local port=$1
  local name=$2
  if is_running $port; then
    echo "Stopping $name on port $port..."
    pid=$(lsof -ti :$port 2>/dev/null || netstat -tuln 2>/dev/null | grep ":$port " | awk '{print $7}' | cut -d/ -f1)
    if [ -n "$pid" ]; then
      kill -15 $pid # Try SIGTERM first
      sleep 1
      if is_running $port; then
        echo "Forcing termination of $name..."
        kill -9 $pid # Force with SIGKILL if still running
      fi
      echo "$name stopped"
    fi
  else
    echo "$name not running on port $port"
  fi
}

# Function to make HTTP request to node
function node_request {
  local node_num=$1
  local endpoint=$2
  local http_port=$((BASE_HTTP_PORT + node_num - 1))
  curl -s "http://localhost:$http_port/$endpoint"
}

# Function to get counter from a node
function get_counter_value {
  local node_num=$1
  local result=$(node_request $node_num "counter")
  local counter=$(echo "$result" | grep -oE "Counter: [0-9]+" | cut -d' ' -f2)
  local version=$(echo "$result" | grep -oE "version [0-9]+" | cut -d' ' -f2)

  # Return as counter:version format
  if [ -n "$counter" ] && [ -n "$version" ]; then
    echo "$counter:$version"
  else
    echo "ERROR"
  fi
}

# Function to check consistency across nodes
function check_consistency {
  echo -e "${CYAN}Checking cluster consistency...${NC}"

  local values=()
  local consistent=true
  local first_value=""
  local running_nodes=0

  for ((i = 1; i <= NUM_NODES; i++)); do
    if is_node_running $i && is_http_running $i; then
      local result=$(get_counter_value $i)
      running_nodes=$((running_nodes + 1))

      if [ "$result" = "ERROR" ]; then
        echo -e "${RED}Node $i: Error getting counter value${NC}"
        consistent=false
      else
        local counter=${result%:*}
        local version=${result#*:}

        echo -e "${GREEN}Node $i: Counter=$counter, Version=$version${NC}"
        values+=("$result")

        if [ -z "$first_value" ]; then
          first_value=$result
        elif [ "$result" != "$first_value" ]; then
          consistent=false
        fi
      fi
    else
      echo -e "${RED}Node $i: Not running${NC}"
    fi
  done

  if [ $running_nodes -eq 0 ]; then
    echo -e "${RED}No running nodes found. Cannot check consistency.${NC}"
    return 1
  fi

  if $consistent; then
    if [ ${#values[@]} -gt 1 ]; then
      local counter=${first_value%:*}
      local version=${first_value#*:}
      echo -e "${GREEN}✓ All nodes are consistent with counter=$counter, version=$version${NC}"
    else
      echo -e "${YELLOW}⚠ Only one node is running. Consistency check requires multiple nodes.${NC}"
    fi
  else
    echo -e "${RED}✗ Inconsistency detected across nodes!${NC}"
    return 1
  fi

  return 0
}

# Function to run a short functionality test
function run_functionality_test {
  echo -e "${BLUE}=====================================${NC}"
  echo -e "${BLUE}    Running Functionality Test      ${NC}"
  echo -e "${BLUE}=====================================${NC}"

  # First check if nodes are running
  local running_nodes=0
  for ((i = 1; i <= NUM_NODES; i++)); do
    if is_node_running $i && is_http_running $i; then
      running_nodes=$((running_nodes + 1))
    fi
  done

  if [ $running_nodes -lt 2 ]; then
    echo -e "${RED}Need at least 2 running nodes for testing. Only $running_nodes found.${NC}"
    return 1
  fi

  # Test 1: Check initial consistency
  echo -e "${CYAN}Test 1: Checking initial cluster state${NC}"
  check_consistency
  echo ""

  # Choose nodes for testing
  local inc_node=1
  local dec_node=3
  for ((i = 1; i <= NUM_NODES; i++)); do
    if is_node_running $i && is_http_running $i; then
      inc_node=$i
      break
    fi
  done

  for ((i = NUM_NODES; i >= 1; i--)); do
    if [ $i -ne $inc_node ] && is_node_running $i && is_http_running $i; then
      dec_node=$i
      break
    fi
  done

  # Test 2: Increment counter on first node
  echo -e "${CYAN}Test 2: Incrementing counter on node $inc_node${NC}"
  echo -n "Before: "
  node_request $inc_node "counter"
  node_request $inc_node "increment" >/dev/null
  echo -n "After: "
  node_request $inc_node "counter"
  echo ""

  # Wait for gossip propagation
  echo "Waiting for propagation (5s)..."
  sleep 5

  # Test 3: Check if increment propagated
  echo -e "${CYAN}Test 3: Checking propagation of increment${NC}"
  check_consistency
  echo ""

  # Test 4: Decrement counter on second node
  echo -e "${CYAN}Test 4: Decrementing counter on node $dec_node${NC}"
  echo -n "Before: "
  node_request $dec_node "counter"
  node_request $dec_node "decrement" >/dev/null
  echo -n "After: "
  node_request $dec_node "counter"
  echo ""

  # Wait for gossip propagation
  echo "Waiting for propagation (5s)..."
  sleep 5

  # Test 5: Final consistency check
  echo -e "${CYAN}Test 5: Final consistency check${NC}"
  check_consistency

  echo ""
  echo -e "${GREEN}Test complete!${NC}"
}

# Function to process command
function process_command {
  # Check if Go is installed
  if ! command -v go &>/dev/null; then
    echo -e "${RED}Error: Go is not installed or not in your PATH${NC}"
    exit 1
  fi

  # Convert shorthand commands to full commands
  local cmd=$1
  case $cmd in
  inc) cmd="increment" ;;
  dec) cmd="decrement" ;;
  cnt) cmd="counter" ;;
  p) cmd="peers" ;;
  esac

  case $cmd in
  start)
    create_dirs

    # Check Go module structure
    check_go_mod_structure

    # Check paths before starting
    if ! check_paths; then
      exit 1
    fi

    # Check ports before starting
    check_ports

    # Start discovery server
    if ! start_discovery; then
      echo -e "${RED}Failed to start discovery server. Aborting cluster startup.${NC}"
      exit 1
    fi

    # Start nodes
    local failed_nodes=0
    for ((i = 1; i <= NUM_NODES; i++)); do
      if ! start_node $i; then
        failed_nodes=$((failed_nodes + 1))
      fi
      sleep 1 # Brief pause between starting nodes
    done

    if [ $failed_nodes -eq 0 ]; then
      echo -e "${GREEN}Cluster started successfully with $NUM_NODES nodes${NC}"
    else
      echo -e "${YELLOW}WARNING: Cluster started with $((NUM_NODES - failed_nodes)) nodes. $failed_nodes node(s) failed to start.${NC}"
    fi
    echo "Run './run-cluster.sh status' to verify all components are running"

    # Wait a moment for HTTP servers to initialize
    echo "Waiting for HTTP servers to initialize..."
    sleep 5

    # Check HTTP servers
    local http_failures=0
    for ((i = 1; i <= NUM_NODES; i++)); do
      local http_port=$((BASE_HTTP_PORT + i - 1))
      # Try the node port directly for HTTP connections
      if curl --output /dev/null --silent --fail http://localhost:$http_port/counter; then
        echo -e "${GREEN}Node $i HTTP server is ready on port $http_port${NC}"
      else
        echo -e "${YELLOW}Node $i HTTP server not responding on port $http_port${NC}"
        http_failures=$((http_failures + 1))
      fi
    done

    if [ $http_failures -gt 0 ]; then
      echo -e "${YELLOW}WARNING: $http_failures HTTP server(s) are not responding.${NC}"
      echo -e "${YELLOW}Check your counter implementation and logs for details.${NC}"
      echo -e "${YELLOW}Counter operations may not work correctly.${NC}"
    fi

    # Print node configurations
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}     Node Configurations            ${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo -e "Node 1: Default configuration"
    echo -e "Node 2: More aggressive sync (3 peers, 1s interval)"
    echo -e "Node 3: No WAL"
    echo -e "Node 4: WAL without fsync"
    echo -e "Node 5: Slower sync (5s interval)"
    echo ""
    ;;

  stop)
    echo "Stopping cluster components..."

    # Stop nodes first, then discovery
    for ((i = 1; i <= NUM_NODES; i++)); do
      stop_on_port $((BASE_NODE_PORT + i - 1)) "Node $i"
      # Also try using saved PID if available
      if [ -f "$LOG_DIR/node-$i.pid" ]; then
        pid=$(cat "$LOG_DIR/node-$i.pid")
        if kill -0 $pid 2>/dev/null; then
          echo "Stopping Node $i (PID: $pid)..."
          kill -15 $pid
          sleep 1
          # Force kill if still running
          if kill -0 $pid 2>/dev/null; then
            kill -9 $pid
          fi
        fi
        rm "$LOG_DIR/node-$i.pid"
      fi
    done

    # Stop discovery server
    stop_on_port $DISCOVERY_PORT "Discovery server"
    # Also try using saved PID if available
    if [ -f "$LOG_DIR/discovery.pid" ]; then
      pid=$(cat "$LOG_DIR/discovery.pid")
      if kill -0 $pid 2>/dev/null; then
        echo "Stopping Discovery server (PID: $pid)..."
        kill -15 $pid
        sleep 1
        # Force kill if still running
        if kill -0 $pid 2>/dev/null; then
          kill -9 $pid
        fi
      fi
      rm "$LOG_DIR/discovery.pid"
    fi

    # Verify all ports are cleared
    local all_clear=true
    if is_running $DISCOVERY_PORT; then
      echo -e "${YELLOW}WARNING: Port $DISCOVERY_PORT still in use after stop attempt${NC}"
      all_clear=false
    fi

    for ((i = 1; i <= NUM_NODES; i++)); do
      local node_port=$((BASE_NODE_PORT + i - 1))
      if is_running $node_port; then
        echo -e "${YELLOW}WARNING: Port $node_port still in use after stop attempt${NC}"
        all_clear=false
      fi
    done

    if $all_clear; then
      echo -e "${GREEN}All ports successfully cleared. Cluster stopped.${NC}"
    else
      echo -e "${YELLOW}Some ports are still in use. You may need to manually kill processes.${NC}"
      echo "Try: ps aux | grep 'part3/cmd' | grep -v grep to find processes"
      echo "Then: kill -9 <PID> to forcefully terminate them"
    fi
    ;;

  status)
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}     Distributed Counter Status     ${NC}"
    echo -e "${BLUE}=====================================${NC}"

    # Check discovery status
    if is_running $DISCOVERY_PORT; then
      echo -e "${GREEN}✓ Discovery server is RUNNING on port $DISCOVERY_PORT${NC}"
    else
      echo -e "${RED}✗ Discovery server is NOT RUNNING${NC}"
    fi

    # Check nodes status
    local running_count=0
    for ((i = 1; i <= NUM_NODES; i++)); do
      local node_port=$((BASE_NODE_PORT + i - 1))
      local http_port=$((BASE_HTTP_PORT + i - 1))
      if is_running $node_port; then
        running_count=$((running_count + 1))
        echo -e "${GREEN}✓ Node $i is RUNNING (Port: $node_port, HTTP: $http_port)${NC}"
        # Also check if HTTP server is running
        if curl --output /dev/null --silent --head --fail http://localhost:$http_port/counter; then
          echo -e "  ${GREEN}✓ HTTP server on port $http_port is responding${NC}"

          # Get counter value
          local counter_info=$(get_counter_value $i)
          if [ "$counter_info" != "ERROR" ]; then
            local counter=${counter_info%:*}
            local version=${counter_info#*:}
            echo -e "  ${GREEN}✓ Counter: $counter, Version: $version${NC}"
          else
            echo -e "  ${RED}✗ Error getting counter value${NC}"
          fi
        else
          echo -e "  ${RED}✗ HTTP server on port $http_port is not responding${NC}"
        fi
      else
        echo -e "${RED}✗ Node $i is NOT RUNNING${NC}"
      fi
    done

    echo ""
    echo -e "${BLUE}Summary:${NC} $running_count of $NUM_NODES nodes running"
    ;;

  increment | inc)
    if [ -z "$2" ]; then
      # Randomly select a node
      node_num=$((RANDOM % NUM_NODES + 1))
    else
      node_num=$2
    fi

    if [[ $node_num -lt 1 || $node_num -gt $NUM_NODES ]]; then
      echo -e "${RED}Invalid node number. Use 1-$NUM_NODES${NC}"
    else
      if is_node_running $node_num; then
        echo "Incrementing counter on node $node_num..."
        node_request $node_num "increment"
      else
        echo -e "${RED}Node $node_num is not running${NC}"
      fi
    fi
    ;;

  decrement | dec)
    if [ -z "$2" ]; then
      # Randomly select a node
      node_num=$((RANDOM % NUM_NODES + 1))
    else
      node_num=$2
    fi

    if [[ $node_num -lt 1 || $node_num -gt $NUM_NODES ]]; then
      echo -e "${RED}Invalid node number. Use 1-$NUM_NODES${NC}"
    else
      if is_node_running $node_num; then
        echo "Decrementing counter on node $node_num..."
        node_request $node_num "decrement"
      else
        echo -e "${RED}Node $node_num is not running${NC}"
      fi
    fi
    ;;

  counter | cnt)
    if [ -z "$2" ]; then
      # Get from all nodes
      echo -e "${BLUE}=====================================${NC}"
      echo -e "${BLUE}     Counter Values Across Nodes    ${NC}"
      echo -e "${BLUE}=====================================${NC}"

      for ((i = 1; i <= NUM_NODES; i++)); do
        if is_node_running $i && is_http_running $i; then
          local result=$(node_request $i "counter")
          echo -e "${GREEN}Node $i:${NC} $result"
        else
          echo -e "${RED}Node $i: Not running${NC}"
        fi
      done
    else
      node_num=$2
      if [[ $node_num -lt 1 || $node_num -gt $NUM_NODES ]]; then
        echo -e "${RED}Invalid node number. Use 1-$NUM_NODES${NC}"
      else
        if is_node_running $node_num && is_http_running $node_num; then
          echo "Getting counter value from node $node_num:"
          node_request $node_num "counter"
        else
          echo -e "${RED}Node $node_num is not running${NC}"
        fi
      fi
    fi
    ;;

  peers | p)
    if [ -z "$2" ]; then
      # Get from all nodes
      echo -e "${BLUE}=====================================${NC}"
      echo -e "${BLUE}     Peer Information Across Nodes  ${NC}"
      echo -e "${BLUE}=====================================${NC}"

      for ((i = 1; i <= NUM_NODES; i++)); do
        if is_node_running $i && is_http_running $i; then
          echo -e "${GREEN}Node $i peers:${NC}"
          node_request $i "peers"
          echo ""
        else
          echo -e "${RED}Node $i: Not running${NC}"
        fi
      done
    else
      node_num=$2
      if [[ $node_num -lt 1 || $node_num -gt $NUM_NODES ]]; then
        echo -e "${RED}Invalid node number. Use 1-$NUM_NODES${NC}"
      else
        if is_node_running $node_num && is_http_running $node_num; then
          echo "Getting peer information from node $node_num:"
          node_request $node_num "peers"
        else
          echo -e "${RED}Node $node_num is not running${NC}"
        fi
      fi
    fi
    ;;

  consistency)
    check_consistency
    ;;

  check)
    run_functionality_test
    ;;

  logs)
    local node_num=$2
    if [ -z "$node_num" ]; then
      echo -e "${BLUE}=====================================${NC}"
      echo -e "${BLUE}       Viewing Cluster Logs         ${NC}"
      echo -e "${BLUE}=====================================${NC}"

      echo -e "${CYAN}Discovery server log:${NC}"
      if [ -f "$LOG_DIR/discovery.log" ]; then
        tail -n 20 "$LOG_DIR/discovery.log"
      else
        echo "No discovery server log found"
      fi
      echo ""
      echo -e "${CYAN}Node logs (last 5 lines each):${NC}"
      for ((i = 1; i <= NUM_NODES; i++)); do
        echo -e "${YELLOW}--- Node $i log ---${NC}"
        if [ -f "$LOG_DIR/node-$i.log" ]; then
          tail -n 5 "$LOG_DIR/node-$i.log"
        else
          echo "No log found for node $i"
        fi
        echo ""
      done
    else
      if [ "$node_num" = "discovery" ]; then
        echo -e "${BLUE}=====================================${NC}"
        echo -e "${BLUE}     Discovery Server Log           ${NC}"
        echo -e "${BLUE}=====================================${NC}"
        if [ -f "$LOG_DIR/discovery.log" ]; then
          tail -n 50 "$LOG_DIR/discovery.log"
        else
          echo "No discovery server log found"
        fi
      else
        if [[ $node_num -lt 1 || $node_num -gt $NUM_NODES ]]; then
          echo -e "${RED}Invalid node number. Use 1-$NUM_NODES or 'discovery'${NC}"
        else
          echo -e "${BLUE}=====================================${NC}"
          echo -e "${BLUE}     Node $node_num Log              ${NC}"
          echo -e "${BLUE}=====================================${NC}"
          if [ -f "$LOG_DIR/node-$node_num.log" ]; then
            tail -n 50 "$LOG_DIR/node-$node_num.log"
          else
            echo "No log found for node $node_num"
          fi
        fi
      fi
    fi
    ;;

  clean)
    echo "Cleaning up logs and data..."
    rm -rf $LOG_DIR/*
    rm -rf data/wal/*
    echo -e "${GREEN}Cleanup complete${NC}"
    ;;

  simulation)
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}     Distributed Counter Simulation  ${NC}"
    echo -e "${BLUE}=====================================${NC}"

    # Check if a duration was specified
    local duration=60     # Default 60 seconds
    local interval=2      # Default check every 2 seconds
    local ops_per_cycle=3 # Default operations per cycle

    if [ -n "$2" ]; then
      duration=$2
    fi

    if [ -n "$3" ]; then
      interval=$3
    fi

    if [ -n "$4" ]; then
      ops_per_cycle=$4
    fi

    echo "Running simulation for $duration seconds (checking every $interval seconds, $ops_per_cycle ops per cycle)..."
    echo "Press Ctrl+C to stop the simulation"

    # Track convergence metrics
    local total_checks=0
    local consistent_checks=0
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))

    # Clear the screen and set cursor to home position
    clear

    while [ $(date +%s) -lt $end_time ]; do
      # Perform random operations
      for ((i = 0; i < ops_per_cycle; i++)); do
        # Choose random node
        local node_num=$((RANDOM % NUM_NODES + 1))

        # Choose random operation (increment or decrement)
        if [ $((RANDOM % 2)) -eq 0 ]; then
          # Increment
          if is_node_running $node_num && is_http_running $node_num; then
            local result=$(node_request $node_num "increment")
            echo -e "\033[K${CYAN}[$(date +%H:%M:%S)]${NC} Incremented counter on Node $node_num: $result"
          fi
        else
          # Decrement
          if is_node_running $node_num && is_http_running $node_num; then
            local result=$(node_request $node_num "decrement")
            echo -e "\033[K${CYAN}[$(date +%H:%M:%S)]${NC} Decremented counter on Node $node_num: $result"
          fi
        fi

        # Brief pause between operations
        sleep 0.2
      done

      # Check consistency
      echo -e "\033[K${YELLOW}[$(date +%H:%M:%S)]${NC} Checking consistency..."

      # Get counter values from all nodes
      local values=()
      local first_value=""
      local consistent=true
      local running_nodes=0

      for ((i = 1; i <= NUM_NODES; i++)); do
        if is_node_running $i && is_http_running $i; then
          local result=$(get_counter_value $i)
          running_nodes=$((running_nodes + 1))

          if [ "$result" != "ERROR" ]; then
            local counter=${result%:*}
            local version=${result#*:}

            values+=("$result")
            echo -e "\033[K${GREEN}Node $i:${NC} Counter=$counter, Version=$version"

            if [ -z "$first_value" ]; then
              first_value=$result
            elif [ "$result" != "$first_value" ]; then
              consistent=false
            fi
          else
            echo -e "\033[K${RED}Node $i: Error getting counter value${NC}"
            consistent=false
          fi
        else
          echo -e "\033[K${RED}Node $i: Not running${NC}"
        fi
      done

      # Update metrics
      total_checks=$((total_checks + 1))

      if [ $running_nodes -gt 1 ]; then
        if $consistent; then
          consistent_checks=$((consistent_checks + 1))
          local counter=${first_value%:*}
          local version=${first_value#*:}
          echo -e "\033[K${GREEN}✓ CONSISTENT: All nodes have counter=$counter, version=$version${NC}"
        else
          echo -e "\033[K${RED}✗ INCONSISTENT: Nodes have different values${NC}"
        fi

        # Calculate and display consistency percentage
        local consistency_pct=$(((consistent_checks * 100) / total_checks))
        echo -e "\033[K${BLUE}Consistency rate:${NC} $consistency_pct% ($consistent_checks/$total_checks checks)"

        # Display time remaining
        local current_time=$(date +%s)
        local remaining=$((end_time - current_time))
        echo -e "\033[K${BLUE}Time remaining:${NC} $remaining seconds"
      else
        echo -e "\033[K${YELLOW}⚠ Not enough running nodes to check consistency${NC}"
      fi

      # Move cursor back up to overwrite the same area
      local lines_to_move=$((NUM_NODES + 5))
      for ((i = 0; i < lines_to_move; i++)); do
        echo -e "\033[1A"
      done

      # Wait before next cycle
      sleep $interval
    done

    # Final results (don't overwrite)
    echo -e "\n\n\n\n\n\n\n\n" # Add enough newlines to clear previous content
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}     Simulation Results             ${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${CYAN}Duration:${NC} $duration seconds"
    echo -e "${CYAN}Operations:${NC} ~$((total_checks * ops_per_cycle)) operations performed"
    echo -e "${CYAN}Checks:${NC} $total_checks consistency checks"
    echo -e "${CYAN}Consistent:${NC} $consistent_checks checks showed consistency"

    # Calculate final consistency percentage
    if [ $total_checks -gt 0 ]; then
      local consistency_pct=$(((consistent_checks * 100) / total_checks))
      echo -e "${CYAN}Consistency rate:${NC} $consistency_pct%"

      # Interpret results
      if [ $consistency_pct -ge 95 ]; then
        echo -e "${GREEN}Excellent consistency! Your cluster is highly reliable.${NC}"
      elif [ $consistency_pct -ge 80 ]; then
        echo -e "${GREEN}Good consistency. The cluster is functioning well.${NC}"
      elif [ $consistency_pct -ge 60 ]; then
        echo -e "${YELLOW}Moderate consistency. Consider tuning your sync intervals.${NC}"
      else
        echo -e "${RED}Poor consistency. You might need to investigate network conditions or increase sync frequency.${NC}"
      fi
    fi

    # Final state of the cluster
    echo -e "\n${BLUE}Final Cluster State:${NC}"
    check_consistency
    ;;

  help | *)
    show_usage
    ;;
  esac
}

# Main script execution
if [ $# -eq 0 ]; then
  show_usage
else
  process_command "$@"
fi
