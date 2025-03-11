# Building a Distributed Counter with Gossip Protocol

This repository contains a step-by-step implementation of a distributed counter system using gossip protocol. The project demonstrates core concepts in distributed systems including peer-to-peer communication, eventual consistency, and fault tolerance.

## Series Overview

This blog series walks through building a complete distributed counter system from scratch:

- **Part 0: Introduction to Distributed Counter with Gossip Protocol**  
  Overview of distributed systems concepts and the gossip protocol approach.

- **Part 1: TCP and Network Layer**  
  Implementation of the low-level networking components using TCP for reliable node communication.

- **Part 2: Peer Discovery Service**  
  Building a discovery mechanism to help nodes find each other in the network.

- **Part 3: Consistency & Recoverability**  
  Ensuring data consistency across the distributed system and handling node failures with a Write-Ahead Log (WAL).

## System Architecture

Our distributed counter system consists of the following components:

### Core Components

1. **Node**: The primary entity in our system that maintains counter state and communicates with peers.
2. **Transport Layer**: Handles TCP communication between nodes.
3. **Peer Management**: Tracks and manages connections to other nodes in the system.
4. **Discovery Service**: Helps nodes find and connect to each other.
5. **Protocol**: Defines message structures for communication between nodes.
6. **Write-Ahead Log (WAL)**: Provides persistence and recovery capabilities.

### Key Features

- **Eventual Consistency**: The system prioritizes availability and partition tolerance, achieving consistency through gossip protocol.
- **Anti-Entropy Mechanism**: Periodic state synchronization to prevent data drift.
- **Peer Discovery**: Centralized discovery service for node registration and peer lookup.
- **Fault Tolerance**: The system continues to function despite node failures.
- **Durability & Recovery**: Write-Ahead Log ensures operations can be recovered after restarts or crashes.

## Implementation Details

This project implements Design by Contract (DBC) principles throughout the codebase, using assertions to verify preconditions, postconditions, and invariants. The `assertions` package provides functions like `Assert`, `AssertEqual`, and `AssertNotNil` that make the code more robust and self-documenting while catching errors early during development.

### Gossip Protocol

The system uses a gossip protocol for distributing counter updates across the network:

- Each node maintains its own version of the distributed counter
- When a node's counter changes, it "gossips" the update to a subset of peers
- Updates propagate throughout the network through subsequent gossip rounds
- Nodes use versioning to determine which updates to apply

### Write-Ahead Log (WAL)

The WAL implementation provides durability and recovery capabilities:

- All operations (increment, decrement, updates) are logged before being applied
- During startup, nodes replay their WAL to recover their state
- The WAL handles corruption detection and repair
- Logs are segmented to allow for efficient storage management
- Periodic fsync operations ensure durability even in case of sudden failures

### Message Types

The protocol defines two primary message types:

- **PULL**: Requests the current state from another node
- **PUSH**: Sends the current state to another node

### Anti-Entropy Process

To prevent data drift and ensure eventual consistency:

- Nodes periodically initiate sync rounds with random peers
- Version numbers are used to determine the most recent state
- Higher versions always override lower versions
- The WAL ensures operations can be recovered even after failures

## Getting Started

### Prerequisites

- Go 1.22 or higher
- Basic understanding of distributed systems concepts

### Running the System

#### Using the Management Script

The easiest way to run the system is using the provided cluster management script:

1. Start a complete cluster with 5 nodes:

   ```bash
   ./run-cluster.sh start
   ```

2. Check the status of the cluster:

   ```bash
   ./run-cluster.sh status
   ```

3. Increment or decrement the counter:

   ```bash
   ./run-cluster.sh increment  # Increment on a random node
   ./run-cluster.sh dec 2      # Decrement on node 2
   ```

4. Get counter values from all nodes to verify consistency:

   ```bash
   ./run-cluster.sh counter
   ```

5. Stop the cluster:
   ```bash
   ./run-cluster.sh stop
   ```

Other useful commands:

```bash
./run-cluster.sh peers         # View peer connections across nodes
./run-cluster.sh logs          # View system logs
./run-cluster.sh consistency   # Check if counter values are consistent
./run-cluster.sh check         # Run a quick functionality test
./run-cluster.sh simulation    # Run an automated convergence simulation
```

#### Simulation Mode

The simulation command runs an automated test that randomly increments and decrements the counter across nodes while monitoring convergence:

```bash
./run-cluster.sh simulation [duration] [interval] [ops_per_cycle]
```

Parameters:

- `duration`: How long to run the simulation in seconds (default: 60)
- `interval`: How often to check consistency in seconds (default: 2)
- `ops_per_cycle`: How many operations to perform in each cycle (default: 3)

Example:

```bash
./run-cluster.sh simulation 120 3 5  # 2-minute simulation, check every 3s, 5 ops per cycle
```

This simulation is particularly useful for:

- Testing eventual consistency behavior
- Observing convergence time under different loads
- Evaluating the impact of different node configurations
- Demonstrating fault tolerance by stopping/starting nodes during the simulation

#### Running Manually

If you prefer to run components manually:

1. Start the discovery server:

   ```bash
   go run part3/cmd/discovery/main.go
   ```

2. Start multiple counter nodes:

   ```bash
   go run part3/cmd/counter/main.go --addr=localhost:9001 --http-port=8011
   go run part3/cmd/counter/main.go --addr=localhost:9002 --http-port=8012
   go run part3/cmd/counter/main.go --addr=localhost:9003 --http-port=8013
   ```

3. Interact with any node to increment or decrement the counter:

   ```bash
   curl http://localhost:8011/increment
   curl http://localhost:8012/decrement
   ```

4. Observe how updates propagate through the system:
   ```bash
   curl http://localhost:8011/counter
   curl http://localhost:8012/counter
   curl http://localhost:8013/counter
   ```

#### Node Configuration Options

Each node can be configured with various options:

```bash
go run part3/cmd/counter/main.go --help
```

Notable options include:

- `--addr`: TCP address for node communication (default: localhost:9000)
- `--discovery`: Discovery server address (default: localhost:8000)
- `--sync`: Gossip sync interval (default: 2s)
- `--max-peers`: Maximum number of peers to sync with (default: 2)
- `--wal`: Enable/disable Write-Ahead Log (default: true)
- `--wal-fsync`: Enable immediate fsync for WAL (default: true)
- `--http-port`: Custom HTTP port for the node's API (default: derived from node port)

## Different Node Configurations

The cluster script by default starts nodes with different configurations to demonstrate how parameter choices affect behavior:

- **Node 1**: Default configuration
- **Node 2**: More aggressive sync (3 peers, 1s interval)
- **Node 3**: No WAL (persistence disabled)
- **Node 4**: WAL without fsync (better performance but less durability)
- **Node 5**: Slower sync (5s interval)

This diversity helps in understanding how different configuration choices impact performance, consistency, and durability in a distributed system.

## Port Configuration

The system uses various ports for different services:

- **Discovery Server**: Port 8000
- **Node TCP Connections**: Ports 9000-9004 (for a 5-node cluster)
- **Node HTTP APIs**: Ports 8010-8014 (for a 5-node cluster)

This separation ensures there are no port conflicts between the discovery service and the nodes' HTTP interfaces.

## Code Structure

### Core Packages

- `assertions`: Runtime assertion utilities for implementing Design by Contract
- `node`: Main node implementation with state management logic
- `peer`: Peer tracking and management
- `protocol`: Message definitions and transport interface
- `discovery`: Centralized discovery service implementation
- `wal`: Write-Ahead Log implementation for durability and recovery

### Commands

- `part3/cmd/counter`: The counter node implementation
- `part3/cmd/discovery`: The discovery server implementation

## WAL Implementation Details

The Write-Ahead Log (WAL) provides:

- **Segmented Storage**: Logs are divided into segments for efficient management
- **CRC Checksums**: Each entry includes a checksum to detect corruption
- **Automatic Recovery**: During startup, the WAL validates and recovers from any corruption
- **Configurable Syncing**: Options for immediate or periodic fsync operations
- **Log Rotation**: When segments reach a configurable size, they are rotated to prevent unbounded growth

## Testing the System

The system includes several mechanisms for validating functionality:

- **Consistency Checking**: Verify that all nodes converge to the same counter value
- **Fault Tolerance Testing**: Nodes can be stopped and restarted to test recovery
- **Automated Simulation**: Run the simulation command to perform random operations and monitor convergence
- **Performance Testing**: Different sync intervals and peer counts can be tested for performance impact

### Testing Fault Tolerance

To test how the system handles node failures:

1. Start the cluster:

   ```bash
   ./run-cluster.sh start
   ```

2. Start the simulation to create background activity:

   ```bash
   ./run-cluster.sh simulation 300 3 2  # Run for 5 minutes, checking every 3s
   ```

3. In another terminal, kill one of the nodes:

   ```bash
   kill $(cat logs/node-2.pid)
   ```

4. Watch how the system adapts to the node failure

5. Restart the failed node:

   ```bash
   ./run-cluster.sh start
   ```

6. Observe how the node recovers and catches up with the current state

## Performance Considerations

Several factors affect the performance and consistency guarantees of the system:

- **Sync Interval**: Shorter intervals increase network traffic but improve convergence time
- **Max Sync Peers**: More peers improve propagation speed but increase network load
- **WAL Fsync**: Immediate fsync provides better durability but impacts performance
- **Network Conditions**: Latency and packet loss affect gossip propagation speed

The management script's simulation mode can help evaluate these tradeoffs in your specific environment.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
