# Distributed Counter with Gossip Protocol

This repository contains the companion code for the blog post series "Implementing Distributed Counter" at [ogzhanolguncu.com](https://ogzhanolguncu.com/blog/implementing-distributed-counter-part-0/).

## Project Overview

This project demonstrates how to build a distributed counter system in Go, using CRDTs (Conflict-free Replicated Data Types) with a gossip protocol for state synchronization and a service discovery mechanism to enable dynamic node discovery.

### Key Features

- **PNCounter CRDT**: Uses a Positive-Negative Counter CRDT to ensure eventual consistency across distributed nodes
- **Gossip Protocol**: Implements an efficient anti-entropy gossip protocol with digest-based synchronization to minimize network traffic
- **Service Discovery**: Includes a HTTP-based discovery service to dynamically register and discover peers
- **Failure Detection**: Contains mechanisms to detect and handle node failures
- **Network Partitioning Resilience**: Handles network partitions and automatically recovers when connectivity is restored

## Architecture

```
              DISTRIBUTED COUNTER ARCHITECTURE (Final)
              ========================================
                      +----------------+
                      |    Client      |
                      +--------+-------+
                               |
                               | HTTP
                               v
                      +----------------+
                      |  API Gateway   |
                      |----------------|
                      | Load Balancer  |
                      | Reverse Proxy  |
                      |----------------|
                      | Endpoints:     |
                      | GET  /counter  |
                      | POST /increment|
                      | POST /decrement|
                      | GET  /nodes    |
                      | GET  /health   |
                      +--------+-------+
                               |
                               | HTTP
               +-----------------+-----------------+
               |                                   |
               v                                   v
+----------------------+<-- DigestPull/Ack/Push -->+---------------------+
|       Node 1         |     (State Sync)         |       Node 2         |
|----------------------|                          |----------------------|
| PNCounter            |                          | PNCounter            |
|  - Increments: {...} |                          |  - Increments: {...} |
|  - Decrements: {...} |                          |  - Decrements: {...} |
| PeerManager          |                          | PeerManager          |
| TCPTransport         |                          | TCPTransport         |
| WAL                  |                          | WAL                  |
+--------+-------------+                          +--------+-------------+
         ^    |                                             ^    |
         |    | HTTP                                        |    | HTTP
         |    v                                             |    v
+--------+-------------+                          +--------+-------------+
| DiscoveryClient      |                          | DiscoveryClient      |
+----------------------+                          +----------------------+
         |    ^                                             |    ^
         |    | Register                                    |    | Register
         |    | Heartbeat                                   |    | Heartbeat
         |    | GetPeers                                    |    | GetPeers
         v    |                                             v    |
+--------------------------------------------------------------------------+
|                                Discovery Server                          |
|--------------------------------------------------------------------------|
| - knownPeers: {"node1_addr": {LastSeen}, "node2_addr": {LastSeen}}       |
| - Endpoints:                                                             |
|   * POST /register                                                       |
|   * POST /heartbeat                                                      |
|   * GET /peers                                                           |
+--------------------------------------------------------------------------+
```

## Project Structure

The project is organized in several parts, each building upon the previous:

- **[Part 0: CRDT - Implementing a PNCounter](https://ogzhanolguncu.com/blog/implementing-distributed-counter-part-0/)**
- **[Part 1: Node - Structure and In-Memory Communication](https://ogzhanolguncu.com/blog/implementing-distributed-counter-part-1/)**
- **[Part 2: Networking - Peer Management and TCP Transport](https://ogzhanolguncu.com/blog/implementing-distributed-counter-part-2/)** (You are here)
- **Part 3: Finding Peers - The Discovery Service** (Not yet published)
- **Part 4: Adding Persistence - The Write-Ahead Log (WAL)** (Not yet published)
- **Part 5: Finishing Touches - API Gateway** (Not yet published)

## Running the System

### From Source

To run the distributed counter system from source:

```bash
# Navigate to the appropriate part directory
cd part2  # or whichever part you want to run

# Build the project
go build -o counter

# Run the application
./counter
```

### Using Docker

You can run a complete cluster using Docker Compose:

```bash
# Start the cluster with Docker Compose
docker-compose up

# Start in detached mode
docker-compose up -d

# Stop the cluster
docker-compose down
```

This will start a complete system with:

- 1 discovery server
- 5 counter nodes
- 1 API gateway

### Running a Local Cluster

For testing distributed functionality, a convenient script is provided to start a local cluster:

```bash
# Make the script executable
chmod +x run-cluster.sh

# Start the cluster
./run-cluster.sh start

# Check the status of the cluster
./run-cluster.sh status

# Increment the counter on a specific node (node 2)
./run-cluster.sh increment 2
# Or use the shorthand
./run-cluster.sh inc 2

# Decrement the counter on a random node
./run-cluster.sh decrement
# Or use the shorthand
./run-cluster.sh dec

# Get counter values from all nodes
./run-cluster.sh counter
# Or use the shorthand
./run-cluster.sh cnt

# Get peer information from all nodes
./run-cluster.sh peers
# Or use the shorthand
./run-cluster.sh p

# Check if all nodes have consistent counter values
./run-cluster.sh consistency

# View logs from a specific node
./run-cluster.sh logs 1
# View discovery server logs
./run-cluster.sh logs discovery

# Run a simulation (60s duration, checking every 2s, 3 ops per cycle)
./run-cluster.sh simulation 60 2 3

# Stop the cluster
./run-cluster.sh stop

# Display help information
./run-cluster.sh help
```

### Monitoring the Cluster

You can use the provided monitoring script to watch the counter values across nodes:

```bash
# Make the script executable
chmod +x monitor.sh

# Run the monitor (updates every 1 second)
./monitor.sh

# Run with a custom update interval (e.g., 2 seconds)
./monitor.sh 2
```
