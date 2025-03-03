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
  Ensuring data consistency across the distributed system and handling node failures.

## System Architecture

Our distributed counter system consists of the following components:

### Core Components

1. **Node**: The primary entity in our system that maintains counter state and communicates with peers.
2. **Transport Layer**: Handles TCP communication between nodes.
3. **Peer Management**: Tracks and manages connections to other nodes in the system.
4. **Discovery Service**: Helps nodes find and connect to each other.
5. **Protocol**: Defines message structures for communication between nodes.

### Key Features

- **Eventual Consistency**: The system prioritizes availability and partition tolerance, achieving consistency through gossip protocol.
- **Anti-Entropy Mechanism**: Periodic state synchronization to prevent data drift.
- **Peer Discovery**: Centralized discovery service for node registration and peer lookup.
- **Fault Tolerance**: The system continues to function despite node failures.

## Implementation Details

This project implements Design by Contract (DBC) principles throughout the codebase, using assertions to verify preconditions, postconditions, and invariants. The `assertions` package provides functions like `Assert`, `AssertEqual`, and `AssertNotNil` that make the code more robust and self-documenting while catching errors early during development.

### Gossip Protocol

The system uses a gossip protocol for distributing counter updates across the network:

- Each node maintains its own version of the distributed counter
- When a node's counter changes, it "gossips" the update to a subset of peers
- Updates propagate throughout the network through subsequent gossip rounds
- Nodes use versioning to determine which updates to apply

### Message Types

The protocol defines two primary message types:

- **PULL**: Requests the current state from another node
- **PUSH**: Sends the current state to another node

### Anti-Entropy Process

To prevent data drift and ensure eventual consistency:

- Nodes periodically initiate sync rounds with random peers
- Version numbers are used to determine the most recent state
- Higher versions always override lower versions

## Getting Started

- Go 1.22 or higher
- Basic understanding of distributed systems concepts

### Running the System

1. Start the discovery server:

   ```
   go run cmd/discovery/main.go
   ```

2. Start multiple counter nodes:

   ```
   go run cmd/counter/main.go -addr=localhost:8001
   go run cmd/counter/main.go -addr=localhost:8002
   go run cmd/counter/main.go -addr=localhost:8003
   ```

3. Interact with any node to increment or decrement the counter:

   ```
   curl http://localhost:8001/increment
   curl http://localhost:8002/decrement
   ```

4. Observe how updates propagate through the system:
   ```
   curl http://localhost:8001/counter
   curl http://localhost:8002/counter
   curl http://localhost:8003/counter
   ```

## Code Structure

### Core Packages

- `assertions`: Runtime assertion utilities for implementing Design by Contract
- `node`: Main node implementation with state management logic
- `peer`: Peer tracking and management
- `protocol`: Message definitions and transport interface
- `discovery`: Centralized discovery service implementation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
