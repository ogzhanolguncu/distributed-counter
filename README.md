# Building a Distributed Counter with Gossip Protocol

This repository contains a step-by-step implementation of a distributed counter system using gossip protocol. The project demonstrates core concepts in distributed systems including peer-to-peer communication, eventual consistency, and fault tolerance.

## System Architecture

```
                GOSSIP PROTOCOL ARCHITECTURE
                ===========================

+---------------+             +---------------+             +---------------+
|    Node 1     |             |    Node 2     |             |    Node 3     |
|---------------|             |---------------|             |---------------|
| Counter: 42   |<-PULL/PUSH->| Counter: 42   |<-PULL/PUSH->| Counter: 42   |
| Version: 5    |    Data     | Version: 5    |    Data     | Version: 5    |
| PeerManager   |  Exchange   | PeerManager   |  Exchange   | PeerManager   |
| TCPTransport  |             | TCPTransport  |             | TCPTransport  |
| WAL (optional)|             | WAL (optional)|             | WAL (optional)|
+---------------+     |       +---------------+     |       +---------------+
      ^   |           |             ^   |           |             ^   |
      |   | HTTP      |             |   | HTTP      |             |   | HTTP
      |   v           |             |   v           |             |   v
+---------------+     |       +---------------+     |       +---------------+
|DiscoveryClient|     |       |DiscoveryClient|     |       |DiscoveryClient|
+---------------+     |       +---------------+     |       +---------------+
      |   ^           |             |   ^           |             |   ^
      |   | Register  |             |   | Register  |             |   | Register
      |   | Heartbeat |             |   | Heartbeat |             |   | Heartbeat
      |   | GetPeers  |             |   | GetPeers  |             |   | GetPeers
      v   |           v             v   |           v             v   |
+-----------------------------------------------------------------------------+
|                          Discovery Server                                   |
|-----------------------------------------------------------------------------|
| - knownPeers: {"node1": {LastSeen}, "node2": {LastSeen}, "node3": {...}}   |
| - Endpoints:                                                                |
|   * POST /register - Nodes register themselves                              |
|   * POST /heartbeat - Nodes indicate they're alive                          |
|   * GET /peers - Nodes discover other nodes                                 |
+-----------------------------------------------------------------------------+
```

## Series Overview

This blog series walks through building a complete distributed counter system from scratch:

- **Part 0: Introduction to Distributed Counter with Gossip Protocol**
- **Part 1: TCP and Network Layer**
- **Part 2: Peer Discovery Service**
- **Part 3: Consistency & Recoverability**

## Core Components

```
COMPONENT RELATIONSHIPS
======================

+-------------+
| Node        |<-------------+
|-------------|             |
| - state     |             |
| - eventLoop |        uses |
+-------------+             |
      | uses               \|/
      |              +-----------+
      |              | Transport |
      |              +-----------+
      |                    |
      |                    | implements
      |                    |
      |              +-----------+
      |              | TCP       |
      v              +-----------+
+-------------+
| PeerManager |
+-------------+
      |
      | uses
      v
+-------------+            +-------------+
| Discovery   |<---------->| WAL         |
| Client      |            | (Optional)  |
+-------------+            +-------------+
```

### Key Features

- **Eventual Consistency**: The system prioritizes availability and partition tolerance, achieving consistency through gossip protocol.
- **Anti-Entropy Mechanism**: Periodic state synchronization to prevent data drift.
- **Peer Discovery**: Centralized discovery service for node registration and peer lookup.
- **Fault Tolerance**: The system continues to function despite node failures.
- **Durability & Recovery**: Write-Ahead Log ensures operations can be recovered after restarts or crashes.

## Data Exchange Formats

```
MESSAGE FORMATS
==============

1. Node-to-Node (Binary TCP):
   +------+----------+-----------------+
   | Type | Version  | Counter         |
   | 1B   | 4B       | 8B              |
   +------+----------+-----------------+
   Types: 0x01=PULL, 0x02=PUSH

2. Node-to-Discovery (HTTP JSON):
   Register/Heartbeat:
   {
     "addr": "node1:port"
   }

   Peers Response:
   [
     {"addr": "node1:port"},
     {"addr": "node2:port"},
     ...
   ]
```

## Gossip Protocol Flow

```
GOSSIP PROTOCOL FLOW
===================

  ┌─────────┐                         ┌─────────┐
  │ Node A  │                         │ Node B  │
  │ v2, c=5 │                         │ v1, c=3 │
  └────┬────┘                         └────┬────┘
       │                                   │
       │    1. PULL (v=2, counter=5)       │
       │ ──────────────────────────────>   │
       │                                   │
       │    2. PUSH (v=2, counter=5)       │
       │ <──────────────────────────────   │
       │                                   │
       │                                   │  Node B updates:
       │                                   │  - version: 1 → 2
       │                                   │  - counter: 3 → 5
       │                                   │
       │    3. Node B broadcasts update    │
       │                                   │
  ┌────┴────┐                         ┌────┴────┐
  │ Node A  │                         │ Node B  │
  │ v2, c=5 │                         │ v2, c=5 │
  └─────────┘                         └─────────┘
       │                                   │
       │                                   ▼
       │                             ┌─────────┐
       │                             │ Node C  │
       │                             │ v2, c=5 │
       │                             └─────────┘
```

## Write-Ahead Log (WAL)

```
WAL STRUCTURE
============

┌───────────────┐
│ Segment File  │
├───────────────┤
│ ┌───────────┐ │
│ │ Entry 1   │ │  Entry Format:
│ ├───────────┤ │  ┌────────────┬────────┬─────┬──────┐
│ │ Entry 2   │ │  │ Seq Number │ Length │ CRC │ Data │
│ ├───────────┤ │  │ (8 bytes)  │ (4B)   │ (4B)│ (var)│
│ │ Entry 3   │ │  └────────────┴────────┴─────┴──────┘
│ ├───────────┤ │
│ │ ...       │ │
│ └───────────┘ │
└───────────────┘
```

## Running the System

### Using the Management Script

```
SYSTEM DEPLOYMENT
================

┌────────────────┐
│ run-cluster.sh │
└───────┬────────┘
        │
        ▼
┌───────────────────┐
│ Discovery Server  │
│ (Port 8000)       │
└─────────┬─────────┘
    ┌─────┴─────┐
    ▼           ▼
┌─────────┐ ┌─────────┐
│ Node 1  │ │ Node 2  │ ...
│ (9000)  │ │ (9001)  │
└─────────┘ └─────────┘
    │           │
    └─────┬─────┘
          ▼
┌─────────────────┐
│ HTTP APIs       │
│ (8010-8014)     │
└─────────────────┘
```

Start a complete cluster with 5 nodes:

```bash
./run-cluster.sh start
```

Interact with the cluster:

```bash
./run-cluster.sh increment  # Increment on a random node
./run-cluster.sh dec 2      # Decrement on node 2
./run-cluster.sh counter    # Get counter values from all nodes
./run-cluster.sh peers      # View peer connections
./run-cluster.sh simulation # Run an automated convergence test
```
