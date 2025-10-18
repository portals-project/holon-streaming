# HOLON Streaming System Wiki

A comprehensive guide to the Holon decentralized exactly-once streaming platform powered by Conflict-free Replicated Data Types (CRDTs).

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [CRDT System](#crdt-system)
5. [Deployment Scenarios](#deployment-scenarios)
6. [Development Guide](#development-guide)
7. [Configuration](#configuration)
8. [Examples and Use Cases](#examples-and-use-cases)
9. [Troubleshooting](#troubleshooting)
10. [Quick Start](#quick-start)

## System Overview

### What is Holon?

Holon Streaming is a decentralized exactly-once streaming platform inspired by the philosophical concept of a "holon" - something that is simultaneously a whole in itself and a part of a larger whole. The system provides:

- **Decentralized Processing**: No single point of failure
- **Exactly-Once Semantics**: Guaranteed through CRDTs
- **Fault Tolerance**: Automatic recovery and partition redistribution
- **Work Stealing**: Dynamic load balancing
- **Performance Comparison**: Built-in benchmarking against Apache Flink

### Key Features

- **CRDT-Powered**: Uses Conflict-free Replicated Data Types for consistency
- **Kafka-Based**: Built on Apache Kafka for message streaming
- **Scalable**: Supports multiple nodes with dynamic partition assignment
- **Recovery**: Automatic failure detection and state recovery
- **Cloud Integration**: Google Cloud Storage and Firestore support

### Technology Stack

- **Language**: Scala 3.3.4
- **Build System**: SBT
- **Message Broker**: Apache Kafka
- **CRDT Library**: Apache Pekko (formerly Akka) Cluster
- **Cloud**: Google Cloud Storage, Firestore
- **Containerization**: Docker, Kubernetes
- **Benchmarking**: Nexmark benchmark suite

## Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Holon Node 0  │    │   Holon Node 1  │    │   Holon Node N  │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │  Recovery   │ │    │ │  Recovery   │ │    │ │  Recovery   │ │
│ │  System     │ │    │ │  System     │ │    │ │  System     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Partition   │ │    │ │ Partition   │ │    │ │ Partition   │ │
│ │ Manager     │ │    │ │ Manager     │ │    │ │ Manager     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ CRDT State  │ │    │ │ CRDT State  │ │    │ │ CRDT State  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Apache Kafka  │
                    │                 │
                    │ ┌─────────────┐ │
                    │ │   Input     │ │
                    │ │   Topic     │ │
                    │ └─────────────┘ │
                    │ ┌─────────────┐ │
                    │ │  Broadcast  │ │
                    │ │   Topic     │ │
                    │ └─────────────┘ │
                    │ ┌─────────────┐ │
                    │ │  Control    │ │
                    │ │   Topic     │ │
                    │ └─────────────┘ │
                    │ ┌─────────────┐ │
                    │ │  Output     │ │
                    │ │   Topic     │ │
                    │ └─────────────┘ │
                    └─────────────────┘
```

### Component Relationships

The system consists of several key components that work together:

1. **Holon Nodes**: Main processing units that handle stream processing
2. **Recovery System**: Manages failure detection and recovery
3. **Partition Management**: Handles ownership and load balancing
4. **CRDT System**: Ensures consistency through conflict-free data structures
5. **Message System**: Kafka-based communication between nodes

### Data Flow

1. **Input**: Events are produced to Kafka input topic
2. **Partitioning**: Events are partitioned across nodes
3. **Processing**: Each node processes its assigned partitions using CRDTs
4. **Broadcasting**: CRDT updates are broadcast to all nodes
5. **Output**: Results are written to output topic

## Core Components

### Holon Nodes

**File**: [`src/main/scala/holon/Holon.scala`](src/main/scala/holon/Holon.scala)

The main interface for Holon nodes. Each node is identified by a unique ID and manages:

- Job submission and updates
- Partition ownership
- Lifecycle management

```scala
trait Holon {
  def submitOrUpdate(job: Job): Unit
  def stop(): Unit
  def partitions(): List[Int]
}
```

### Recovery System

**File**: [`src/main/scala/holon/backend/Recovery.scala`](src/main/scala/holon/backend/Recovery.scala)

The core recovery system handles:

- **Failure Detection**: Monitors node health through heartbeats
- **Partition Redistribution**: Reassigns partitions when nodes fail
- **State Recovery**: Restores state from checkpoints
- **Work Stealing**: Dynamic load balancing

Key features:
- Heartbeat-based failure detection
- Automatic partition ownership transfer
- Checkpoint-based state recovery
- Work stealing for load balancing

### Partition Management

**File**: [`src/main/scala/holon/backend/PartitionOwnershipManager.scala`](src/main/scala/holon/backend/PartitionOwnershipManager.scala)

Manages partition ownership across nodes:

- **Ownership Tracking**: Maintains which node owns which partitions
- **Ownership Transfer**: Handles partition reassignment
- **Version Control**: Uses versioning to resolve ownership conflicts
- **Failure Recovery**: Redistributes partitions after node failures

### Failure Detection

**File**: [`src/main/scala/holon/backend/FailureDetector.scala`](src/main/scala/holon/backend/FailureDetector.scala)

Implements distributed failure detection:

- **Heartbeat System**: Regular heartbeats between nodes
- **Timeout Detection**: Identifies failed nodes based on missing heartbeats
- **Failure Notification**: Alerts the recovery system of failures

### Kafka System

**File**: [`src/main/scala/holon/backend/KafkaSystem.scala`](src/main/scala/holon/backend/KafkaSystem.scala)

Manages Kafka infrastructure:

- **Topic Creation**: Sets up required Kafka topics
- **Partition Configuration**: Configures topic partitions
- **Embedded Kafka**: Uses embedded Kafka for local development

## CRDT System

### CRDT Wrapper Interface

**File**: [`src/main/scala/holon/crdt/CRDTWrapper.scala`](src/main/scala/holon/crdt/CRDTWrapper.scala)

The base interface for all CRDT implementations:

```scala
trait CRDTWrapper[T, V] {
  type EventType
  def checkType(tsEvent: Any): Option[EventType]
  def timeStamp(event: EventType): Long
  def empty(address: SelfUniqueAddress): T
  def update(crdt: T, address: SelfUniqueAddress, delta: EventType): T
  def updateWithDelta(crdt: T, addr: SelfUniqueAddress, e: EventType): (T, Option[ReplicatedDelta])
  def merge(a: T, b: T): T
  def mergeDelta(crdt: T, delta: ReplicatedDelta): T
  def value(crdt: T): V
}
```

### CRDT Implementations

#### PassThroughDeltaWrapper

**File** [`src\main\scala\holon\crdt\PassThroughDeltaWrapper.scala`](src\main\scala\holon\crdt\PassThroughDeltaWrapper.scala)

Does not perform any action, passes very event without updating state:

- **Purpose**: Nexmark Q0 query - pass through performance measure
- **CRDT Type**: GCounter
- **Event Type**: Nexmark.Events.Bid
- **Logic**: No logic, only pass through

#### AuctionToHighestBidWrapper

**File**: [`src/main/scala/holon/crdt/AuctionToHighestBidWrapper.scala`](src/main/scala/holon/crdt/AuctionToHighestBidWrapper.scala)

Tracks the highest bid for each auction using a GSet:

- **Purpose**: Nexmark Q4 query - find highest bid per auction
- **CRDT Type**: GSet[(Long, Long)] (auction ID → price)
- **Event Type**: Nexmark.Events.Bid
- **Logic**: Only adds bids that are higher than current highest

#### AuctionToCategoryWrapper

**File**: [`src/main/scala/holon/crdt/AuctionToCategoryWrapper.scala`](src/main/scala/holon/crdt/AuctionToCategoryWrapper.scala)

Maps auctions to their categories:

- **Purpose**: Nexmark Q4 query - auction categorization
- **CRDT Type**: GSet[(Long, Long)] (auction ID → category)
- **Event Type**: Nexmark.Events.Auction
- **Logic**: Simple add operation for auction-category pairs

#### TQ2LWWMapWrapper

**File**: [`src/main/scala/holon/crdt/TQ2LWWMapWrapper.scala`](src/main/scala/holon/crdt/TQ2LWWMapWrapper.scala)

Tracks highest value taxi trips using LWWMap:

- **Purpose**: Taxi query TQ2 - find highest value trip
- **CRDT Type**: LWWMap[String, Array[Byte]]
- **Event Type**: TaxiProducer.Events.TaxiTripEvent
- **Logic**: Custom clock using trip amount as timestamp

#### HighestBidLWWRegisterWrapper

**File**: [`src/main/scala/holon/crdt/HighestBidLWWRegisterWrapper.scala`](src/main/scala/holon/crdt/HighestBidLWWRegisterWrapper.scala)

Tracks the single highest bid across all auctions:

- **Purpose**: Nexmark Q7 query - global highest bid
- **CRDT Type**: LWWMap[String, Array[Byte]]
- **Event Type**: Nexmark.Events.Bid
- **Logic**: Custom clock using bid price as timestamp

## Deployment Scenarios

The Holon system can be deployed in multiple ways depending on your needs, from local development to large-scale production experiments. Here are all the available deployment options:

### 1. Local Development (Direct SBT)

**Best for**: Development, debugging, and quick testing

Run the system directly using SBT without Docker:

```bash
# Compile the project
sbt compile

# Run a specific query (adjust Config.scala for behavior)
sbt "runMain holon.example.nexmark.Query"
```

**Configuration**: Modify [`src/main/scala/holon/Config.scala`](src/main/scala/holon/Config.scala) to adjust:
- Number of nodes (`N_NODES`)
- Partitions per node (`PARTITIONS_PER_NODE`)
- Query type (`WORKLOAD`: 0=Q0, 4=Q4, 7=Q7)
- Performance parameters

### 2. Local Docker Deployment

**Directory**: [`examples/nexmark-local/`](examples/nexmark-local/)

**Best for**: Local testing with containerized environment

```bash
cd examples/nexmark-local
docker compose up --build
```

**Configuration**: Modify [`examples/nexmark-local/docker-compose.yml`](examples/nexmark-local/docker-compose.yml) to adjust:
- Number of Holon nodes
- Kafka configuration
- Environment variables
- Volume mounts

**Features**:
- 5 Holon nodes by default
- Embedded Kafka with Kafka UI
- Local volume mounts for logs
- Easy scaling by adding more node services

### 3. Remote Docker Deployment (KTH Server)

**Directory**: [`examples/nexmark-remote/`](examples/nexmark-remote/)

**Best for**: Production-like testing on dedicated hardware

**Setup** (first time only):
```bash
# 1. SSH access setup
ssh jonas_spenger@croaker.eecs.kth.se

# 2. Generate SSH key (if needed)
ssh-keygen -t ed25519
ssh-copy-id jonas_spenger@croaker.eecs.kth.se

# 3. Create Docker context
docker context create remote-docker --docker "host=ssh://jonas_spenger@croaker.eecs.kth.se"
docker context use remote-docker
```

**Deployment**:
```bash
# Switch to remote context
docker context use remote-docker

# Deploy to remote server
cd examples/nexmark-remote
docker compose up -d --build
```

**Features**:
- Remote host deployment on KTH server
- Persistent volume mounts for logs/snapshots
- Multiple producers for load testing

**Monitoring**:
```bash
# Check container status
docker ps

# View logs
docker logs <container_name>

# Download logs locally
scp jonas_spenger@croaker.eecs.kth.se:/home/jonas_spenger/projects/holon/holon-experiments/logs/output.log ./local-logs/
```

### 4. Remote JAR Deployment (Large Scale)

**Directory**: [`examples/nexmark-remote-java/`](examples/nexmark-remote-java/)

**Best for**: Large-scale experiments with 100+ nodes

**Prerequisites**:
- Compile the entire project to JAR (takes ~20 minutes)
- JAR file must be at: `target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar`

**Build Process**:
```bash
# Build the assembly JAR
sbt assembly

# Verify JAR exists
ls -la target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar
```

**Deployment**:
```bash
cd examples/nexmark-remote-java
docker compose up -d --build
```

**Features**:
- Pre-compiled JAR for faster startup
- Optimized for large-scale experiments
- Mounted JAR file in containers
- Supports 100+ nodes efficiently

### 5. Kubernetes Deployment (outdated!)

**Directory**: [`examples/nexmark/`](examples/nexmark/)

**Best for**: Cloud-native production deployment

```bash
# Build and push images
docker build -t gcr.io/<project-id>/holon-node:latest .
docker push gcr.io/<project-id>/holon-node:latest

# Deploy with Helm
helm install kafka ./kafka-chart
helm install holon ./holon-chart
```

**Features**:
- Kubernetes-native deployment
- Helm charts for configuration
- Google Cloud integration
- Auto-scaling capabilities

### 6. Flink Comparison Deployments

#### Local Flink (Not Recommended)

**Directory**: [`examples/flink-local/`](examples/flink-local/)

```bash
cd examples/flink-local
docker compose up --build
```

**Note**: Not recommended as Flink requires significant resources to run optimally.

#### Remote Flink (Outdated)

**Directory**: [`examples/flink-remote/`](examples/flink-remote/)

**Status**: This deployment is outdated and no longer maintained.

#### Remote Flink Java (Current)

**Directory**: [`examples/flink-remote-java/`](examples/flink-remote-java/)

**Best for**: Performance comparison with Holon

```bash
cd examples/flink-remote-java
bash deploy.sh
```

**Features**:
- Uses pre-compiled JAR files
- Runs Nexmark queries in Flink
- Direct performance comparison with Holon
- Remote deployment on KTH server


## Development Guide

### Building the Project

```bash
# Build the project
sbt compile

# Run tests
sbt test

# Create assembly JAR (Needed for large scale testing)
sbt assembly

# Build Docker images
docker build -t holon-node .
```

### Development Workflow

1. **Code Changes**: Make changes to Scala source files
2. **Compilation**: Run `sbt compile` to check for errors
3. **Testing**: Run `sbt test` for unit tests
4. **Local Testing**: Use `examples/nexmark-local/` for quick testing
5. **Integration Testing**: Use `examples/nexmark-remote/` for full testing

### Adding New Queries

1. **Create CRDT Wrapper**: Implement `CRDTWrapper` interface
2. **Create Process Function**: Extend `WindowedQueryFun` (for Delta CRDTs) or `WindowedFullStateQueryFun` (for regular CRDTs)
3. **Create Factory**: Implement `ProcFunFactory`
4. **Update Configuration**: Add query to configuration
5. **Test**: Use local deployment for testing

### Adding New CRDTs

1. **Implement Interface**: Extend `CRDTWrapper[T, V]`
2. **Define Event Types**: Specify which events the CRDT handles
3. **Implement Operations**: Define update, merge, and value operations
4. **Add Serialization**: Create serialization support
5. **Test**: Verify CRDT properties (commutativity, associativity, idempotency)

## Configuration

### Environment Variables

**File**: [`src/main/scala/holon/Config.scala`](src/main/scala/holon/Config.scala)

#### Cluster Configuration

```bash
N_NODES=10                    # Number of Holon nodes
PARTITIONS_PER_NODE=5         # Partitions per node
WORKLOAD=0                    # Query type (0=Q0, 4=Q4, 7=Q7)
```

#### Producer Configuration

```bash
PRODUCER_SLEEP_MS=100         # Sleep between events (ms)
PRODUCER_BATCH_SIZE=1024      # Batch size for producers
PRODUCER_COUNT=50            # Number of producers
EVENTS_PER_SECOND=10000      # Target event rate
WINDOW_LENGTH=10000          # Window length (ms)
```

#### Performance Tuning

```bash
SLEEP_BETWEEN_POLLS=0         # Sleep between Kafka polls
WORK_STEALING_THRESHOLD=10000000  # Threshold for work stealing
HEARTBEAT_INTERVAL=500        # Heartbeat interval (ms)
FAILURE_DETECTION_THRESHOLD=2000000  # Failure detection timeout
CHECKPOINT_INTERVAL=10000    # Checkpoint interval (ms)
```

#### Rate Limiting

```bash
ENABLE_RATE_LIMIT=false      # Enable rate limiting
PROCESSING_RATE_LIMIT=10000  # Events per second limit
```

### Kafka Configuration

#### Topics

- **input**: Raw event stream
- **broadcast**: CRDT updates
- **control**: Control messages (heartbeats, ownership)
- **output**: Query results

#### Channels

- **CHN_INPUT (0x00)**: Input events
- **CHN_BROADCAST (0x01)**: CRDT updates
- **CHN_CONTROL (0x02)**: Control messages
- **CHN_OUTPUT (0x03)**: Output results

### Cloud Storage Configuration

```bash
USE_CLOUD_STORAGE_CHECKPOINTS=false  # Enable cloud checkpoints
GCS_BUCKET_NAME=failure-recovery-dev  # Google Cloud Storage bucket
GC_CREDENTIALS_FILE_PATH=path/to/credentials.json  # Service account key
FIRESTORE_START_KEY=flags_ruben  # Firestore key prefix
```

## Examples and Use Cases

### Nexmark Queries

#### Q0: Pass-Through Query

**Files**:
- [`src/main/scala/holon/backend/Q0ProcessFun.scala`](src/main/scala/holon/backend/Q0ProcessFun.scala)
- [`src/main/scala/holon/example/nexmark/Q0Factory.scala`](src/main/scala/holon/example/nexmark/Q0Factory.scala)

Simple pass-through query for baseline performance testing.

#### Q4: Average Winning Bid per Category

**Files**:
- [`src/main/scala/holon/backend/Q4ProcessFun.scala`](src/main/scala/holon/backend/Q4ProcessFun.scala)
- [`src/main/scala/holon/example/nexmark/Q4Factory.scala`](src/main/scala/holon/example/nexmark/Q4Factory.scala)

Complex aggregation query using multiple CRDTs:
- `AuctionToHighestBidWrapper`: Tracks highest bids
- `AuctionToCategoryWrapper`: Maps auctions to categories

#### Q7: Highest Bid Query

**Files**:
- [`src/main/scala/holon/backend/Q7ProcessFun.scala`](src/main/scala/holon/backend/Q7ProcessFun.scala)
- [`src/main/scala/holon/example/nexmark/Q7Factory.scala`](src/main/scala/holon/example/nexmark/Q7Factory.scala)

Global highest bid tracking using LWWMap.

### Taxi Query (TQ2 - Unfinished)

**Files**:
- [`src/main/scala/holon/backend/TQ2ProcessFun.scala`](src/main/scala/holon/backend/TQ2ProcessFun.scala)
- [`src/main/scala/holon/example/taxi/TQ2Factory.scala`](src/main/scala/holon/example/taxi/TQ2Factory.scala)
- [`src/main/scala/holon/crdt/TQ2LWWMapWrapper.scala`](src/main/scala/holon/crdt/TQ2LWWMapWrapper.scala)

Real-world taxi data processing:
- Tracks highest value taxi trips
- Uses custom clock based on trip amount
- Demonstrates real-world streaming applications

### Custom Query Development

To create a new query:

1. **Define Event Types**: Create case classes for your events
2. **Implement CRDT Wrapper**: Create a new CRDT wrapper
3. **Create Process Function**: Implement the query logic
4. **Create Factory**: Wire everything together
5. **Add Configuration**: Update configuration files

Example structure:

```scala
// 1. Event types
case class MyEvent(id: Long, value: Double, timestamp: Long)

// 2. CRDT wrapper
object MyCRDTWrapper extends CRDTWrapper[LWWMap[String, Array[Byte]], String] {
  // Implementation
}

// 3. Process function
class MyProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]])
  extends WindowedQueryFun(partition, crdts) {
  // Query logic
}

// 4. Factory
class MyFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(MyCRDTWrapper, partition, 0, serializationRW))
    new MyProcessFun(partition, crdts)
  }
}
```

## Troubleshooting

### Common Issues

#### 1. Kafka Connection Issues

**Problem**: Nodes cannot connect to Kafka

**Solutions**:
- Check Kafka is running: `docker ps | grep kafka`
- Verify Kafka configuration in `docker-compose.yml`
- Check network connectivity between containers
- Ensure Kafka topics are created properly

#### 2. Partition Ownership Issues

**Problem**: Nodes not receiving partitions

**Solutions**:
- Check `PartitionOwnershipManager` logs
- Verify `N_NODES` and `PARTITIONS_PER_NODE` configuration
- Ensure all nodes are running before job submission
- Check control channel communication

#### 3. CRDT Convergence Issues

**Problem**: CRDTs not converging across nodes

**Solutions**:
- Verify CRDT implementation follows commutative, associative, idempotent properties
- Check broadcast channel communication
- Ensure all nodes receive CRDT updates
- Verify serialization/deserialization

#### 4. Performance Issues

**Problem**: Low throughput or high latency

**Solutions**:
- Adjust `SLEEP_BETWEEN_POLLS` for higher throughput
- Tune `PRODUCER_BATCH_SIZE` for better batching
- Enable rate limiting with `ENABLE_RATE_LIMIT=true`
- Check Kafka partition distribution
- Monitor work stealing behavior

### Debugging Techniques

#### 1. Enable Debug Logging

```scala
Logger.setLevel("Recovery", "DEBUG")
Logger.setLevel("PartitionOwnershipManager", "DEBUG")
Logger.setLevel("FailureDetector", "DEBUG")
```

#### 2. Monitor Kafka Topics

```bash
# Check topic contents
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic input --from-beginning

# Monitor partition assignments
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic input
```

#### 3. Check Node Health

```bash
# View node logs
docker logs holon-node-0
docker logs holon-node-1

# Check partition ownership
# Look for "Node X is responsible for partitions: [...]" in logs
```

#### 4. Performance Monitoring

```bash
# Monitor processing rates
grep "ProcessRate" logs/output.log

# Check work stealing
grep "work steal" logs/output.log

# Monitor CRDT updates
grep "CRDT update" logs/output.log
```

### Performance Optimization

#### 1. Tuning Parameters

```bash
# For high throughput
SLEEP_BETWEEN_POLLS=0
PRODUCER_BATCH_SIZE=4096
ENABLE_RATE_LIMIT=false

# For low latency
SLEEP_BETWEEN_POLLS=1
PRODUCER_BATCH_SIZE=1024
ENABLE_RATE_LIMIT=true
PROCESSING_RATE_LIMIT=5000
```

#### 2. Kafka Optimization

```yaml
# In docker-compose.yml
environment:
  KAFKA_CFG_NUM_PARTITIONS: "50"  # Increase partitions
  KAFKA_CFG_REPLICATION_FACTOR: "1"  # Single replica for performance
```

#### 3. Memory Tuning

```bash
# JVM options in build.sbt
javaOptions ++= Seq(
  "-Xmx4g",           # Increase heap size
  "-XX:+UseG1GC",     # Use G1 garbage collector
  "-XX:MaxGCPauseMillis=200"  # Target GC pause time
)
```

## Quick Start

### 1. Local Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd holon-streaming-clone

# Build the project
sbt compile

# Start local development environment
cd examples/nexmark-local
docker compose up --build
```

### 2. Run a Simple Query

```bash
# Set environment variables
export N_NODES=3
export PARTITIONS_PER_NODE=2
export WORKLOAD=0  # Q0 query

# Start the system
docker compose up -d
```

### 3. Monitor the System

- **Kafka UI**: http://localhost:8080
- **Logs**: `docker logs holon-node-0`
- **Output**: Check the output consumer logs

### 4. Run Performance Tests

```bash
# Remote deployment
cd examples/nexmark-remote
docker context create remote-docker --docker "host=ssh://user@remote-host"
docker context use remote-docker
docker compose up -d --build
```

### 5. Compare with Flink

```bash
# Start Flink comparison
cd examples/flink-remote
bash deploy.sh

# Compare results
# Check logs in both systems for performance metrics
```

### Key Files to Know

- **Main Entry Point**: [`src/main/scala/holon/example/nexmark/Query.scala`](src/main/scala/holon/example/nexmark/Query.scala)
- **Configuration**: [`src/main/scala/holon/Config.scala`](src/main/scala/holon/Config.scala)
- **Core Recovery**: [`src/main/scala/holon/backend/Recovery.scala`](src/main/scala/holon/backend/Recovery.scala)
- **Local Setup**: [`examples/nexmark-local/docker-compose.yml`](examples/nexmark-local/docker-compose.yml)
- **Build Script**: [`full-build.sh`](full-build.sh)

### Next Steps

1. **Explore Examples**: Run different queries (Q0, Q4, Q7, TQ2)
2. **Modify Configuration**: Adjust node count, partitions, and performance parameters
3. **Add Custom Queries**: Implement your own streaming queries
4. **Performance Tuning**: Optimize for your specific use case
5. **Cloud Deployment**: Deploy to Kubernetes for production use

---
