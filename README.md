# Cassandra 5.0 Java Driver Examples

Educational examples demonstrating Apache Cassandra 5.0 features using the Java Driver 4.17.0. These applications showcase real-world patterns for time-series data and event logging use cases.

## Overview

This repository contains two standalone Java applications that demonstrate Cassandra driver capabilities:

1. **[IoT Time-Series Platform](iot-timeseries/)** - Sensor data ingestion and querying
2. **[Real-Time Event Logging](event-logging/)** - High-throughput event processing with Query Builder API

Both applications are packaged as executable JARs with comprehensive integration tests.

## Features Demonstrated

### Core Driver Features
- Connection management with advanced configuration
- Prepared statements and statement caching
- Asynchronous operations with `CompletionStage`
- Batch statements (logged and unlogged)
- Multiple consistency levels (ONE, QUORUM, LOCAL_QUORUM)
- Retry policies and speculative execution
- Query tracing and performance monitoring

### Cassandra 5.0 Features
- **Storage-Attached Indexes (SAI)** - Advanced indexing capabilities
- **Time Window Compaction Strategy (TWCS)** - Optimized for time-series data
- **Lightweight Transactions (LWT)** - Conditional updates with IF NOT EXISTS/IF EXISTS
- **Counter columns** - Atomic increment operations
- **Collections** - LIST, SET, MAP support including frozen collections
- **User-Defined Types (UDT)** - Complex data types
- **TTL** - Automatic data expiration

### Data Modeling Patterns
- Time-series partitioning strategies
- Static columns for denormalization
- Counter tables for aggregations
- Collection columns for flexible schemas
- Composite partition keys

## Prerequisites

- **Java**: 11 or higher
- **Maven**: 3.6+ (for building from source)
- **Cassandra**: 5.0+ (Docker/Podman or standalone)

## Quick Start

### 1. Start Cassandra

Using Docker:
```bash
docker run -d --name cassandra -p 9042:9042 cassandra:5.0
```

Using Podman:
```bash
podman run -d --name cassandra -p 9042:9042 cassandra:5.0
```

Wait for Cassandra to be ready (~30 seconds):
```bash
docker exec cassandra cqlsh -e "SELECT release_version FROM system.local;"
```

### 2. Build the Applications

```bash
mvn clean package
```

This creates executable JARs:
- `iot-timeseries/target/iot-timeseries-1.0-jar-with-dependencies.jar` (15MB)
- `event-logging/target/event-logging-1.0-jar-with-dependencies.jar` (15MB)

### 3. Run the Examples

**IoT Time-Series (Full Demo):**
```bash
java -jar iot-timeseries/target/iot-timeseries-1.0-jar-with-dependencies.jar \
  --mode demo \
  --contact-point 127.0.0.1 \
  --port 9042 \
  --datacenter datacenter1
```

**Event Logging (Full Demo):**
```bash
java -jar event-logging/target/event-logging-1.0-jar-with-dependencies.jar \
  --mode demo \
  --contact-point 127.0.0.1 \
  --port 9042 \
  --datacenter datacenter1
```

## Testing

Run integration tests using Testcontainers (automatically starts Cassandra 5.0 in Docker):

```bash
# Test IoT application
mvn test -pl iot-timeseries

# Test Event Logging application
mvn test -pl event-logging

# Test all
mvn test
```

**Test Results:**
- IoT Time-Series: 8 tests
- Event Logging: 10 tests
- Total: **18 tests passing** ✅

## Project Structure

```
cassandra-examples/
├── pom.xml                          # Parent POM with shared dependencies
├── iot-timeseries/                  # Application 1: IoT Time-Series
│   ├── pom.xml
│   ├── README.md
│   └── src/
│       ├── main/java/com/axonops/iot/
│       │   ├── IotApplication.java          # Main entry point
│       │   ├── SchemaSetup.java            # Keyspace/table creation
│       │   ├── LoadGenerator.java          # Write load generator
│       │   └── QueryDemo.java              # Query demonstrations
│       └── test/java/com/axonops/iot/
│           └── IotIntegrationTest.java     # Integration tests
└── event-logging/                   # Application 2: Event Logging
    ├── pom.xml
    ├── README.md
    └── src/
        ├── main/java/com/axonops/events/
        │   ├── EventLoggingApplication.java # Main entry point
        │   ├── SchemaSetup.java            # Keyspace/table creation
        │   ├── EventGenerator.java         # Event load generator
        │   └── QueryBuilderDemo.java       # Query Builder API examples
        └── test/java/com/axonops/events/
            └── EventLoggingIntegrationTest.java
```

## Configuration Options

Both applications support the following command-line arguments:

| Argument          | Default       | Description                           |
|-------------------|---------------|---------------------------------------|
| `--contact-point` | 127.0.0.1     | Cassandra host address                |
| `--port`          | 9042          | Cassandra native transport port       |
| `--datacenter`    | datacenter1   | Local datacenter name                 |
| `--mode`          | setup         | Operation mode (see below)            |
| `--write-rate`    | 100 (IoT)     | Writes/events per second              |
| `--event-rate`    | 1000 (Events) | Events per second                     |
| `--duration`      | 60            | Duration in seconds for load tests    |

### Operation Modes

**IoT Time-Series:**
- `setup` - Create schema and insert sample data
- `write` - Run load generator
- `read` - Demonstrate query patterns
- `demo` - Run all modes sequentially

**Event Logging:**
- `setup` - Create schema and insert sample data
- `write` - Run event generator
- `query` - Demonstrate Query Builder API
- `demo` - Run all modes sequentially

## Performance Characteristics

### IoT Time-Series
- Sustained write rate: 50-100 writes/sec per core
- P99 read latency: <10ms
- Batch operations: 3 writes per batch
- Async operations with rate limiting

### Event Logging
- Sustained event rate: 100-1000 events/sec
- Query Builder overhead: ~1ms
- Counter updates: Atomic and consistent
- Batch inserts: Configurable batch size

## Educational Focus

These applications are designed for **learning and demonstration**, not production use. The focus is on:

✅ **Clarity**: Simple, readable code with extensive comments
✅ **Completeness**: Comprehensive coverage of driver features
✅ **Best Practices**: Industry-standard patterns and techniques
✅ **KISS Principle**: No frameworks, pure Java

## Resources

- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/latest/)
- [Java Driver Documentation](https://docs.datastax.com/en/developer/java-driver/4.17/)
- [Cassandra 5.0 Release Notes](https://cassandra.apache.org/doc/5.0/)
- [Storage-Attached Indexes (SAI)](https://cassandra.apache.org/doc/latest/cassandra/cql/indexes.html#sai-index)

## License

See [LICENSE](LICENSE) file.

## Contributing

These are educational examples. Feel free to use them as a starting point for your own applications.
