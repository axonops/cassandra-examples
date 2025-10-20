# IoT Time-Series Platform

Educational application demonstrating time-series data patterns with Apache Cassandra 5.0 and the Java Driver 4.17.0.

## Overview

This application simulates an IoT platform for sensor data collection, storage, and querying. It demonstrates best practices for handling time-series data at scale with Cassandra.

## Features Demonstrated

### Schema Design
- **Time-series partitioning** - Daily buckets to prevent large partitions
- **TWCS compaction** - Optimized for time-windowed data
- **Static columns** - Sensor metadata denormalization
- **Counter tables** - Hourly aggregations
- **Collections** - Alert metadata with LIST, SET, MAP
- **TTL** - Automatic data expiration (30 days for sensor data)

### Driver Features
- **Prepared statements** - Cached for performance
- **Async operations** - Non-blocking writes with `CompletionStage`
- **Batch statements** - Logged and unlogged batches
- **Consistency levels** - ONE, QUORUM, LOCAL_QUORUM demonstrations
- **LWT** - Conditional sensor registration with IF NOT EXISTS
- **Pagination** - Handling large result sets
- **Query tracing** - Performance analysis

### Cassandra 5.0 Features
- **SAI indexes** - Storage-Attached Index on temperature column
- **Secondary indexes** - Traditional index on alert_type
- **TimeUUID** - For sensor registration ordering

## Data Model

### sensor_data
Main time-series table with daily partitions:
```cql
CREATE TABLE sensor_data (
    sensor_id text,
    date_bucket text,           -- YYYY-MM-DD
    timestamp timestamp,
    temperature double,
    humidity double,
    pressure double,
    battery_level int,
    reading_json text,          -- JSON for flexible payloads
    PRIMARY KEY ((sensor_id, date_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', ...}
  AND default_time_to_live = 2592000;  -- 30 days
```

**Partition Key**: `(sensor_id, date_bucket)` - Ensures manageable partition sizes
**Clustering Key**: `timestamp DESC` - Latest readings first

### sensor_metadata
Sensor registration with static columns:
```cql
CREATE TABLE sensor_metadata (
    sensor_id text,
    registration_id timeuuid,
    sensor_type text static,    -- Static: same for all registrations
    location text static,
    latitude double static,
    longitude double static,
    registration_time timestamp,
    config map<text, text>,
    PRIMARY KEY (sensor_id, registration_id)
) WITH CLUSTERING ORDER BY (registration_id DESC);
```

### hourly_aggregates
Counter table for statistics:
```cql
CREATE TABLE hourly_aggregates (
    sensor_id text,
    hour_bucket text,           -- YYYY-MM-DD-HH
    reading_count counter,
    alert_count counter,
    PRIMARY KEY (sensor_id, hour_bucket)
);
```

### alert_events
Alerts with collection columns:
```cql
CREATE TABLE alert_events (
    sensor_id text,
    alert_time timestamp,
    alert_type text,
    severity int,
    message text,
    affected_metrics list<text>,  -- Ordered list
    tags set<text>,               -- Unique tags
    metadata map<text, text>,     -- Key-value pairs
    PRIMARY KEY (sensor_id, alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);
```

## Usage

### Prerequisites

Start Cassandra 5.0:
```bash
docker run -d --name cassandra -p 9042:9042 cassandra:5.0
```

Build the application:
```bash
mvn clean package -pl iot-timeseries
```

### Running the Application

**1. Setup Mode** - Create schema and insert sample data:
```bash
java -jar target/iot-timeseries-1.0-jar-with-dependencies.jar \
  --mode setup \
  --contact-point 127.0.0.1 \
  --port 9042 \
  --datacenter datacenter1
```

This creates:
- Keyspace `iot_demo` with RF=1
- 4 tables (sensor_data, sensor_metadata, hourly_aggregates, alert_events)
- SAI index on temperature
- Secondary index on alert_type
- 5 sample sensors

**2. Write Mode** - Generate sensor data:
```bash
java -jar target/iot-timeseries-1.0-jar-with-dependencies.jar \
  --mode write \
  --contact-point 127.0.0.1 \
  --write-rate 100 \
  --duration 60
```

Options:
- `--write-rate`: Writes per second (default: 100)
- `--duration`: Duration in seconds (default: 60)

Generates:
- Sensor readings for 5 sensors
- Temperature, humidity, pressure, battery level
- Automatic counter updates
- 10% of writes use batch statements (3 readings per batch)

**3. Read Mode** - Demonstrate queries:
```bash
java -jar target/iot-timeseries-1.0-jar-with-dependencies.jar \
  --mode read \
  --contact-point 127.0.0.1
```

Demonstrates:
- Simple queries with prepared statements
- Range queries for time-series data
- Pagination for large result sets
- Async query execution
- Different consistency levels (ONE, QUORUM, LOCAL_QUORUM)
- Counter reads
- Collection queries (LIST, SET, MAP)
- Query tracing with execution info

**4. Demo Mode** - Run all modes sequentially:
```bash
java -jar target/iot-timeseries-1.0-jar-with-dependencies.jar \
  --mode demo \
  --contact-point 127.0.0.1 \
  --write-rate 100 \
  --duration 30
```

### Configuration Options

| Argument          | Default       | Description                        |
|-------------------|---------------|------------------------------------|
| `--contact-point` | 127.0.0.1     | Cassandra host address             |
| `--port`          | 9042          | Cassandra native transport port    |
| `--datacenter`    | datacenter1   | Local datacenter name              |
| `--mode`          | setup         | Operation mode                     |
| `--write-rate`    | 100           | Writes per second                  |
| `--duration`      | 60            | Duration in seconds for write mode |

## Performance Characteristics

### Write Performance
- **Sustained rate**: 50-100 writes/sec per core
- **Batch efficiency**: 3 readings per batch (10% of writes)
- **Async operations**: Non-blocking with rate limiting
- **Counter updates**: Atomic increments per write

### Read Performance
- **Simple queries**: <5ms p99 latency
- **Range queries**: 10-50ms for 10-100 rows
- **Pagination**: 5 rows per page (configurable)
- **Async queries**: Parallel execution

### Load Generator Metrics
The load generator reports:
- Total writes completed
- Total errors
- Average write rate
- Error rate percentage
- Real-time metrics every 5 seconds

Example output:
```
Metrics - Writes: 300, Errors: 0, Error Rate: 0.00%
=== Final Metrics ===
Duration: 5 seconds
Total Writes: 300
Total Errors: 0
Average Write Rate: 60 writes/sec
Error Rate: 0.00%
```

## Code Structure

```
src/main/java/com/axonops/iot/
├── IotApplication.java       # Main entry point, CLI parsing
├── SchemaSetup.java         # Keyspace and table creation
├── LoadGenerator.java       # Async write load generator
└── QueryDemo.java           # Query pattern demonstrations

src/test/java/com/axonops/iot/
└── IotIntegrationTest.java  # Testcontainers integration tests
```

## Integration Tests

Run tests with Testcontainers (auto-starts Cassandra 5.0):
```bash
mvn test -pl iot-timeseries
```

**Test Coverage** (8 tests):
1. Schema creation validation
2. Sensor data insertion and retrieval
3. Range queries with time filtering
4. Counter updates and reads
5. Lightweight transactions (LWT)
6. Async operations
7. Load generator functionality
8. Collection queries (LIST, SET, MAP)

## Query Examples

### Insert Sensor Reading (Prepared Statement)
```java
PreparedStatement prepared = session.prepare(
    "INSERT INTO sensor_data " +
    "(sensor_id, date_bucket, timestamp, temperature, humidity, pressure, " +
    "battery_level, reading_json) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
    "USING TTL ?");

BoundStatement statement = prepared.bind(
    "sensor-1",
    "2025-10-20",
    Instant.now(),
    25.5,
    60.0,
    1013.25,
    85,
    "{\"location\":\"lab\"}",
    86400  // 24 hour TTL
);

session.execute(statement);
```

### Range Query for Time-Series Data
```java
PreparedStatement prepared = session.prepare(
    "SELECT timestamp, temperature, humidity, pressure " +
    "FROM sensor_data " +
    "WHERE sensor_id = ? AND date_bucket = ? " +
    "AND timestamp >= ? AND timestamp <= ?");

ResultSet rs = session.execute(prepared.bind(
    "sensor-1",
    "2025-10-20",
    rangeStart,
    rangeEnd
));
```

### Async Batch Insert
```java
BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
    .addStatement(insertStmt1)
    .addStatement(insertStmt2)
    .addStatement(insertStmt3)
    .build();

CompletionStage<AsyncResultSet> future = session.executeAsync(batch);
future.whenComplete((rs, error) -> {
    if (error == null) {
        // Success
    } else {
        // Handle error
    }
});
```

## Best Practices Demonstrated

1. **Partition Size Management**: Daily buckets prevent unbounded growth
2. **Prepared Statements**: Cached and reused for performance
3. **Async Writes**: Non-blocking for high throughput
4. **Appropriate Batches**: Unlogged batches for same-partition writes
5. **TTL Usage**: Automatic data expiration
6. **Counter Tables**: Separate table for aggregations
7. **Error Handling**: Comprehensive exception handling
8. **Metrics**: Real-time performance monitoring

## Learning Resources

- [Time-Series Data Modeling](https://cassandra.apache.org/doc/latest/cassandra/data-modeling/data-modeling-time-series.html)
- [TWCS Compaction](https://cassandra.apache.org/doc/latest/cassandra/operating/compaction/twcs.html)
- [Prepared Statements](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/statements/prepared/)
- [Async Programming](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/async/)

## Troubleshooting

**Connection refused**:
- Ensure Cassandra is running and accessible
- Wait 30-60 seconds after starting Cassandra

**ReplicationFactor error**:
- Default RF=1 for single-node setups
- Increase RF in `SchemaSetup.java` for multi-node clusters

**High error rate**:
- Reduce `--write-rate` parameter
- Check Cassandra logs for errors
- Verify system resources (CPU, memory, disk)
