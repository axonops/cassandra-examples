# Real-Time Event Logging

Educational application demonstrating high-throughput event logging patterns with Apache Cassandra 5.0 and the Java Driver 4.17.0, with emphasis on the Query Builder API.

## Overview

This application simulates a real-time event logging system for distributed applications. It showcases the Query Builder API for type-safe query construction and demonstrates patterns for handling high-volume event streams.

## Features Demonstrated

### Query Builder API
- **Type-safe queries** - Programmatic query construction
- **SELECT builders** - Complex queries with filtering
- **INSERT builders** - With TTL and timestamp options
- **UPDATE builders** - Including conditional updates
- **DELETE builders** - Range deletes and conditional deletes
- **BATCH builders** - Dynamic batch construction

### Schema Design
- **Event log** - Hourly partitions for high-volume data
- **Counter statistics** - Event counts by category
- **User activity** - Session tracking with TTL
- **Alert rules** - Versioned rules with frozen collections

### Driver Features
- **Prepared statements** - For event inserts
- **Async operations** - Non-blocking event writes
- **Counter updates** - Atomic increments
- **Frozen collections** - Complex nested data types
- **Dynamic attributes** - MAP columns for flexibility
- **Conditional updates** - LWT with IF EXISTS

### Cassandra 5.0 Features
- **SAI indexes** - Storage-Attached Index on severity
- **TWCS compaction** - Hourly time windows
- **TTL** - Session and event expiration

## Data Model

### event_log
Main event storage with hourly partitions:
```cql
CREATE TABLE event_log (
    source_id text,
    hour_bucket text,           -- YYYY-MM-DD-HH
    event_time timestamp,
    event_id uuid,
    severity text,
    category text,
    message text,
    payload_json text,          -- Full event as JSON
    PRIMARY KEY ((source_id, hour_bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS',
                    'compaction_window_size': 1}
  AND default_time_to_live = 604800;  -- 7 days
```

**Partition Key**: `(source_id, hour_bucket)` - Manageable hourly partitions
**Clustering Keys**: `event_time DESC, event_id ASC` - Time-ordered with UUID uniqueness

### event_statistics
Counter table for real-time metrics:
```cql
CREATE TABLE event_statistics (
    category text,
    time_bucket text,           -- YYYY-MM-DD-HH
    event_count counter,
    error_count counter,
    warning_count counter,
    PRIMARY KEY (category, time_bucket)
);
```

### user_activity
Session tracking with dynamic attributes:
```cql
CREATE TABLE user_activity (
    user_id text,
    session_id uuid,
    activity_time timestamp,
    action text,
    attributes map<text, text>, -- Dynamic key-value pairs
    PRIMARY KEY (user_id, session_id, activity_time)
) WITH default_time_to_live = 86400;  -- 24 hour session window
```

### alert_rules
Versioned rules with frozen collections:
```cql
CREATE TABLE alert_rules (
    rule_id uuid,
    version_time timestamp,
    rule_name text,
    enabled boolean,
    conditions frozen<map<text, text>>,  -- Immutable conditions
    actions frozen<list<text>>,          -- Immutable action list
    tags set<text>,
    PRIMARY KEY (rule_id, version_time)
) WITH CLUSTERING ORDER BY (version_time DESC);
```

## Usage

### Prerequisites

Start Cassandra 5.0:
```bash
docker run -d --name cassandra -p 9042:9042 cassandra:5.0
```

Build the application:
```bash
mvn clean package -pl event-logging
```

### Running the Application

**1. Setup Mode** - Create schema and insert sample data:
```bash
java -jar target/event-logging-1.0-jar-with-dependencies.jar \
  --mode setup \
  --contact-point 127.0.0.1 \
  --port 9042 \
  --datacenter datacenter1
```

This creates:
- Keyspace `events_demo` with RF=1
- 4 tables (event_log, event_statistics, user_activity, alert_rules)
- SAI index on severity
- Secondary indexes on category and action
- Sample event data

**2. Write Mode** - Generate events:
```bash
java -jar target/event-logging-1.0-jar-with-dependencies.jar \
  --mode write \
  --contact-point 127.0.0.1 \
  --event-rate 1000 \
  --duration 60
```

Options:
- `--event-rate`: Events per second (default: 1000)
- `--duration`: Duration in seconds (default: 60)

Generates:
- Events from 5 different sources
- Multiple severities (DEBUG, INFO, WARN, ERROR)
- Various categories (application, security, database, network, system)
- Automatic counter statistics updates
- JSON payloads

**3. Query Mode** - Demonstrate Query Builder API:
```bash
java -jar target/event-logging-1.0-jar-with-dependencies.jar \
  --mode query \
  --contact-point 127.0.0.1
```

Demonstrates:
- SELECT with filtering and limits
- INSERT with TTL and custom timestamps
- UPDATE with counter increments
- DELETE with range conditions
- BATCH statement construction
- Conditional updates (IF EXISTS)
- Complex queries with multiple conditions

**4. Demo Mode** - Run all modes sequentially:
```bash
java -jar target/event-logging-1.0-jar-with-dependencies.jar \
  --mode demo \
  --contact-point 127.0.0.1 \
  --event-rate 100 \
  --duration 30
```

### Configuration Options

| Argument          | Default       | Description                        |
|-------------------|---------------|------------------------------------|
| `--contact-point` | 127.0.0.1     | Cassandra host address             |
| `--port`          | 9042          | Cassandra native transport port    |
| `--datacenter`    | datacenter1   | Local datacenter name              |
| `--mode`          | setup         | Operation mode                     |
| `--event-rate`    | 1000          | Events per second                  |
| `--duration`      | 60            | Duration in seconds for write mode |

## Performance Characteristics

### Write Performance
- **Sustained rate**: 100-1000 events/sec
- **Async operations**: Non-blocking event inserts
- **Counter updates**: Atomic statistics per event
- **Batch efficiency**: Dynamic batching for related events

### Query Performance
- **Query Builder overhead**: ~1ms additional latency
- **Simple queries**: <5ms p99 latency
- **Complex queries**: 10-20ms with multiple conditions
- **Counter reads**: <5ms for statistics

### Event Generator Metrics
Example output:
```
Metrics - Events: 3000, Errors: 0, Error Rate: 0.00%
=== Final Metrics ===
Duration: 3 seconds
Total Events: 3000
Total Errors: 0
Average Event Rate: 1000 events/sec
Error Rate: 0.00%
```

## Code Structure

```
src/main/java/com/axonops/events/
├── EventLoggingApplication.java  # Main entry point, CLI parsing
├── SchemaSetup.java             # Keyspace and table creation
├── EventGenerator.java          # Async event generator
└── QueryBuilderDemo.java        # Query Builder API examples

src/test/java/com/axonops/events/
└── EventLoggingIntegrationTest.java  # Testcontainers tests
```

## Integration Tests

Run tests with Testcontainers (auto-starts Cassandra 5.0):
```bash
mvn test -pl event-logging
```

**Test Coverage** (10 tests):
1. Schema creation validation
2. Query Builder SELECT
3. Query Builder INSERT with TTL
4. Query Builder UPDATE with counters
5. Query Builder DELETE
6. Batch statement construction
7. User activity with dynamic attributes (MAP)
8. Frozen collections for alert rules
9. Event generator functionality
10. Conditional updates (IF EXISTS)

## Query Builder Examples

### SELECT with Query Builder
```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

Select select = selectFrom("events_demo", "event_log")
    .all()
    .whereColumn("source_id").isEqualTo(literal("api-server"))
    .whereColumn("hour_bucket").isEqualTo(literal("2025-10-20-17"))
    .whereColumn("event_time").isGreaterThan(literal(rangeStart))
    .limit(10);

ResultSet rs = session.execute(select.build());
```

Generated CQL:
```sql
SELECT * FROM events_demo.event_log
WHERE source_id='api-server'
AND hour_bucket='2025-10-20-17'
AND event_time>'2025-10-20T15:00:00Z'
LIMIT 10
```

### INSERT with TTL and Timestamp
```java
Insert insert = insertInto("events_demo", "event_log")
    .value("source_id", literal("api-server"))
    .value("hour_bucket", literal("2025-10-20-17"))
    .value("event_time", literal(Instant.now()))
    .value("event_id", literal(UUID.randomUUID()))
    .value("severity", literal("INFO"))
    .value("category", literal("application"))
    .value("message", literal("Request processed"))
    .value("payload_json", literal("{}"))
    .usingTtl(3600)                          // 1 hour TTL
    .usingTimestamp(Instant.now().toEpochMilli() * 1000);

session.execute(insert.build());
```

### UPDATE with Counter Increment
```java
Update update = update("events_demo", "event_statistics")
    .increment("event_count", literal(1))
    .increment("error_count", literal(errorCount))
    .whereColumn("category").isEqualTo(literal("application"))
    .whereColumn("time_bucket").isEqualTo(literal("2025-10-20-17"));

session.execute(update.build());
```

### Conditional UPDATE (IF EXISTS)
```java
Update conditionalUpdate = update("events_demo", "alert_rules")
    .setColumn("enabled", literal(false))
    .whereColumn("rule_id").isEqualTo(literal(ruleId))
    .whereColumn("version_time").isEqualTo(literal(versionTime))
    .ifExists();

ResultSet rs = session.execute(conditionalUpdate.build());
boolean applied = rs.wasApplied();  // Check if condition was met
```

### DELETE with Range
```java
Delete delete = deleteFrom("events_demo", "event_log")
    .whereColumn("source_id").isEqualTo(literal("api-server"))
    .whereColumn("hour_bucket").isEqualTo(literal("2025-10-20-17"))
    .whereColumn("event_time").isLessThan(literal(cutoffTime));

session.execute(delete.build());
```

### BATCH Statement Builder
```java
BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);

for (Event event : events) {
    Insert insert = insertInto("events_demo", "event_log")
        .value("source_id", literal(event.getSourceId()))
        .value("hour_bucket", literal(event.getHourBucket()))
        .value("event_time", literal(event.getTime()))
        .value("event_id", literal(event.getId()))
        // ... more values
        .usingTtl(3600);

    batch.addStatement(insert.build());
}

session.execute(batch.build());
```

## Query Builder Benefits

1. **Type Safety**: Compile-time checking of column names and types
2. **Readability**: Fluent API easier to understand than string concatenation
3. **Maintenance**: Refactoring-friendly compared to CQL strings
4. **Reusability**: Query templates with parameterization
5. **IDE Support**: Auto-completion and inline documentation

## Best Practices Demonstrated

1. **Hourly Partitions**: Manageable partition sizes for high-volume events
2. **Query Builder API**: Type-safe programmatic query construction
3. **Counter Tables**: Separate table for real-time statistics
4. **TTL Strategy**: Automatic cleanup of old events and sessions
5. **Frozen Collections**: Immutable complex types for versioned data
6. **Async Writes**: Non-blocking for high throughput
7. **Dynamic Attributes**: MAP columns for flexible schemas
8. **Conditional Updates**: LWT for safe rule updates

## Learning Resources

- [Query Builder API Documentation](https://docs.datastax.com/en/developer/java-driver/4.17/manual/query_builder/)
- [Counter Columns](https://cassandra.apache.org/doc/latest/cassandra/cql/types.html#counters)
- [Frozen Collections](https://cassandra.apache.org/doc/latest/cassandra/cql/types.html#frozen)
- [Lightweight Transactions](https://cassandra.apache.org/doc/latest/cassandra/cql/dml.html#lightweight-transactions)
- [TTL and Expiration](https://cassandra.apache.org/doc/latest/cassandra/cql/dml.html#time-to-live)

## Troubleshooting

**High event loss**:
- Reduce `--event-rate` parameter
- Check Cassandra logs for timeouts
- Increase `REQUEST_TIMEOUT` in application

**Counter inconsistencies**:
- Counters are eventually consistent
- Avoid mixing counter and regular updates
- Use separate counter tables

**Frozen collection updates**:
- Frozen collections are immutable
- Update requires replacing entire collection
- Use regular collections if frequent updates needed

**Query Builder compilation errors**:
- Import static QueryBuilder methods
- Use `literal()` for constant values
- Check column names match schema
