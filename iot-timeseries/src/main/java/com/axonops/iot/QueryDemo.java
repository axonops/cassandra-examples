package com.axonops.iot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletionStage;

/**
 * Query demonstration showing various read patterns and driver features.
 * Demonstrates: range queries, pagination, async reads, different consistency levels,
 * query tracing, and performance measurement.
 */
public class QueryDemo {
    private static final Logger logger = LoggerFactory.getLogger(QueryDemo.class);
    private final CqlSession session;

    public QueryDemo(CqlSession session) {
        this.session = session;
    }

    public void demonstrateQueries() {
        logger.info("=== Query Demonstrations ===");

        // 1. Simple select with prepared statement
        demonstrateSimpleQuery();

        // 2. Range query with time-series data
        demonstrateRangeQuery();

        // 3. Pagination for large result sets
        demonstratePagination();

        // 4. Async query execution
        demonstrateAsyncQuery();

        // 5. Different consistency levels
        demonstrateConsistencyLevels();

        // 6. Counter reads
        demonstrateCounterQuery();

        // 7. Query with collections
        demonstrateCollectionQuery();

        // 8. Query tracing
        demonstrateQueryTracing();
    }

    /**
     * Simple query using prepared statement.
     * Demonstrates: prepared statements, result set iteration.
     */
    private void demonstrateSimpleQuery() {
        logger.info("--- Simple Query Demo ---");

        PreparedStatement prepared = session.prepare(
                "SELECT sensor_id, sensor_type, location, latitude, longitude " +
                "FROM iot_demo.sensor_metadata WHERE sensor_id = ?");

        BoundStatement bound = prepared.bind("sensor-1");
        ResultSet rs = session.execute(bound);

        for (Row row : rs) {
            logger.info("Sensor: {}, Type: {}, Location: {} ({}, {})",
                    row.getString("sensor_id"),
                    row.getString("sensor_type"),
                    row.getString("location"),
                    row.getDouble("latitude"),
                    row.getDouble("longitude"));
        }
    }

    /**
     * Range query for time-series data.
     * Demonstrates: range queries, timestamp filtering, partition key usage.
     */
    private void demonstrateRangeQuery() {
        logger.info("--- Range Query Demo ---");

        String sensorId = "sensor-1";
        String dateBucket = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        Instant rangeStart = Instant.now().minusSeconds(3600);  // Last hour
        Instant rangeEnd = Instant.now();

        PreparedStatement prepared = session.prepare(
                "SELECT timestamp, temperature, humidity, pressure, battery_level " +
                "FROM iot_demo.sensor_data " +
                "WHERE sensor_id = ? AND date_bucket = ? " +
                "AND timestamp >= ? AND timestamp <= ? " +
                "LIMIT 10");

        long startTime = System.nanoTime();
        BoundStatement bound = prepared.bind(sensorId, dateBucket, rangeStart, rangeEnd);
        ResultSet rs = session.execute(bound);
        long duration = (System.nanoTime() - startTime) / 1_000_000;  // Convert to ms

        int count = 0;
        for (Row row : rs) {
            if (count < 3) {  // Log first 3 results
                logger.info("Reading at {}: Temp={:.2f}°C, Humidity={:.1f}%, Pressure={:.1f}mb, Battery={}%",
                        row.getInstant("timestamp"),
                        row.getDouble("temperature"),
                        row.getDouble("humidity"),
                        row.getDouble("pressure"),
                        row.getInt("battery_level"));
            }
            count++;
        }

        logger.info("Range query returned {} rows in {}ms", count, duration);
    }

    /**
     * Pagination demonstration.
     * Demonstrates: paging state, fetch size, manual pagination.
     */
    private void demonstratePagination() {
        logger.info("--- Pagination Demo ---");

        SimpleStatement statement = SimpleStatement.builder(
                "SELECT sensor_id, date_bucket, timestamp, temperature " +
                "FROM iot_demo.sensor_data")
                .setPageSize(5)  // Small page size for demo
                .build();

        ResultSet rs = session.execute(statement);
        int totalRows = 0;
        int pageCount = 0;

        // Iterate through pages
        for (Row row : rs) {
            totalRows++;
        }

        // Get paging state info
        int remaining = rs.getAvailableWithoutFetching();
        boolean hasMore = !rs.isFullyFetched();

        logger.info("Pagination: Total rows: {}, Has more pages: {}", totalRows, hasMore);
    }

    /**
     * Async query execution.
     * Demonstrates: async operations, CompletionStage handling, non-blocking queries.
     */
    private void demonstrateAsyncQuery() {
        logger.info("--- Async Query Demo ---");

        PreparedStatement prepared = session.prepare(
                "SELECT sensor_id, sensor_type FROM iot_demo.sensor_metadata");

        long startTime = System.nanoTime();

        // Execute multiple queries asynchronously
        CompletionStage<AsyncResultSet> future1 = session.executeAsync(prepared.bind());
        CompletionStage<AsyncResultSet> future2 = session.executeAsync(prepared.bind());
        CompletionStage<AsyncResultSet> future3 = session.executeAsync(prepared.bind());

        // Wait for all to complete
        future1.thenCombine(future2, (rs1, rs2) -> rs1.currentPage().spliterator().estimateSize() +
                                                     rs2.currentPage().spliterator().estimateSize())
                .thenCombine(future3, (count, rs3) -> count + rs3.currentPage().spliterator().estimateSize())
                .whenComplete((totalCount, error) -> {
                    long duration = (System.nanoTime() - startTime) / 1_000_000;
                    if (error == null) {
                        logger.info("Async queries completed in {}ms, total results: {}", duration, totalCount);
                    } else {
                        logger.error("Async query error", error);
                    }
                })
                .toCompletableFuture()
                .join();  // Wait for completion (not recommended in real async code)
    }

    /**
     * Demonstrate different consistency levels.
     * Demonstrates: ONE, QUORUM, LOCAL_QUORUM consistency levels.
     */
    private void demonstrateConsistencyLevels() {
        logger.info("--- Consistency Levels Demo ---");

        PreparedStatement prepared = session.prepare(
                "SELECT COUNT(*) FROM iot_demo.sensor_data WHERE sensor_id = ? AND date_bucket = ?");

        String sensorId = "sensor-1";
        String dateBucket = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);

        // Try different consistency levels
        ConsistencyLevel[] levels = {
                ConsistencyLevel.ONE,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.LOCAL_QUORUM
        };

        for (ConsistencyLevel level : levels) {
            long startTime = System.nanoTime();
            BoundStatement bound = prepared.bind(sensorId, dateBucket)
                    .setConsistencyLevel(level);

            try {
                ResultSet rs = session.execute(bound);
                long duration = (System.nanoTime() - startTime) / 1_000_000;
                Row row = rs.one();
                long count = row != null ? row.getLong(0) : 0;
                logger.info("Consistency {}: {} rows, latency: {}ms", level, count, duration);
            } catch (Exception e) {
                logger.error("Query with {} failed: {}", level, e.getMessage());
            }
        }
    }

    /**
     * Counter column queries.
     * Demonstrates: reading counter values.
     */
    private void demonstrateCounterQuery() {
        logger.info("--- Counter Query Demo ---");

        PreparedStatement prepared = session.prepare(
                "SELECT sensor_id, hour_bucket, reading_count, alert_count " +
                "FROM iot_demo.hourly_aggregates " +
                "WHERE sensor_id = ? LIMIT 5");

        for (int i = 1; i <= 3; i++) {
            ResultSet rs = session.execute(prepared.bind("sensor-" + i));
            for (Row row : rs) {
                logger.info("Sensor {}, Hour {}: Readings={}, Alerts={}",
                        row.getString("sensor_id"),
                        row.getString("hour_bucket"),
                        row.getLong("reading_count"),
                        row.getLong("alert_count"));
            }
        }
    }

    /**
     * Query with collection columns.
     * Demonstrates: reading LIST, SET, MAP collections.
     */
    private void demonstrateCollectionQuery() {
        logger.info("--- Collection Query Demo ---");

        // First, insert a sample alert with collections
        insertSampleAlert();

        // Query alerts
        SimpleStatement statement = SimpleStatement.builder(
                "SELECT sensor_id, alert_time, alert_type, severity, " +
                "affected_metrics, tags, metadata " +
                "FROM iot_demo.alert_events LIMIT 5")
                .build();

        ResultSet rs = session.execute(statement);
        for (Row row : rs) {
            logger.info("Alert: Sensor={}, Type={}, Severity={}, Metrics={}, Tags={}, Metadata={}",
                    row.getString("sensor_id"),
                    row.getString("alert_type"),
                    row.getInt("severity"),
                    row.getList("affected_metrics", String.class),
                    row.getSet("tags", String.class),
                    row.getMap("metadata", String.class, String.class));
        }
    }

    /**
     * Query tracing demonstration.
     * Demonstrates: enabling tracing, analyzing query execution.
     */
    private void demonstrateQueryTracing() {
        logger.info("--- Query Tracing Demo ---");

        SimpleStatement statement = SimpleStatement.builder(
                "SELECT * FROM iot_demo.sensor_data WHERE sensor_id = ? AND date_bucket = ? LIMIT 1")
                .addPositionalValue("sensor-1")
                .addPositionalValue(LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE))
                .setTracing(true)  // Enable tracing
                .build();

        ResultSet rs = session.execute(statement);
        rs.one();  // Consume result

        ExecutionInfo execInfo = rs.getExecutionInfo();
        logger.info("Query traced:");
        logger.info("  Coordinator: {}", execInfo.getCoordinator());
        logger.info("  Tried hosts: {}", execInfo.getErrors().size());
        logger.info("  Trace ID: {}", execInfo.getTracingId());
        logger.info("  (Check system_traces.sessions and system_traces.events in Cassandra for full trace)");
    }

    private void insertSampleAlert() {
        try {
            SimpleStatement insert = SimpleStatement.builder(
                    "INSERT INTO iot_demo.alert_events " +
                    "(sensor_id, alert_time, alert_type, severity, message, " +
                    "affected_metrics, tags, metadata) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                    .addPositionalValue("sensor-1")
                    .addPositionalValue(Instant.now())
                    .addPositionalValue("THRESHOLD_EXCEEDED")
                    .addPositionalValue(2)
                    .addPositionalValue("Temperature exceeded threshold")
                    .addPositionalValue(java.util.List.of("temperature", "humidity"))
                    .addPositionalValue(java.util.Set.of("critical", "temperature"))
                    .addPositionalValue(java.util.Map.of(
                            "threshold", "30.0",
                            "actual", "32.5",
                            "unit", "celsius"
                    ))
                    .build();

            session.execute(insert);
        } catch (Exception e) {
            // Ignore if already exists
        }
    }
}
