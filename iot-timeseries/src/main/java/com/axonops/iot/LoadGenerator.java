package com.axonops.iot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load generator for IoT sensor data.
 * Demonstrates: prepared statements, async writes, batch statements,
 * different consistency levels, and performance monitoring.
 */
public class LoadGenerator {
    private static final Logger logger = LoggerFactory.getLogger(LoadGenerator.class);
    private final CqlSession session;
    private final int targetWritesPerSecond;
    private final Random random = new Random();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    // Prepared statements for reuse
    private final PreparedStatement insertSensorData;
    private final PreparedStatement updateCounters;

    // Metrics
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public LoadGenerator(CqlSession session, int targetWritesPerSecond) {
        this.session = session;
        this.targetWritesPerSecond = targetWritesPerSecond;

        // Prepare statements once for reuse (performance best practice)
        this.insertSensorData = session.prepare(
                "INSERT INTO iot_demo.sensor_data " +
                "(sensor_id, date_bucket, timestamp, temperature, humidity, pressure, " +
                "battery_level, reading_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "USING TTL ?");

        this.updateCounters = session.prepare(
                "UPDATE iot_demo.hourly_aggregates " +
                "SET reading_count = reading_count + 1 " +
                "WHERE sensor_id = ? AND hour_bucket = ?");

        logger.info("LoadGenerator initialized with target rate: {} writes/sec", targetWritesPerSecond);
    }

    /**
     * Run load generation for specified duration.
     * Demonstrates async writes with rate limiting.
     */
    public void run(int durationSeconds, AtomicBoolean running) {
        logger.info("Starting load generation for {} seconds", durationSeconds);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (durationSeconds * 1000L);
        long intervalMs = 1000L;
        int writesPerInterval = targetWritesPerSecond;

        ScheduledExecutorService metricsExecutor = Executors.newSingleThreadScheduledExecutor();
        metricsExecutor.scheduleAtFixedRate(this::reportMetrics, 5, 5, TimeUnit.SECONDS);

        try {
            while (System.currentTimeMillis() < endTime && running.get()) {
                long intervalStart = System.currentTimeMillis();

                // Generate batch of writes for this interval
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();

                for (int i = 0; i < writesPerInterval; i++) {
                    if (!running.get()) break;

                    // Demonstrate different write patterns
                    if (i % 10 == 0) {
                        // Every 10th write: use batch statement
                        futures.add(writeBatchAsync());
                    } else {
                        // Regular async write
                        futures.add(writeAsync());
                    }
                }

                // Wait for all async operations to complete
                CompletableFuture.allOf(futures.stream()
                        .map(CompletionStage::toCompletableFuture)
                        .toArray(CompletableFuture[]::new))
                        .join();

                // Sleep to maintain target rate
                long elapsed = System.currentTimeMillis() - intervalStart;
                if (elapsed < intervalMs) {
                    Thread.sleep(intervalMs - elapsed);
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Load generation interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            metricsExecutor.shutdown();
            executor.shutdown();
            reportFinalMetrics(startTime);
        }
    }

    /**
     * Async write of single sensor reading.
     * Demonstrates: prepared statements, async execution, consistency levels.
     */
    private CompletionStage<AsyncResultSet> writeAsync() {
        String sensorId = "sensor-" + (random.nextInt(5) + 1);
        Instant now = Instant.now();
        String dateBucket = formatDateBucket(now);
        String hourBucket = formatHourBucket(now);

        // Generate realistic sensor readings
        double temperature = 20.0 + (random.nextDouble() * 10.0);
        double humidity = 40.0 + (random.nextDouble() * 20.0);
        double pressure = 1000.0 + (random.nextDouble() * 50.0);
        int batteryLevel = 50 + random.nextInt(50);

        // JSON payload demo
        String json = String.format("{\"temp\":%.2f,\"hum\":%.2f}", temperature, humidity);

        // Bind prepared statement with values
        BoundStatement statement = insertSensorData.bind(
                sensorId,
                dateBucket,
                now,
                temperature,
                humidity,
                pressure,
                batteryLevel,
                json,
                86400  // TTL: 24 hours for this example
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);  // Demonstrate consistency level

        // Execute async and handle result
        return session.executeAsync(statement)
                .whenComplete((rs, error) -> {
                    if (error == null) {
                        writeCount.incrementAndGet();
                        // Update counter asynchronously
                        updateCounterAsync(sensorId, hourBucket);
                    } else {
                        errorCount.incrementAndGet();
                        logger.error("Write error", error);
                    }
                });
    }

    /**
     * Batch write demonstrating batch statements.
     * Note: Use UNLOGGED batch for writes to same partition (best practice).
     */
    private CompletionStage<AsyncResultSet> writeBatchAsync() {
        String sensorId = "sensor-" + (random.nextInt(5) + 1);
        Instant now = Instant.now();
        String dateBucket = formatDateBucket(now);

        // Create UNLOGGED batch (better performance for same-partition writes)
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);

        // Add 3 readings to batch
        for (int i = 0; i < 3; i++) {
            Instant timestamp = now.plusSeconds(i);
            batch.addStatement(insertSensorData.bind(
                    sensorId,
                    dateBucket,
                    timestamp,
                    20.0 + random.nextDouble() * 10.0,
                    40.0 + random.nextDouble() * 20.0,
                    1000.0 + random.nextDouble() * 50.0,
                    50 + random.nextInt(50),
                    "{}",
                    86400
            ));
        }

        return session.executeAsync(batch.build())
                .whenComplete((rs, error) -> {
                    if (error == null) {
                        writeCount.addAndGet(3);
                    } else {
                        errorCount.incrementAndGet();
                        logger.error("Batch write error", error);
                    }
                });
    }

    /**
     * Update counter column atomically.
     * Demonstrates: counter updates, async execution.
     */
    private void updateCounterAsync(String sensorId, String hourBucket) {
        BoundStatement statement = updateCounters.bind(sensorId, hourBucket);

        session.executeAsync(statement)
                .whenComplete((rs, error) -> {
                    if (error != null) {
                        logger.error("Counter update error", error);
                    }
                });
    }

    private String formatDateBucket(Instant instant) {
        return DateTimeFormatter.ISO_LOCAL_DATE
                .format(instant.atZone(ZoneId.systemDefault()));
    }

    private String formatHourBucket(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                .format(instant.atZone(ZoneId.systemDefault()));
    }

    private void reportMetrics() {
        long writes = writeCount.get();
        long errors = errorCount.get();
        logger.info("Metrics - Writes: {}, Errors: {}, Error Rate: {:.2f}%",
                writes, errors, errors > 0 ? (errors * 100.0 / writes) : 0.0);
    }

    private void reportFinalMetrics(long startTime) {
        long duration = (System.currentTimeMillis() - startTime) / 1000;
        long totalWrites = writeCount.get();
        long totalErrors = errorCount.get();

        logger.info("=== Final Metrics ===");
        logger.info("Duration: {} seconds", duration);
        logger.info("Total Writes: {}", totalWrites);
        logger.info("Total Errors: {}", totalErrors);
        logger.info("Average Write Rate: {} writes/sec", duration > 0 ? totalWrites / duration : 0);
        logger.info("Error Rate: {:.2f}%",
                totalWrites > 0 ? (totalErrors * 100.0 / totalWrites) : 0.0);
    }
}
