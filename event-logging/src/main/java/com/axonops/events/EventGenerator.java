package com.axonops.events;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Event generator demonstrating high-throughput event logging.
 * Shows prepared statements, async operations, and batch processing.
 */
public class EventGenerator {
    private static final Logger logger = LoggerFactory.getLogger(EventGenerator.class);
    private final CqlSession session;
    private final int targetEventsPerSecond;
    private final Random random = new Random();

    private final PreparedStatement insertEvent;
    private final PreparedStatement updateStatistics;

    private final AtomicLong eventCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    private static final String[] SOURCES = {"api-server", "web-app", "mobile-app", "backend-service", "database"};
    private static final String[] SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR"};
    private static final String[] CATEGORIES = {"application", "security", "database", "network", "system"};
    private static final String[] MESSAGES = {
            "Request processed successfully",
            "Connection established",
            "Query executed",
            "User authenticated",
            "Cache hit",
            "Rate limit exceeded",
            "Timeout occurred",
            "Connection failed"
    };

    public EventGenerator(CqlSession session, int targetEventsPerSecond) {
        this.session = session;
        this.targetEventsPerSecond = targetEventsPerSecond;

        // Prepare statements
        this.insertEvent = session.prepare(
                "INSERT INTO events_demo.event_log " +
                "(source_id, hour_bucket, event_time, event_id, severity, category, message, payload_json) " +
                "VALUES (?, ?, ?, uuid(), ?, ?, ?, ?) " +
                "USING TTL ?");

        this.updateStatistics = session.prepare(
                "UPDATE events_demo.event_statistics " +
                "SET event_count = event_count + 1 " +
                "WHERE category = ? AND time_bucket = ?");

        logger.info("EventGenerator initialized with target rate: {} events/sec", targetEventsPerSecond);
    }

    public void run(int durationSeconds, AtomicBoolean running) {
        logger.info("Starting event generation for {} seconds", durationSeconds);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (durationSeconds * 1000L);

        ScheduledExecutorService metricsExecutor = Executors.newSingleThreadScheduledExecutor();
        metricsExecutor.scheduleAtFixedRate(this::reportMetrics, 5, 5, TimeUnit.SECONDS);

        try {
            while (System.currentTimeMillis() < endTime && running.get()) {
                long intervalStart = System.currentTimeMillis();

                // Generate events for this second
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();

                for (int i = 0; i < targetEventsPerSecond; i++) {
                    if (!running.get()) break;
                    futures.add(generateEventAsync());
                }

                // Wait for all async operations
                CompletableFuture.allOf(futures.stream()
                        .map(CompletionStage::toCompletableFuture)
                        .toArray(CompletableFuture[]::new))
                        .join();

                // Sleep to maintain rate
                long elapsed = System.currentTimeMillis() - intervalStart;
                if (elapsed < 1000) {
                    Thread.sleep(1000 - elapsed);
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Event generation interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            metricsExecutor.shutdown();
            reportFinalMetrics(startTime);
        }
    }

    private CompletionStage<AsyncResultSet> generateEventAsync() {
        Instant now = Instant.now();
        String hourBucket = formatHourBucket(now);
        String sourceId = SOURCES[random.nextInt(SOURCES.length)];
        String severity = SEVERITIES[random.nextInt(SEVERITIES.length)];
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        String message = MESSAGES[random.nextInt(MESSAGES.length)];

        // Generate JSON payload
        String payload = String.format(
                "{\"timestamp\":\"%s\",\"source\":\"%s\",\"severity\":\"%s\"}",
                now, sourceId, severity);

        BoundStatement statement = insertEvent.bind(
                sourceId,
                hourBucket,
                now,
                severity,
                category,
                message,
                payload,
                86400  // TTL: 24 hours
        );

        return session.executeAsync(statement)
                .whenComplete((rs, error) -> {
                    if (error == null) {
                        eventCount.incrementAndGet();
                        updateStatisticsAsync(category, hourBucket);
                    } else {
                        errorCount.incrementAndGet();
                        logger.error("Event write error", error);
                    }
                });
    }

    private void updateStatisticsAsync(String category, String timeBucket) {
        BoundStatement statement = updateStatistics.bind(category, timeBucket);
        session.executeAsync(statement)
                .whenComplete((rs, error) -> {
                    if (error != null) {
                        logger.error("Statistics update error", error);
                    }
                });
    }

    private String formatHourBucket(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                .format(instant.atZone(ZoneId.systemDefault()));
    }

    private void reportMetrics() {
        long events = eventCount.get();
        long errors = errorCount.get();
        logger.info("Metrics - Events: {}, Errors: {}, Error Rate: {:.2f}%",
                events, errors, errors > 0 ? (errors * 100.0 / events) : 0.0);
    }

    private void reportFinalMetrics(long startTime) {
        long duration = (System.currentTimeMillis() - startTime) / 1000;
        long totalEvents = eventCount.get();
        long totalErrors = errorCount.get();

        logger.info("=== Final Metrics ===");
        logger.info("Duration: {} seconds", duration);
        logger.info("Total Events: {}", totalEvents);
        logger.info("Total Errors: {}", totalErrors);
        logger.info("Average Event Rate: {} events/sec", duration > 0 ? totalEvents / duration : 0);
        logger.info("Error Rate: {:.2f}%",
                totalEvents > 0 ? (totalErrors * 100.0 / totalEvents) : 0.0);
    }
}
