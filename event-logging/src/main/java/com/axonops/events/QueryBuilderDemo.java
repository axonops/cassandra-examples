package com.axonops.events;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

/**
 * Query Builder API demonstrations.
 * Shows programmatic query construction with type safety and readability.
 */
public class QueryBuilderDemo {
    private static final Logger logger = LoggerFactory.getLogger(QueryBuilderDemo.class);
    private final CqlSession session;

    public QueryBuilderDemo(CqlSession session) {
        this.session = session;
    }

    public void demonstrateQueryBuilder() {
        logger.info("=== Query Builder Demonstrations ===");

        demonstrateSelectQueries();
        demonstrateInsertQueries();
        demonstrateUpdateQueries();
        demonstrateDeleteQueries();
        demonstrateBatchBuilder();
        demonstrateComplexQueries();
    }

    /**
     * SELECT statement builder demonstrations.
     * Shows various SELECT patterns with Query Builder.
     */
    private void demonstrateSelectQueries() {
        logger.info("--- SELECT Query Builder Demo ---");

        // Simple SELECT with WHERE clause
        Select select = selectFrom("events_demo", "event_log")
                .all()
                .whereColumn("source_id").isEqualTo(literal("api-server"))
                .whereColumn("hour_bucket").isEqualTo(literal(getCurrentHourBucket()))
                .limit(5);

        logger.info("Query: {}", select.asCql());
        ResultSet rs = session.execute(select.build());
        int count = 0;
        for (Row row : rs) {
            count++;
        }
        logger.info("Retrieved {} events", count);

        // SELECT with specific columns
        Select selectColumns = selectFrom("events_demo", "event_log")
                .columns("source_id", "event_time", "severity", "message")
                .whereColumn("source_id").isEqualTo(literal("api-server"))
                .whereColumn("hour_bucket").isEqualTo(literal(getCurrentHourBucket()))
                .limit(3);

        logger.info("Query: {}", selectColumns.asCql());
        rs = session.execute(selectColumns.build());
        for (Row row : rs) {
            logger.info("Event: {} - {} - {}",
                    row.getInstant("event_time"),
                    row.getString("severity"),
                    row.getString("message"));
        }

        // SELECT with specific columns
        Select selectCount = selectFrom("events_demo", "event_statistics")
                .column("category")
                .column("event_count")
                .whereColumn("category").isEqualTo(literal("application"))
                .limit(5);

        logger.info("Query: {}", selectCount.asCql());
        rs = session.execute(selectCount.build());
        int rowCount = 0;
        for (Row row : rs) {
            rowCount++;
            logger.info("Category: {}, Count: {}",
                    row.getString("category"),
                    row.getLong("event_count"));
        }
        logger.info("Retrieved {} statistics rows", rowCount);
    }

    /**
     * INSERT statement builder demonstrations.
     * Shows inserts with TTL and timestamp options.
     */
    private void demonstrateInsertQueries() {
        logger.info("--- INSERT Query Builder Demo ---");

        Instant now = Instant.now();
        String hourBucket = formatHourBucket(now);

        // INSERT with TTL
        Insert insert = insertInto("events_demo", "event_log")
                .value("source_id", literal("query-builder-demo"))
                .value("hour_bucket", literal(hourBucket))
                .value("event_time", literal(now))
                .value("event_id", literal(UUID.randomUUID()))
                .value("severity", literal("INFO"))
                .value("category", literal("demo"))
                .value("message", literal("Query Builder demonstration"))
                .value("payload_json", literal("{\"demo\":true}"))
                .usingTtl(3600);  // 1 hour TTL

        logger.info("Query: {}", insert.asCql());
        session.execute(insert.build());
        logger.info("Inserted event with TTL");

        // INSERT with custom timestamp
        Insert insertWithTimestamp = insertInto("events_demo", "event_log")
                .value("source_id", literal("query-builder-demo"))
                .value("hour_bucket", literal(hourBucket))
                .value("event_time", literal(now.plusSeconds(1)))
                .value("event_id", literal(UUID.randomUUID()))
                .value("severity", literal("DEBUG"))
                .value("category", literal("demo"))
                .value("message", literal("With custom timestamp"))
                .value("payload_json", literal("{}"))
                .usingTimestamp(now.toEpochMilli() * 1000);  // Microseconds

        logger.info("Query: {}", insertWithTimestamp.asCql());
        session.execute(insertWithTimestamp.build());
        logger.info("Inserted event with custom timestamp");
    }

    /**
     * UPDATE statement builder demonstrations.
     * Shows conditional updates and collection operations.
     */
    private void demonstrateUpdateQueries() {
        logger.info("--- UPDATE Query Builder Demo ---");

        // Counter update
        Update updateCounter = update("events_demo", "event_statistics")
                .increment("event_count", literal(10))
                .whereColumn("category").isEqualTo(literal("demo"))
                .whereColumn("time_bucket").isEqualTo(literal(getCurrentHourBucket()));

        logger.info("Query: {}", updateCounter.asCql());
        session.execute(updateCounter.build());
        logger.info("Updated counter");

        // Conditional update with IF EXISTS
        UUID ruleId = UUID.randomUUID();
        Update updateRule = update("events_demo", "alert_rules")
                .setColumn("enabled", literal(false))
                .whereColumn("rule_id").isEqualTo(literal(ruleId))
                .whereColumn("version_time").isEqualTo(literal(Instant.now()))
                .ifExists();

        logger.info("Query: {}", updateRule.asCql());
        ResultSet rs = session.execute(updateRule.build());
        logger.info("Conditional update applied: {}", rs.wasApplied());
    }

    /**
     * DELETE statement builder demonstrations.
     * Shows range deletes and conditional deletes.
     */
    private void demonstrateDeleteQueries() {
        logger.info("--- DELETE Query Builder Demo ---");

        // Delete specific row
        Delete delete = deleteFrom("events_demo", "event_log")
                .whereColumn("source_id").isEqualTo(literal("query-builder-demo"))
                .whereColumn("hour_bucket").isEqualTo(literal(getCurrentHourBucket()))
                .whereColumn("event_time").isLessThan(literal(Instant.now().minusSeconds(60)));

        logger.info("Query: {}", delete.asCql());
        session.execute(delete.build());
        logger.info("Deleted old events");

        // Conditional delete
        Delete deleteIfExists = deleteFrom("events_demo", "alert_rules")
                .whereColumn("rule_id").isEqualTo(literal(UUID.randomUUID()))
                .whereColumn("version_time").isEqualTo(literal(Instant.now()))
                .ifExists();

        logger.info("Query: {}", deleteIfExists.asCql());
        ResultSet rs = session.execute(deleteIfExists.build());
        logger.info("Conditional delete applied: {}", rs.wasApplied());
    }

    /**
     * Batch statement builder demonstration.
     * Shows how to construct batch statements programmatically.
     */
    private void demonstrateBatchBuilder() {
        logger.info("--- BATCH Query Builder Demo ---");

        Instant now = Instant.now();
        String hourBucket = formatHourBucket(now);

        // Build batch with multiple inserts
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);

        for (int i = 0; i < 3; i++) {
            Insert insert = insertInto("events_demo", "event_log")
                    .value("source_id", literal("batch-demo"))
                    .value("hour_bucket", literal(hourBucket))
                    .value("event_time", literal(now.plusSeconds(i)))
                    .value("event_id", literal(UUID.randomUUID()))
                    .value("severity", literal("INFO"))
                    .value("category", literal("batch"))
                    .value("message", literal("Batch event " + i))
                    .value("payload_json", literal("{}"))
                    .usingTtl(3600);

            batch.addStatement(insert.build());
        }

        logger.info("Executing batch with 3 statements");
        session.execute(batch.build());
        logger.info("Batch executed successfully");
    }

    /**
     * Complex query demonstrations.
     * Shows advanced query patterns and query metadata.
     */
    private void demonstrateComplexQueries() {
        logger.info("--- Complex Query Demo ---");

        // Query with ordering and filtering
        Select complexSelect = selectFrom("events_demo", "event_log")
                .all()
                .whereColumn("source_id").isEqualTo(literal("api-server"))
                .whereColumn("hour_bucket").isEqualTo(literal(getCurrentHourBucket()))
                .whereColumn("event_time").isGreaterThanOrEqualTo(literal(Instant.now().minusSeconds(300)))
                .limit(10);

        logger.info("Query: {}", complexSelect.asCql());

        long startTime = System.nanoTime();
        ResultSet rs = session.execute(complexSelect.build());
        long duration = (System.nanoTime() - startTime) / 1_000_000;

        int count = 0;
        for (Row row : rs) {
            count++;
        }

        logger.info("Complex query returned {} rows in {}ms", count, duration);

        // Query execution info
        ExecutionInfo execInfo = rs.getExecutionInfo();
        logger.info("Coordinator: {}", execInfo.getCoordinator());
        logger.info("Query warnings: {}", execInfo.getWarnings().size());
    }

    private String getCurrentHourBucket() {
        return formatHourBucket(Instant.now());
    }

    private String formatHourBucket(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                .format(instant.atZone(ZoneId.systemDefault()));
    }
}
