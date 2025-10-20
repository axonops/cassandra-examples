package com.axonops.events;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Event Logging application using Testcontainers.
 * Demonstrates: Query Builder API, batch operations, and advanced querying.
 */
@Testcontainers
public class EventLoggingIntegrationTest {

    @Container
    private static final CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:5.0"))
            .withExposedPorts(9042);

    private static CqlSession session;

    @BeforeAll
    static void setup() {
        cassandra.start();

        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(
                        cassandra.getHost(),
                        cassandra.getMappedPort(9042)))
                .withLocalDatacenter(cassandra.getLocalDatacenter())
                .build();

        // Setup schema
        SchemaSetup schemaSetup = new SchemaSetup(session);
        schemaSetup.createKeyspace();
        schemaSetup.createTables();
        schemaSetup.createIndexes();
        schemaSetup.insertSampleData();
    }

    @AfterAll
    static void teardown() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void testSchemaCreation() {
        ResultSet rs = session.execute("SELECT keyspace_name FROM system_schema.keyspaces " +
                "WHERE keyspace_name = 'events_demo'");
        assertNotNull(rs.one(), "Keyspace should exist");

        String[] tables = {"event_log", "event_statistics", "user_activity", "alert_rules"};
        for (String table : tables) {
            rs = session.execute("SELECT table_name FROM system_schema.tables " +
                    "WHERE keyspace_name = 'events_demo' AND table_name = ?", table);
            assertNotNull(rs.one(), "Table " + table + " should exist");
        }
    }

    @Test
    void testQueryBuilderSelect() {
        // Insert test data first
        String sourceId = "test-source";
        String hourBucket = formatHourBucket(Instant.now());

        session.execute(
                insertInto("events_demo", "event_log")
                        .value("source_id", literal(sourceId))
                        .value("hour_bucket", literal(hourBucket))
                        .value("event_time", literal(Instant.now()))
                        .value("event_id", literal(UUID.randomUUID()))
                        .value("severity", literal("INFO"))
                        .value("category", literal("test"))
                        .value("message", literal("Test event"))
                        .value("payload_json", literal("{}"))
                        .build());

        // Query using Query Builder
        var select = selectFrom("events_demo", "event_log")
                .all()
                .whereColumn("source_id").isEqualTo(literal(sourceId))
                .whereColumn("hour_bucket").isEqualTo(literal(hourBucket));

        ResultSet rs = session.execute(select.build());
        Row row = rs.one();

        assertNotNull(row);
        assertEquals("INFO", row.getString("severity"));
        assertEquals("test", row.getString("category"));
    }

    @Test
    void testQueryBuilderInsertWithTTL() {
        String sourceId = "ttl-test-source";
        String hourBucket = formatHourBucket(Instant.now());
        Instant now = Instant.now();

        // Insert with TTL using Query Builder
        var insert = insertInto("events_demo", "event_log")
                .value("source_id", literal(sourceId))
                .value("hour_bucket", literal(hourBucket))
                .value("event_time", literal(now))
                .value("event_id", literal(UUID.randomUUID()))
                .value("severity", literal("DEBUG"))
                .value("category", literal("ttl-test"))
                .value("message", literal("TTL test event"))
                .value("payload_json", literal("{\"ttl\":true}"))
                .usingTtl(3600);

        session.execute(insert.build());

        // Query back
        ResultSet rs = session.execute(
                "SELECT TTL(message) FROM events_demo.event_log " +
                "WHERE source_id = ? AND hour_bucket = ?",
                sourceId, hourBucket);

        Row row = rs.one();
        assertNotNull(row);
        int ttl = row.getInt(0);
        assertTrue(ttl > 0 && ttl <= 3600, "TTL should be set and within range");
    }

    @Test
    void testQueryBuilderUpdate() {
        String category = "update-test";
        String timeBucket = formatHourBucket(Instant.now());

        // Update counter using Query Builder
        var update = update("events_demo", "event_statistics")
                .increment("event_count", literal(5L))
                .whereColumn("category").isEqualTo(literal(category))
                .whereColumn("time_bucket").isEqualTo(literal(timeBucket));

        session.execute(update.build());

        // Verify
        ResultSet rs = session.execute(
                "SELECT event_count FROM events_demo.event_statistics " +
                "WHERE category = ? AND time_bucket = ?",
                category, timeBucket);

        Row row = rs.one();
        assertNotNull(row);
        assertEquals(5L, row.getLong("event_count"));
    }

    @Test
    void testQueryBuilderDelete() {
        String sourceId = "delete-test";
        String hourBucket = formatHourBucket(Instant.now());
        Instant now = Instant.now();

        // Insert event
        session.execute(
                "INSERT INTO events_demo.event_log " +
                "(source_id, hour_bucket, event_time, event_id, severity, category, message, payload_json) " +
                "VALUES (?, ?, ?, uuid(), ?, ?, ?, ?)",
                sourceId, hourBucket, now, "INFO", "delete-test", "To be deleted", "{}");

        // Delete using Query Builder
        var delete = deleteFrom("events_demo", "event_log")
                .whereColumn("source_id").isEqualTo(literal(sourceId))
                .whereColumn("hour_bucket").isEqualTo(literal(hourBucket))
                .whereColumn("event_time").isEqualTo(literal(now));

        session.execute(delete.build());

        // Verify deletion
        ResultSet rs = session.execute(
                "SELECT * FROM events_demo.event_log WHERE source_id = ? AND hour_bucket = ?",
                sourceId, hourBucket);

        assertNull(rs.one(), "Event should be deleted");
    }

    @Test
    void testBatchStatements() {
        String sourceId = "batch-test";
        String hourBucket = formatHourBucket(Instant.now());

        // Build batch with Query Builder
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.UNLOGGED);

        for (int i = 0; i < 5; i++) {
            var insert = insertInto("events_demo", "event_log")
                    .value("source_id", literal(sourceId))
                    .value("hour_bucket", literal(hourBucket))
                    .value("event_time", literal(Instant.now().plusSeconds(i)))
                    .value("event_id", literal(UUID.randomUUID()))
                    .value("severity", literal("INFO"))
                    .value("category", literal("batch"))
                    .value("message", literal("Batch event " + i))
                    .value("payload_json", literal("{}"));

            batch.addStatement(insert.build());
        }

        session.execute(batch.build());

        // Verify batch insert
        ResultSet rs = session.execute(
                "SELECT COUNT(*) FROM events_demo.event_log WHERE source_id = ? AND hour_bucket = ?",
                sourceId, hourBucket);

        Row row = rs.one();
        assertNotNull(row);
        assertTrue(row.getLong(0) >= 5, "Should have at least 5 events from batch");
    }

    @Test
    void testUserActivityWithDynamicAttributes() {
        String userId = "test-user";
        UUID sessionId = UUID.randomUUID();
        Instant now = Instant.now();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("browser", "Chrome");
        attributes.put("os", "Linux");
        attributes.put("version", "1.0");

        // Insert user activity
        session.execute(
                "INSERT INTO events_demo.user_activity " +
                "(user_id, session_id, activity_time, action, attributes) " +
                "VALUES (?, ?, ?, ?, ?)",
                userId, sessionId, now, "login", attributes);

        // Query back
        ResultSet rs = session.execute(
                "SELECT * FROM events_demo.user_activity WHERE user_id = ?",
                userId);

        Row row = rs.one();
        assertNotNull(row);
        assertEquals("login", row.getString("action"));

        Map<String, String> retrievedAttrs = row.getMap("attributes", String.class, String.class);
        assertEquals(3, retrievedAttrs.size());
        assertEquals("Chrome", retrievedAttrs.get("browser"));
    }

    @Test
    void testFrozenCollections() {
        UUID ruleId = UUID.randomUUID();
        Instant now = Instant.now();

        Map<String, String> conditions = new HashMap<>();
        conditions.put("threshold", "100");
        conditions.put("window", "5m");

        List<String> actions = Arrays.asList("email", "slack", "pagerduty");
        Set<String> tags = new HashSet<>(Arrays.asList("critical", "alert"));

        // Insert alert rule with frozen collections
        session.execute(
                "INSERT INTO events_demo.alert_rules " +
                "(rule_id, version_time, rule_name, enabled, conditions, actions, tags) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ruleId, now, "Test Rule", true, conditions, actions, tags);

        // Query back
        ResultSet rs = session.execute(
                "SELECT * FROM events_demo.alert_rules WHERE rule_id = ?",
                ruleId);

        Row row = rs.one();
        assertNotNull(row);
        assertEquals("Test Rule", row.getString("rule_name"));
        assertTrue(row.getBoolean("enabled"));

        Map<String, String> retrievedConditions = row.getMap("conditions", String.class, String.class);
        assertEquals(2, retrievedConditions.size());

        List<String> retrievedActions = row.getList("actions", String.class);
        assertEquals(3, retrievedActions.size());

        Set<String> retrievedTags = row.getSet("tags", String.class);
        assertEquals(2, retrievedTags.size());
    }

    @Test
    void testEventGenerator() {
        EventGenerator generator = new EventGenerator(session, 50);  // 50 events/sec
        AtomicBoolean running = new AtomicBoolean(true);

        // Run for 2 seconds
        generator.run(2, running);

        // Verify events were generated
        ResultSet rs = session.execute(
                "SELECT COUNT(*) FROM events_demo.event_log");

        Row row = rs.one();
        assertNotNull(row);
        long count = row.getLong(0);
        assertTrue(count > 0, "Should have generated events");
    }

    @Test
    void testConditionalUpdate() {
        UUID ruleId = UUID.randomUUID();
        Instant now = Instant.now();

        // Insert initial rule
        session.execute(
                "INSERT INTO events_demo.alert_rules " +
                "(rule_id, version_time, rule_name, enabled, conditions, actions, tags) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ruleId, now, "Initial Rule", true,
                Collections.emptyMap(), Collections.emptyList(), Collections.emptySet());

        // Conditional update with IF EXISTS
        var update = update("events_demo", "alert_rules")
                .setColumn("enabled", literal(false))
                .whereColumn("rule_id").isEqualTo(literal(ruleId))
                .whereColumn("version_time").isEqualTo(literal(now))
                .ifExists();

        ResultSet rs = session.execute(update.build());
        assertTrue(rs.wasApplied(), "Conditional update should succeed");

        // Try conditional update on non-existent row
        var update2 = update("events_demo", "alert_rules")
                .setColumn("enabled", literal(false))
                .whereColumn("rule_id").isEqualTo(literal(UUID.randomUUID()))
                .whereColumn("version_time").isEqualTo(literal(now))
                .ifExists();

        ResultSet rs2 = session.execute(update2.build());
        assertFalse(rs2.wasApplied(), "Conditional update should fail for non-existent row");
    }

    private String formatHourBucket(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                .format(instant.atZone(ZoneId.systemDefault()));
    }
}
