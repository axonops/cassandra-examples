package com.axonops.iot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for IoT Time-Series application using Testcontainers.
 * Demonstrates: schema setup, data insertion, querying, and failure scenarios.
 */
@Testcontainers
public class IotIntegrationTest {

    @Container
    private static final CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:5.0"))
            .withExposedPorts(9042);

    private static CqlSession session;

    @BeforeAll
    static void setup() {
        // Wait for Cassandra to be ready
        cassandra.start();

        // Create session
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
        // Verify keyspace exists
        ResultSet rs = session.execute("SELECT keyspace_name FROM system_schema.keyspaces " +
                "WHERE keyspace_name = 'iot_demo'");
        assertNotNull(rs.one(), "Keyspace should exist");

        // Verify tables exist
        String[] tables = {"sensor_data", "sensor_metadata", "hourly_aggregates", "alert_events"};
        for (String table : tables) {
            rs = session.execute("SELECT table_name FROM system_schema.tables " +
                    "WHERE keyspace_name = 'iot_demo' AND table_name = ?", table);
            assertNotNull(rs.one(), "Table " + table + " should exist");
        }
    }

    @Test
    void testSensorDataInsertion() {
        // Insert sensor data using prepared statement
        PreparedStatement prepared = session.prepare(
                "INSERT INTO iot_demo.sensor_data " +
                "(sensor_id, date_bucket, timestamp, temperature, humidity, pressure, " +
                "battery_level, reading_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        String sensorId = "test-sensor-1";
        String dateBucket = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        Instant now = Instant.now();

        session.execute(prepared.bind(
                sensorId, dateBucket, now,
                25.5, 60.0, 1013.25, 85, "{\"test\":true}"));

        // Query back the data
        PreparedStatement query = session.prepare(
                "SELECT * FROM iot_demo.sensor_data " +
                "WHERE sensor_id = ? AND date_bucket = ?");

        ResultSet rs = session.execute(query.bind(sensorId, dateBucket));
        Row row = rs.one();

        assertNotNull(row, "Should retrieve inserted data");
        assertEquals(25.5, row.getDouble("temperature"), 0.01);
        assertEquals(60.0, row.getDouble("humidity"), 0.01);
        assertEquals(85, row.getInt("battery_level"));
    }

    @Test
    void testRangeQuery() {
        // Insert multiple readings
        PreparedStatement prepared = session.prepare(
                "INSERT INTO iot_demo.sensor_data " +
                "(sensor_id, date_bucket, timestamp, temperature, humidity, pressure, " +
                "battery_level, reading_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        String sensorId = "test-sensor-range";
        String dateBucket = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        Instant now = Instant.now();

        // Insert 5 readings
        for (int i = 0; i < 5; i++) {
            session.execute(prepared.bind(
                    sensorId, dateBucket, now.minusSeconds(i * 60),
                    20.0 + i, 50.0 + i, 1000.0, 90, "{}"));
        }

        // Query range
        PreparedStatement query = session.prepare(
                "SELECT * FROM iot_demo.sensor_data " +
                "WHERE sensor_id = ? AND date_bucket = ? " +
                "AND timestamp >= ? AND timestamp <= ?");

        Instant rangeStart = now.minusSeconds(300);
        Instant rangeEnd = now;

        ResultSet rs = session.execute(query.bind(sensorId, dateBucket, rangeStart, rangeEnd));

        int count = 0;
        for (Row row : rs) {
            count++;
            assertTrue(row.getDouble("temperature") >= 20.0 && row.getDouble("temperature") <= 25.0);
        }

        assertEquals(5, count, "Should retrieve 5 readings");
    }

    @Test
    void testCounterUpdates() {
        // Update counter
        String sensorId = "test-sensor-counter";
        String hourBucket = "2024-01-01-12";

        PreparedStatement update = session.prepare(
                "UPDATE iot_demo.hourly_aggregates " +
                "SET reading_count = reading_count + ? " +
                "WHERE sensor_id = ? AND hour_bucket = ?");

        // Increment counter 5 times
        for (int i = 0; i < 5; i++) {
            session.execute(update.bind(1L, sensorId, hourBucket));
        }

        // Query counter value
        PreparedStatement query = session.prepare(
                "SELECT reading_count FROM iot_demo.hourly_aggregates " +
                "WHERE sensor_id = ? AND hour_bucket = ?");

        ResultSet rs = session.execute(query.bind(sensorId, hourBucket));
        Row row = rs.one();

        assertNotNull(row);
        assertEquals(5L, row.getLong("reading_count"));
    }

    @Test
    void testLightweightTransaction() {
        // Test IF NOT EXISTS (LWT) on entire primary key
        String insertLWT = "INSERT INTO iot_demo.sensor_metadata " +
                "(sensor_id, registration_id, sensor_type, location) " +
                "VALUES (?, ?, ?, ?) IF NOT EXISTS";

        UUID timeUuid = com.datastax.oss.driver.api.core.uuid.Uuids.timeBased();

        // First insert should succeed
        ResultSet rs1 = session.execute(insertLWT, "lwt-sensor", timeUuid, "test", "lab");
        assertTrue(rs1.wasApplied(), "First LWT insert should succeed");

        // Second insert with same primary key should fail (already exists)
        ResultSet rs2 = session.execute(insertLWT, "lwt-sensor", timeUuid, "test", "lab");
        assertFalse(rs2.wasApplied(), "Second LWT insert should fail");
    }

    @Test
    void testAsyncOperations() {
        PreparedStatement prepared = session.prepare(
                "INSERT INTO iot_demo.sensor_data " +
                "(sensor_id, date_bucket, timestamp, temperature, humidity, pressure, " +
                "battery_level, reading_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        String sensorId = "test-sensor-async";
        String dateBucket = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);

        // Execute 10 async inserts
        for (int i = 0; i < 10; i++) {
            session.executeAsync(prepared.bind(
                    sensorId, dateBucket, Instant.now().plusSeconds(i),
                    20.0, 50.0, 1000.0, 90, "{}"))
                    .toCompletableFuture()
                    .join();
        }

        // Verify count
        ResultSet rs = session.execute(
                "SELECT COUNT(*) FROM iot_demo.sensor_data WHERE sensor_id = ? AND date_bucket = ?",
                sensorId, dateBucket);

        Row row = rs.one();
        assertNotNull(row);
        assertTrue(row.getLong(0) >= 10, "Should have at least 10 records");
    }

    @Test
    void testLoadGenerator() {
        // Test load generator with low rate
        LoadGenerator generator = new LoadGenerator(session, 10);  // 10 writes/sec
        AtomicBoolean running = new AtomicBoolean(true);

        // Run for 3 seconds
        generator.run(3, running);

        // Verify data was written
        ResultSet rs = session.execute(
                "SELECT COUNT(*) FROM iot_demo.sensor_data");

        Row row = rs.one();
        assertNotNull(row);
        long count = row.getLong(0);
        assertTrue(count > 0, "Should have written some data");
    }

    @Test
    void testQueryWithCollections() {
        // Insert alert with collections
        session.execute(
                "INSERT INTO iot_demo.alert_events " +
                "(sensor_id, alert_time, alert_type, severity, message, " +
                "affected_metrics, tags, metadata) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "test-sensor",
                Instant.now(),
                "TEST_ALERT",
                1,
                "Test alert message",
                java.util.List.of("temp", "humidity"),
                java.util.Set.of("test", "integration"),
                java.util.Map.of("key1", "value1", "key2", "value2"));

        // Query back
        ResultSet rs = session.execute(
                "SELECT * FROM iot_demo.alert_events WHERE sensor_id = ?",
                "test-sensor");

        Row row = rs.one();
        assertNotNull(row);
        assertEquals("TEST_ALERT", row.getString("alert_type"));
        assertEquals(2, row.getList("affected_metrics", String.class).size());
        assertEquals(2, row.getSet("tags", String.class).size());
        assertEquals(2, row.getMap("metadata", String.class, String.class).size());
    }
}
