package com.axonops.iot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;
import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Schema setup for IoT Time-Series application.
 * Demonstrates keyspace creation, table creation with various features,
 * indexes, and sample data insertion.
 */
public class SchemaSetup {
    private static final Logger logger = LoggerFactory.getLogger(SchemaSetup.class);
    private final CqlSession session;

    public SchemaSetup(CqlSession session) {
        this.session = session;
    }

    /**
     * Create keyspace with NetworkTopologyStrategy for production-ready replication.
     */
    public void createKeyspace() {
        logger.info("Creating keyspace 'iot_demo'");

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS iot_demo " +
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} " +
                "AND durable_writes = true";

        session.execute(createKeyspace);
        session.execute("USE iot_demo");
        logger.info("Keyspace created and set as active");
    }

    /**
     * Create all tables demonstrating various Cassandra features.
     */
    public void createTables() {
        createSensorDataTable();
        createSensorMetadataTable();
        createHourlyAggregatesTable();
        createAlertEventsTable();
    }

    /**
     * Sensor data table: Time-series partitioned by sensor_id and date.
     * Demonstrates: partition keys, clustering columns, TWCS compaction, TTL
     */
    private void createSensorDataTable() {
        logger.info("Creating sensor_data table with TWCS compaction");

        String createTable = "CREATE TABLE IF NOT EXISTS sensor_data (" +
                "sensor_id text, " +
                "date_bucket text, " +  // Format: YYYY-MM-DD for daily partitions
                "timestamp timestamp, " +
                "temperature double, " +
                "humidity double, " +
                "pressure double, " +
                "battery_level int, " +
                "reading_json text, " +  // JSON support demo
                "PRIMARY KEY ((sensor_id, date_bucket), timestamp)) " +
                "WITH CLUSTERING ORDER BY (timestamp DESC) " +
                "AND compaction = {" +
                "'class': 'TimeWindowCompactionStrategy', " +
                "'compaction_window_unit': 'DAYS', " +
                "'compaction_window_size': 1} " +
                "AND default_time_to_live = 2592000";  // 30 days TTL

        session.execute(createTable);
        logger.info("sensor_data table created");
    }

    /**
     * Sensor metadata table with static columns.
     * Demonstrates: static columns, LWT for safe registration
     */
    private void createSensorMetadataTable() {
        logger.info("Creating sensor_metadata table");

        String createTable = "CREATE TABLE IF NOT EXISTS sensor_metadata (" +
                "sensor_id text, " +
                "registration_id timeuuid, " +
                "sensor_type text static, " +  // Static column: same for all registrations
                "location text static, " +
                "latitude double static, " +
                "longitude double static, " +
                "registration_time timestamp, " +
                "config map<text, text>, " +
                "PRIMARY KEY (sensor_id, registration_id)) " +
                "WITH CLUSTERING ORDER BY (registration_id DESC)";

        session.execute(createTable);
        logger.info("sensor_metadata table created");
    }

    /**
     * Hourly aggregates using counters.
     * Demonstrates: counter columns for atomic increments
     */
    private void createHourlyAggregatesTable() {
        logger.info("Creating hourly_aggregates table with counters");

        String createTable = "CREATE TABLE IF NOT EXISTS hourly_aggregates (" +
                "sensor_id text, " +
                "hour_bucket text, " +  // Format: YYYY-MM-DD-HH
                "reading_count counter, " +
                "alert_count counter, " +
                "PRIMARY KEY (sensor_id, hour_bucket)) " +
                "WITH CLUSTERING ORDER BY (hour_bucket DESC)";

        session.execute(createTable);
        logger.info("hourly_aggregates table created");
    }

    /**
     * Alert events with collections.
     * Demonstrates: LIST, SET, MAP collections, LWT for conditional updates
     */
    private void createAlertEventsTable() {
        logger.info("Creating alert_events table with collections");

        String createTable = "CREATE TABLE IF NOT EXISTS alert_events (" +
                "sensor_id text, " +
                "alert_time timestamp, " +
                "alert_type text, " +
                "severity int, " +
                "message text, " +
                "affected_metrics list<text>, " +  // LIST for ordered collection
                "tags set<text>, " +                // SET for unique tags
                "metadata map<text, text>, " +      // MAP for key-value pairs
                "PRIMARY KEY (sensor_id, alert_time)) " +
                "WITH CLUSTERING ORDER BY (alert_time DESC)";

        session.execute(createTable);
        logger.info("alert_events table created");
    }

    /**
     * Create indexes demonstrating different index types.
     */
    public void createIndexes() {
        logger.info("Creating indexes");

        // SAI (Storage-Attached Index) for Cassandra 5.0
        // Falls back to secondary index on older versions
        try {
            session.execute("CREATE CUSTOM INDEX IF NOT EXISTS sensor_data_temp_idx " +
                    "ON sensor_data (temperature) " +
                    "USING 'StorageAttachedIndex'");
            logger.info("Created SAI index on temperature");
        } catch (Exception e) {
            logger.warn("SAI not available, skipping: {}", e.getMessage());
        }

        // Regular secondary index
        session.execute("CREATE INDEX IF NOT EXISTS alert_type_idx ON alert_events (alert_type)");
        logger.info("Created secondary index on alert_type");
    }

    /**
     * Insert sample data to demonstrate various write patterns.
     */
    public void insertSampleData() {
        logger.info("Inserting sample data");

        // Insert sensor metadata with LWT (IF NOT EXISTS)
        String insertMetadata = "INSERT INTO sensor_metadata " +
                "(sensor_id, registration_id, sensor_type, location, latitude, longitude, " +
                "registration_time, config) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";

        for (int i = 1; i <= 5; i++) {
            session.execute(SimpleStatement.builder(insertMetadata)
                    .addPositionalValue("sensor-" + i)
                    .addPositionalValue(Uuids.timeBased())
                    .addPositionalValue("temperature_humidity")
                    .addPositionalValue("Building-A-Floor-" + i)
                    .addPositionalValue(37.7749 + (i * 0.001))
                    .addPositionalValue(-122.4194 + (i * 0.001))
                    .addPositionalValue(Instant.now())
                    .addPositionalValue(java.util.Map.of(
                            "interval", "60",
                            "unit", "celsius"
                    ))
                    .build());
        }

        logger.info("Sample data inserted");
    }
}
