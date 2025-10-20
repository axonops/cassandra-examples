package com.axonops.events;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema setup for Event Logging application.
 * Demonstrates keyspace creation, various table types, and index management.
 */
public class SchemaSetup {
    private static final Logger logger = LoggerFactory.getLogger(SchemaSetup.class);
    private final CqlSession session;

    public SchemaSetup(CqlSession session) {
        this.session = session;
    }

    public void createKeyspace() {
        logger.info("Creating keyspace 'events_demo'");

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS events_demo " +
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} " +
                "AND durable_writes = true";

        session.execute(createKeyspace);
        session.execute("USE events_demo");
        logger.info("Keyspace created and set as active");
    }

    public void createTables() {
        createEventLogTable();
        createEventStatisticsTable();
        createUserActivityTable();
        createAlertRulesTable();
    }

    /**
     * Event log table partitioned by source_id and hour bucket.
     * Demonstrates: time-series partitioning, JSON storage, TWCS compaction
     */
    private void createEventLogTable() {
        logger.info("Creating event_log table");

        String createTable = "CREATE TABLE IF NOT EXISTS event_log (" +
                "source_id text, " +
                "hour_bucket text, " +  // Format: YYYY-MM-DD-HH
                "event_time timestamp, " +
                "event_id uuid, " +
                "severity text, " +
                "category text, " +
                "message text, " +
                "payload_json text, " +  // Full event payload as JSON
                "PRIMARY KEY ((source_id, hour_bucket), event_time, event_id)) " +
                "WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC) " +
                "AND compaction = {" +
                "'class': 'TimeWindowCompactionStrategy', " +
                "'compaction_window_unit': 'HOURS', " +
                "'compaction_window_size': 1} " +
                "AND default_time_to_live = 604800";  // 7 days

        session.execute(createTable);
        logger.info("event_log table created");
    }

    /**
     * Event statistics with counters.
     * Demonstrates: counter columns, rate limiting
     */
    private void createEventStatisticsTable() {
        logger.info("Creating event_statistics table");

        String createTable = "CREATE TABLE IF NOT EXISTS event_statistics (" +
                "category text, " +
                "time_bucket text, " +  // Format: YYYY-MM-DD-HH
                "event_count counter, " +
                "error_count counter, " +
                "warning_count counter, " +
                "PRIMARY KEY (category, time_bucket)) " +
                "WITH CLUSTERING ORDER BY (time_bucket DESC)";

        session.execute(createTable);
        logger.info("event_statistics table created");
    }

    /**
     * User activity tracking with dynamic attributes.
     * Demonstrates: MAP for dynamic attributes, TTL for session windowing
     */
    private void createUserActivityTable() {
        logger.info("Creating user_activity table");

        String createTable = "CREATE TABLE IF NOT EXISTS user_activity (" +
                "user_id text, " +
                "session_id uuid, " +
                "activity_time timestamp, " +
                "action text, " +
                "attributes map<text, text>, " +  // Dynamic attributes
                "PRIMARY KEY (user_id, session_id, activity_time)) " +
                "WITH CLUSTERING ORDER BY (session_id DESC, activity_time DESC) " +
                "AND default_time_to_live = 86400";  // 24 hours session window

        session.execute(createTable);
        logger.info("user_activity table created");
    }

    /**
     * Alert rules with frozen collections.
     * Demonstrates: FROZEN collections, versioning with timestamps
     */
    private void createAlertRulesTable() {
        logger.info("Creating alert_rules table");

        String createTable = "CREATE TABLE IF NOT EXISTS alert_rules (" +
                "rule_id uuid, " +
                "version_time timestamp, " +
                "rule_name text, " +
                "enabled boolean, " +
                "conditions frozen<map<text, text>>, " +  // FROZEN for complex criteria
                "actions frozen<list<text>>, " +
                "tags set<text>, " +
                "PRIMARY KEY (rule_id, version_time)) " +
                "WITH CLUSTERING ORDER BY (version_time DESC)";

        session.execute(createTable);
        logger.info("alert_rules table created");
    }

    public void createIndexes() {
        logger.info("Creating indexes");

        // SAI index for Cassandra 5.0
        try {
            session.execute("CREATE CUSTOM INDEX IF NOT EXISTS event_log_severity_idx " +
                    "ON event_log (severity) " +
                    "USING 'StorageAttachedIndex'");
            logger.info("Created SAI index on severity");
        } catch (Exception e) {
            logger.warn("SAI not available: {}", e.getMessage());
        }

        // Regular secondary index on category
        session.execute("CREATE INDEX IF NOT EXISTS event_log_category_idx ON event_log (category)");
        logger.info("Created secondary index on category");

        // Index on user activity action
        session.execute("CREATE INDEX IF NOT EXISTS user_activity_action_idx ON user_activity (action)");
        logger.info("Created index on user_activity.action");
    }

    public void insertSampleData() {
        logger.info("Inserting sample data");

        Instant now = Instant.now();
        String hourBucket = formatHourBucket(now);

        // Insert sample events
        for (int i = 1; i <= 5; i++) {
            String sourceId = "source-" + i;
            insertSampleEvent(sourceId, hourBucket, now.minusSeconds(i * 60));
        }

        // Insert sample alert rules
        insertSampleAlertRules();

        logger.info("Sample data inserted");
    }

    private void insertSampleEvent(String sourceId, String hourBucket, Instant eventTime) {
        String insert = "INSERT INTO event_log " +
                "(source_id, hour_bucket, event_time, event_id, severity, category, message, payload_json) " +
                "VALUES (?, ?, ?, uuid(), ?, ?, ?, ?)";

        session.execute(SimpleStatement.builder(insert)
                .addPositionalValue(sourceId)
                .addPositionalValue(hourBucket)
                .addPositionalValue(eventTime)
                .addPositionalValue("INFO")
                .addPositionalValue("application")
                .addPositionalValue("Application started")
                .addPositionalValue("{\"app\":\"demo\",\"version\":\"1.0\"}")
                .build());
    }

    private void insertSampleAlertRules() {
        String insert = "INSERT INTO alert_rules " +
                "(rule_id, version_time, rule_name, enabled, conditions, actions, tags) " +
                "VALUES (uuid(), ?, ?, ?, ?, ?, ?)";

        session.execute(SimpleStatement.builder(insert)
                .addPositionalValue(Instant.now())
                .addPositionalValue("High Error Rate")
                .addPositionalValue(true)
                .addPositionalValue(Map.of("threshold", "100", "window", "5m"))
                .addPositionalValue(List.of("email", "slack"))
                .addPositionalValue(Set.of("critical", "errors"))
                .build());
    }

    private String formatHourBucket(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                .format(instant.atZone(ZoneId.systemDefault()));
    }
}
