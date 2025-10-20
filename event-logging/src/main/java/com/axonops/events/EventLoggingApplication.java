package com.axonops.events;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real-Time Event Logging Application demonstrating Cassandra Java Driver features.
 * Showcases query builder API, mapper API, reactive streams, and monitoring capabilities.
 */
public class EventLoggingApplication {
    private static final Logger logger = LoggerFactory.getLogger(EventLoggingApplication.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        String contactPoint = getArg(args, "--contact-point", "127.0.0.1");
        int port = Integer.parseInt(getArg(args, "--port", "9042"));
        String datacenter = getArg(args, "--datacenter", "datacenter1");
        String mode = getArg(args, "--mode", "setup");
        int eventRate = Integer.parseInt(getArg(args, "--event-rate", "1000"));
        int duration = Integer.parseInt(getArg(args, "--duration", "60"));

        logger.info("Starting Event Logging Application");
        logger.info("Contact Point: {}:{}, Datacenter: {}", contactPoint, port, datacenter);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));

        try (CqlSession session = buildSession(contactPoint, port, datacenter)) {
            logger.info("Connected to Cassandra cluster");

            switch (mode.toLowerCase()) {
                case "setup":
                    setupSchema(session);
                    break;
                case "write":
                    runEventGenerator(session, eventRate, duration);
                    break;
                case "query":
                    runQueryBuilderDemo(session);
                    break;
                case "demo":
                    setupSchema(session);
                    runEventGenerator(session, eventRate, 30);
                    runQueryBuilderDemo(session);
                    break;
                default:
                    logger.error("Unknown mode: {}. Use: setup, write, query, or demo", mode);
                    System.exit(1);
            }

            logger.info("Application completed successfully");
        } catch (Exception e) {
            logger.error("Application error", e);
            System.exit(1);
        }
    }

    private static CqlSession buildSession(String contactPoint, int port, String datacenter) {
        logger.info("Building CqlSession with configuration");

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(datacenter)
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
                        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                        .withString(DefaultDriverOption.RETRY_POLICY_CLASS, "DefaultRetryPolicy")
                        .build())
                .build();
    }

    private static void setupSchema(CqlSession session) {
        logger.info("Setting up schema");
        SchemaSetup schemaSetup = new SchemaSetup(session);
        schemaSetup.createKeyspace();
        schemaSetup.createTables();
        schemaSetup.createIndexes();
        schemaSetup.insertSampleData();
        logger.info("Schema setup completed");
    }

    private static void runEventGenerator(CqlSession session, int eventRate, int duration) {
        logger.info("Starting event generator: {} events/sec for {} seconds", eventRate, duration);
        EventGenerator generator = new EventGenerator(session, eventRate);
        generator.run(duration, running);
        logger.info("Event generation completed");
    }

    private static void runQueryBuilderDemo(CqlSession session) {
        logger.info("Running Query Builder demonstrations");
        QueryBuilderDemo demo = new QueryBuilderDemo(session);
        demo.demonstrateQueryBuilder();
        logger.info("Query Builder demonstrations completed");
    }

    private static String getArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
