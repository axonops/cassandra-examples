package com.axonops.iot;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main IoT Time-Series Application demonstrating Cassandra Java Driver features.
 * This application showcases sensor data ingestion, querying, and various driver capabilities.
 */
public class IotApplication {
    private static final Logger logger = LoggerFactory.getLogger(IotApplication.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        String contactPoint = getArg(args, "--contact-point", "127.0.0.1");
        int port = Integer.parseInt(getArg(args, "--port", "9042"));
        String datacenter = getArg(args, "--datacenter", "datacenter1");
        String mode = getArg(args, "--mode", "setup");
        int writeRate = Integer.parseInt(getArg(args, "--write-rate", "100"));
        int duration = Integer.parseInt(getArg(args, "--duration", "60"));

        logger.info("Starting IoT Time-Series Application");
        logger.info("Contact Point: {}:{}, Datacenter: {}", contactPoint, port, datacenter);

        // Register shutdown hook for graceful shutdown
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
                    runLoadGenerator(session, writeRate, duration);
                    break;
                case "read":
                    runQueryDemo(session);
                    break;
                case "demo":
                    setupSchema(session);
                    runLoadGenerator(session, writeRate, 30);
                    runQueryDemo(session);
                    break;
                default:
                    logger.error("Unknown mode: {}. Use: setup, write, read, or demo", mode);
                    System.exit(1);
            }

            logger.info("Application completed successfully");
        } catch (Exception e) {
            logger.error("Application error", e);
            System.exit(1);
        }
    }

    /**
     * Build CqlSession with comprehensive configuration demonstrating driver options.
     */
    private static CqlSession buildSession(String contactPoint, int port, String datacenter) {
        logger.info("Building CqlSession with advanced configuration");

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(datacenter)
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        // Request timeout
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
                        // Connection pool configuration
                        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 2)
                        // Retry policy
                        .withString(DefaultDriverOption.RETRY_POLICY_CLASS,
                                "DefaultRetryPolicy")
                        // Speculative execution for latency-sensitive queries
                        .withString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                                "ConstantSpeculativeExecutionPolicy")
                        .withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, 2)
                        .withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                                Duration.ofMillis(100))
                        // Load balancing policy
                        .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                                "DefaultLoadBalancingPolicy")
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

    private static void runLoadGenerator(CqlSession session, int writeRate, int duration) {
        logger.info("Starting load generator: {} writes/sec for {} seconds", writeRate, duration);
        LoadGenerator generator = new LoadGenerator(session, writeRate);
        generator.run(duration, running);
        logger.info("Load generation completed");
    }

    private static void runQueryDemo(CqlSession session) {
        logger.info("Running query demonstrations");
        QueryDemo demo = new QueryDemo(session);
        demo.demonstrateQueries();
        logger.info("Query demonstrations completed");
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
