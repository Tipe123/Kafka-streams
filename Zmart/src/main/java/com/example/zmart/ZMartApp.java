package com.example.zmart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the ZMart Kafka Streams application.
 *
 * <h2>How to run</h2>
 * <pre>
 *   # 1. Start Kafka (see docker-compose-kafka-cluster-confluent.yml in root)
 *   docker compose -f ../docker-compose-kafka-cluster-confluent.yml up -d
 *
 *   # 2. Create the required topics (the topology won't start without them)
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.raw        --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.stores               --partitions 1 --config cleanup.policy=compact
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.masked     --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.enriched   --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.electronics --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.clothing   --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchases.other      --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.purchase.patterns    --partitions 3
 *   kafka-topics --bootstrap-server localhost:9092 --create --topic zmart.rewards              --partitions 3
 *
 *   # 3. Build and run
 *   mvn compile exec:java -Dexec.mainClass=com.example.zmart.ZMartApp
 * </pre>
 *
 * <h2>Key configuration decisions</h2>
 * <dl>
 *   <dt>application.id</dt>
 *   <dd>Uniquely identifies this consumer group. Also used as a prefix for all
 *       internal topics (repartition + changelog). Two JVMs with the same
 *       application.id form a single scalable unit.</dd>
 *
 *   <dt>processing.guarantee = exactly_once_v2</dt>
 *   <dd>Ensures that each purchase is counted toward rewards exactly once,
 *       even on crash-and-restart. This uses Kafka transactions internally.
 *       Cost: ~20-30% throughput reduction vs at_least_once.</dd>
 *
 *   <dt>num.stream.threads = 2</dt>
 *   <dd>Two threads in this JVM instance, each handling a subset of partitions.
 *       Increase this or add more JVM instances to scale horizontally (up to
 *       the number of input topic partitions).</dd>
 *
 *   <dt>num.standby.replicas = 1</dt>
 *   <dd>Maintain one warm standby for each state store partition on another
 *       instance. On failure, recovery is near-instant because the standby
 *       is already up to date.</dd>
 *
 *   <dt>default.deserialization.exception.handler = LogAndContinueExceptionHandler</dt>
 *   <dd>Bad/malformed JSON records are logged and skipped instead of crashing
 *       the application. In production, route them to a dead-letter topic.</dd>
 * </dl>
 */
public class ZMartApp {

    private static final Logger log = LoggerFactory.getLogger(ZMartApp.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = buildProperties();

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = ZMartTopology.build(builder);

        log.info("Topology description:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // ── State listener: log every state transition ────────────────────────
        streams.setStateListener((newState, oldState) ->
                log.info("Kafka Streams state: {} → {}", oldState, newState));

        // ── Uncaught exception handler: restart on recoverable errors ─────────
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Kafka Streams thread", exception);
            // REPLACE_THREAD: restart just the failed thread, not the whole app
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // ── Graceful shutdown via JVM shutdown hook (Ctrl+C / SIGTERM) ────────
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("zmart-shutdown-hook") {
            @Override
            public void run() {
                log.info("Shutdown signal received. Closing Kafka Streams...");
                streams.close(Duration.ofSeconds(30));  // wait up to 30s for graceful close
                latch.countDown();
            }
        });

        try {
            streams.start();
            log.info("ZMart Kafka Streams application started.");
            log.info("Consuming from: {}", ZMartTopics.PURCHASES_RAW);
            log.info("Produces to: {}, {}, {}, {}, {}, {}",
                    ZMartTopics.PURCHASES_MASKED,
                    ZMartTopics.PURCHASES_ELECTRONICS,
                    ZMartTopics.PURCHASES_CLOTHING,
                    ZMartTopics.PURCHASE_PATTERNS,
                    ZMartTopics.REWARDS,
                    ZMartTopics.PURCHASES_ENRICHED);
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("ZMart Kafka Streams application stopped.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Configuration
    // ─────────────────────────────────────────────────────────────────────────

    private static Properties buildProperties() {
        Properties props = new Properties();

        // Core identity — must be the same across all instances of this app
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "zmart-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Default SerDes — String keys everywhere, values use custom JsonSerdes
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());  // overridden per-operation

        // Start reading from the beginning of each topic on first run
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Exactly-once semantics — critical for rewards (no double-counting)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Parallelism within this JVM instance
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");

        // Keep one warm standby for each state store for fast failover
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");

        // Skip malformed records instead of crashing
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());

        // State store directory — RocksDB files go here
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/zmart");

        return props;
    }
}
