package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the Greetings uppercase Kafka Streams application.
 *
 * Build: mvn package
 * Run: java -jar target/greetings-streams-1.0-SNAPSHOT.jar
 * or: mvn compile exec:java -Dexec.mainClass=com.example.GreetingsUpperCaseApp
 */
public class GreetingsUpperCaseApp {

  private static final Logger log = LoggerFactory.getLogger(GreetingsUpperCaseApp.class);

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-uppercase-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    // Start reading from the beginning so we don't miss messages produced before
    // startup
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    Topology topology = GreetingsTopology.build();
    log.info("Topology description:\n{}", topology.describe());

    KafkaStreams streams = new KafkaStreams(topology, props);

    // Graceful shutdown on Ctrl+C
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        log.info("Shutting down Kafka Streams...");
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      log.info("Kafka Streams started. Consuming from '{}', producing to '{}'",
          GreetingsTopology.INPUT_TOPIC, GreetingsTopology.OUTPUT_TOPIC);
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    log.info("Application stopped.");
  }
}
