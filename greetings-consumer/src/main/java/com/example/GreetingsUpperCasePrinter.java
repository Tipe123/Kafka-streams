package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Plain Kafka consumer that reads from "greetings-uppercase" and prints
 * each message to the terminal.
 *
 * Run: mvn compile exec:java
 * -Dexec.mainClass=com.example.GreetingsUpperCasePrinter
 * or: java -jar target/greetings-consumer-1.0-SNAPSHOT.jar
 */
public class GreetingsUpperCasePrinter {

  private static final Logger log = LoggerFactory.getLogger(GreetingsUpperCasePrinter.class);
  private static final String TOPIC = "greetings-uppercase";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "greetings-printer-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // Read from the beginning so nothing is missed on first start
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(TOPIC));

    // Graceful shutdown on Ctrl+C
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down consumer...");
      consumer.wakeup();
    }));

    log.info("Listening on topic '{}'...", TOPIC);
    System.out.println("========================================");
    System.out.println(" Waiting for uppercase greetings...    ");
    System.out.println("========================================");

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(">>> [partition=%d offset=%d] %s%n",
              record.partition(), record.offset(), record.value());
        }
      }
    } catch (org.apache.kafka.common.errors.WakeupException e) {
      // expected on shutdown
    } finally {
      consumer.close();
      log.info("Consumer closed.");
    }
  }
}
