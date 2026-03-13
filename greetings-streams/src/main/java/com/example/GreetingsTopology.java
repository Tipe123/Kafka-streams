package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Defines the Kafka Streams topology:
 *
 * greetings → mapValues(toUpperCase) → greetings-uppercase
 */
public class GreetingsTopology {

  public static final String INPUT_TOPIC = "greetings";
  public static final String OUTPUT_TOPIC = "greetings-uppercase";

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.stream(
        INPUT_TOPIC,
        Consumed.with(Serdes.String(), Serdes.String()));

    stream
        .mapValues((ValueMapper<String, String>) String::toUpperCase)
        .peek((key, value) -> System.out.printf("[OUT] key=%s value=%s%n", key, value))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }
}
