package com.example;

import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Camel route that reads lines from stdin and publishes each one
 * as a message to the Kafka "greetings" topic.
 *
 * Type a message and press Enter to send it. Type "quit" to exit.
 */
public class GreetingRoute extends RouteBuilder {

  private static final Logger log = LoggerFactory.getLogger(GreetingRoute.class);
  private static final String KAFKA_BROKERS = "localhost:9092";
  private static final String TOPIC = "greetings";

  @Override
  public void configure() {
    from("stream:in?promptMessage=Enter message (or 'quit' to exit): ")
        .routeId("greeting-producer")
        // Trim whitespace so blank lines and "quit" detection are reliable
        .process(exchange -> {
          String body = exchange.getMessage().getBody(String.class);
          exchange.getMessage().setBody(body == null ? "" : body.trim());
        })
        .filter(simple("${body} != ''"))
        .choice()
          .when(simple("${body} == 'quit'"))
            .log("Exiting...")
            .process(exchange -> System.exit(0))
          .otherwise()
            .log("Sending: ${body}")
            .to("kafka:" + TOPIC
                + "?brokers=" + KAFKA_BROKERS
                + "&clientId=camel-greeting-producer"
                + "&requestRequiredAcks=-1") // acks=all for durability
        .end();
  }
}
