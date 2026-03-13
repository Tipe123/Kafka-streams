package com.example;

import org.apache.camel.main.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point – boots Camel Main and registers the greeting route.
 *
 * Run: mvn compile exec:java -Dexec.mainClass=com.example.MainApp
 * Or: mvn package && java -jar target/camel-kafka-producer-1.0-SNAPSHOT.jar
 */
public class MainApp {

  private static final Logger log = LoggerFactory.getLogger(MainApp.class);

  public static void main(String[] args) throws Exception {
    log.info("Starting Camel Kafka Greeting Producer...");

    Main main = new Main();
    main.configure().addRoutesBuilder(new GreetingRoute());

    // Keep the application running until Ctrl+C
    main.run(args);
  }
}
