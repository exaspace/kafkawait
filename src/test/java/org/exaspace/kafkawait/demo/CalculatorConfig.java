package org.exaspace.kafkawait.demo;

/**
 * Some configuration settings for the simple demo application.
 */
public class CalculatorConfig {
    static final int HTTP_LISTEN_PORT = 8000;
    static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    static final String KAFKA_REQUEST_TOPIC = "requests";
    static final String KAFKA_RESPONSE_TOPIC = "responses";
}
