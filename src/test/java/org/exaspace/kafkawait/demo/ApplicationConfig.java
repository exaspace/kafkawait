package org.exaspace.kafkawait.demo;

public class ApplicationConfig {
    static final int HTTP_LISTEN_PORT = 8000;
    static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    static final String KAFKA_REQUEST_TOPIC = "requests";
    static final String KAFKA_RESPONSE_TOPIC = "responses";
}
