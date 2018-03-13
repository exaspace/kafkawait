package org.exaspace.kafkawait.demo;

/**
 * Some configuration settings for the simple demo application.
 */
public class CalculatorConfig {

    public static final String HTTP_LISTEN_HOST = System.getProperty("HTTP_LISTEN_HOST", "0.0.0.0");

    public static final int HTTP_LISTEN_PORT = Integer.valueOf(System.getProperty("HTTP_LISTEN_PORT", "8000"));

    public static final String HTTP_URL = "http://" + HTTP_LISTEN_HOST + ":" + HTTP_LISTEN_PORT;

    public static final String KAFKA_BOOTSTRAP_SERVER = System.getProperty("KAFKA_HOST", "localhost") + ":" +
            System.getProperty("KAFKA_PORT", "9092");

    public static final String KAFKA_REQUEST_TOPIC = "requests";

    public static final String KAFKA_RESPONSE_TOPIC = "responses";


}
