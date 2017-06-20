package org.exaspace.kafkawait.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


import static org.exaspace.kafkawait.demo.CalculatorConfig.*;

/**
 * Event driven component which implements "business logic" of a calculator.
 * <p>
 * It receives input from one Kafka topic and produces output to another Kafka topic.
 * <p>
 * Input and output messages are in JSON format.
 *
 */
public class CalculatorEventProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CalculatorEventProcessor.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    public CalculatorEventProcessor() throws Exception {
        consumer = newKafkaConsumer();
        producer = newKafkaProducer();
    }

    public void run() {

        LOG.info("starting event processor");

        consumer.subscribe(Arrays.asList(KAFKA_REQUEST_TOPIC));

        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : recs.records(KAFKA_REQUEST_TOPIC)) {
                String outputJson = processMessage(record.value());
                publish(outputJson);
            }
        }
    }

    private String processMessage(String json) {
        try {
            CalculatorMessage cm = CalculatorMessage.fromJson(json);
            switch(cm.operation) {
                case "multiply":
                    cm.result = cm.args.get(0) * cm.args.get(1);
                    break;
            }
            return cm.toJson();
        }
        catch (Exception e) {
            CalculatorMessage cm = new CalculatorMessage();
            cm.isError = true;
            return cm.toJson();
        }
    }

    private void publish(String message) {
        try {
            ProducerRecord<String, String> pr = new ProducerRecord<>(KAFKA_RESPONSE_TOPIC, message);
            producer.send(pr).get(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.info("error sending response to kafka: " + e.getMessage());
        }
    }

    private KafkaConsumer<String, String> newKafkaConsumer() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
    }

    private KafkaProducer<String, String> newKafkaProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    }

    public static void main(String[] args) throws Exception {
        new CalculatorEventProcessor().run();
    }
}