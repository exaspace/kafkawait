package org.exaspace.kafkawait.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
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

    public CalculatorEventProcessor() {
        consumer = newKafkaConsumer();
        producer = newKafkaProducer();
    }

    public void run() {
        LOG.info("Starting event processor kafka={}", KAFKA_BOOTSTRAP_SERVER);

        consumer.subscribe(Collections.singletonList(KAFKA_REQUEST_TOPIC));
        var consumerPollTimeout = Duration.ofSeconds(10);
        while (true) {
            var recs = consumer.poll(consumerPollTimeout);
            for (var record : recs.records(KAFKA_REQUEST_TOPIC)) {
                var output = processMessage(record.value());
                publish(output);
            }
        }
    }

    private CalculatorMessage processMessage(String json) {
        try {
            var cm = CalculatorMessage.fromJson(json);
            return switch (cm.operation()) {
                case "multiply" -> new CalculatorMessage(
                        cm.messageId(), "multiply", cm.args(),
                        cm.args().get(0) * cm.args().get(1), false
                );
                default -> cm;
            };
        } catch (Exception e) {
            return new CalculatorMessage(null, null, null, null, true);
        }
    }

    private void publish(CalculatorMessage message) {
        try {
            LOG.debug("sending response to kafka: {}", message.messageId());
            var pr = new ProducerRecord<String, String>(KAFKA_RESPONSE_TOPIC, message.toJson());
            producer.send(pr).get(200, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Publish error! {} id={}. Continuing...", e.getMessage(), message.messageId(), e);
        }
    }

    private KafkaConsumer<String, String> newKafkaConsumer() {
        var consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
    }

    private KafkaProducer<String, String> newKafkaProducer() {
        var producerProps = new HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    }

    public static void main(String[] args) {
        new CalculatorEventProcessor().run();
    }
}
