package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


/**
 * Threadsafe utility which provides a simple high level method call interface on top of asynchronous Kafka
 * message processing, using a KafkaWait instance internally to track message IDs.
 * <p>
 * This service handles messaging using its own Kafka consumer and producer - if you want more control
 * then use KafkaWait directly.
 * <p>
 * Clients of this service are unaware of Kafka and simply call the 'processRequest' method which deals with the Kafka
 * send and receive interactions, returning a Future which ultimately captures the corresponding
 * response or times out. The service uses a Kafka producer to send the request message and a separate
 * Kafka consumer thread to receive and correlate responses.
 *
 * @param <K1> Type of the Kafka key used for the request topic
 * @param <V1> Type of the Kafka value used for the request topic
 * @param <K2> Type of the Kafka key used for the response topic
 * @param <V2> Type of the Kafka value used for the response topic
 * @param <T>  Type of the ID used to match request and response messages
 */
public class KafkaWaitService<K1, V1, K2, V2, T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWaitService.class);

    private final String bootstrapServers;
    private final String requestTopic;
    private final String responseTopic;

    private final KafkaWait<K2, V2, T> kafkaWait;
    private final KafkaProducer<K1, V1> producer;
    private final KafkaConsumer<K2, V2> consumer;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    // Value to pass to Kafka consumer poll (of minimal importance as poll returns as soon as data is present)
    private int consumerPollTimeoutMillis = 10000;

    // Maximum wait time for producer send() future to return
    private int producerSendTimeoutMillis = 500;

    public KafkaWaitService(String bootstrapServers,
                            String requestTopic,
                            String responseTopic,

                            IdExtractor<K2, V2, T> idExtractor,
                            Duration timeout,

                            Serializer<K1> producerKeySerializer,
                            Serializer<V1> producerValueSerializer,
                            Map<String, Object> producerProps,

                            Deserializer<K2> consumerKeyDeserializer,
                            Deserializer<V2> consumerValueDeserializer,
                            Map<String, Object> consumerProps) {

        this.bootstrapServers = bootstrapServers;
        this.requestTopic = requestTopic;
        this.responseTopic = responseTopic;
        producer = newKafkaProducer(producerProps, producerKeySerializer, producerValueSerializer);
        consumer = newKafkaConsumer(consumerProps, consumerKeyDeserializer, consumerValueDeserializer);
        kafkaWait = new KafkaWait<>(idExtractor, timeout);
        executorService.submit(this::listenForKafkaResponses);
    }

    /**
     * Send a message into Kafka and return a Future containing the response (sets the Kafka key to null).
     *
     * @param id    the ID of this request
     * @param value the Kafka request message value
     * @return the Future response record
     */
    public Future<ConsumerRecord<K2, V2>> processRequest(T id, V1 value) {
        return processRequest(id, null, value);
    }

    /**
     * Send a message into Kafka and return a Future containing the response.
     *
     * @param id    The ID of this request
     * @param key   the Kafka request message key
     * @param value the Kafka request message value
     * @return the Future response record
     */
    public Future<ConsumerRecord<K2, V2>> processRequest(T id, K1 key, V1 value) {
        Future<ConsumerRecord<K2, V2>> res = kafkaWait.waitFor(id);
        try {
            blockingKafkaSend(key, value);
        } catch (Exception e) {
            LOG.error("Error publishing id={} msg={}", id, e.getMessage());
            kafkaWait.fail(id, e);
        }
        return res;
    }

    public int getConsumerPollTimeoutMillis() {
        return consumerPollTimeoutMillis;
    }

    public void setConsumerPollTimeoutMillis(final int consumerPollTimeoutMillis) {
        this.consumerPollTimeoutMillis = consumerPollTimeoutMillis;
    }

    public int getProducerSendTimeoutMillis() {
        return producerSendTimeoutMillis;
    }

    public void setProducerSendTimeoutMillis(final int producerSendTimeoutMillis) {
        this.producerSendTimeoutMillis = producerSendTimeoutMillis;
    }

    private void listenForKafkaResponses() {
        consumer.subscribe(Arrays.asList(responseTopic));
        while (true) {
            ConsumerRecords<K2, V2> recs = consumer.poll(consumerPollTimeoutMillis);
            for (ConsumerRecord<K2, V2> record : recs.records(responseTopic)) {
                kafkaWait.onMessage(record);
            }
        }
    }

    private void blockingKafkaSend(K1 key, V1 value) throws InterruptedException,
            ExecutionException, TimeoutException {
        producer.send(new ProducerRecord<K1, V1>(requestTopic, key, value))
                .get(producerSendTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private KafkaProducer<K1, V1> newKafkaProducer(Map<String, Object> props,
                                                   Serializer<K1> keySerializer,
                                                   Serializer<V1> valueSerializer) {
        Map<String, Object> defaultProducerProps = new HashMap<>();
        defaultProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        defaultProducerProps.put(ProducerConfig.ACKS_CONFIG, "1");

        return new KafkaProducer<>(merge(defaultProducerProps, props), keySerializer, valueSerializer);
    }

    private KafkaConsumer<K2, V2> newKafkaConsumer(Map<String, Object> props,
                                                   Deserializer<K2> keyDeserializer,
                                                   Deserializer<V2> valueDeserializer) {
        Map<String, Object> defaultConsumerProps = new HashMap<>();
        defaultConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        defaultConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        defaultConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(merge(defaultConsumerProps, props), keyDeserializer, valueDeserializer);
    }

    private Map<String, Object> merge(Map<String, Object> defaultProps,
                                      Map<String, Object> overrideProps) {
        Map<String, Object> ret = new HashMap<>();
        ret.putAll(defaultProps);
        ret.putAll(overrideProps);
        return ret;
    }

}