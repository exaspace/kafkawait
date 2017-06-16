package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 *
 * A function to extract an ID from a Kafka ConsumerRecord.
 *
 * @param <K> Key type for the Kafka ConsumerRecord
 * @param <V> Value type for the Kafka ConsumerRecord
 * @param <T> Type of the ID to be extracted from the ConsumerRecord
 */
@FunctionalInterface
public interface IdExtractor<K,V,T> {

    public T extractId(ConsumerRecord<K,V> consumerRecord);

}
