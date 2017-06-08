package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface IdExtractor<K,V,T> {

    public T extractId(ConsumerRecord<K,V> consumerRecord);

}
