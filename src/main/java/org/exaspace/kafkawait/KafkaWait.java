package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

/**
 * KafkaWait is used to wait for individual Kafka messages that are identified by a user defined "ID"
 * (this ID can be anything - the user simply supplies a function to extract an ID of type T given
 * a Kafka message). If a message with matching ID arrives within the specified timeout, the returned
 * Future will be completed with the message, else completed with a timeout exception.
 *
 * Clients notify KafkaWait of incoming messages by calling the onMessage function. That is, clients can
 * perform the Kafka message consumption however they like (and can consume from multiple topics if they like),
 * as long as they pass each received ConsumerRecord into the onMessage() function.
 *
 * Correct usage of this class thus consists of two steps (i) setting up a Kafka consumer for whatever topics the client
 * wishes to wait for messages on, calling onMessage(record) for each received message (ii) calling waitFor(id). Usually
 * these calls will be made from different threads.
 *
 * KafkaWait is implemented using a cache of message IDs with timestamps. These IDs are
 * matched to IDs extracted from ConsumerRecords as they arrive. A single scheduled task is used to
 * evict expired cache entries.
 *
 * @param <K> Key type for the Kafka ConsumerRecord
 * @param <V> Value type for the Kafka ConsumerRecord
 * @param <T> Type of the ID to be extracted from the ConsumerRecord
 */
public class KafkaWait<K,V,T> {

    private final IdExtractor<K, V, T> idExtractor;
    private final Duration maxWait;
    private final Map<T, CompletableFuture<ConsumerRecord<K,V>>> cache = new ConcurrentHashMap<>();
    private final Map<T, Long> timestamps = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public KafkaWait(final IdExtractor<K,V,T> idExtractor, final Duration timeout) {
        this.idExtractor = idExtractor;
        this.maxWait = timeout;
        scheduler.scheduleWithFixedDelay(this::cleanup, timeout.toMillis(), timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public Future<ConsumerRecord<K,V>> waitFor(final T id) {
        final CompletableFuture<ConsumerRecord<K,V>> f = new CompletableFuture<>();
        cache.put(id, f);
        timestamps.put(id, System.currentTimeMillis());
        return f;
    }

    public void onMessage(final ConsumerRecord<K,V> record) {
        final T id = this.idExtractor.extractId(record);
        final CompletableFuture<ConsumerRecord<K,V>> f = cache.get(id);
        if (f != null) {
            remove(id);
            f.complete(record);
        }
    }

    public void fail(final T id, final Exception e) {
        final CompletableFuture<ConsumerRecord<K,V>> f = cache.get(id);
        if (f != null) {
            f.completeExceptionally(e);
            remove(id);
        }
    }

    public int size() {
        return cache.size();
    }

    private void cleanup() {
        final long now = System.currentTimeMillis();
        for (final Map.Entry<T, Long> e : timestamps.entrySet()) {
            final long expiryTime = e.getValue() + maxWait.toMillis();
            if (expiryTime <= now) {
                final T id = e.getKey();
                final CompletableFuture<ConsumerRecord<K,V>> future = cache.get(id);
                if (future != null)
                    future.completeExceptionally(new TimeoutException());
                remove(id);
            }
        }
    }

    private void remove(final T id) {
        cache.remove(id);
        timestamps.remove(id);
    }

}
