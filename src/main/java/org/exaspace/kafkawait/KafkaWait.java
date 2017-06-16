package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

/**
 * KafkaWait maintains an auto-expiring memory cache of message IDs (passed in the waitFor function) that are
 * later matched to IDs extracted from ConsumerRecords passed in the onMessage function.
 * The client receives a future which will contain any matching ConsumerRecord that has been passed to onMessage
 * within the timeout. If the timeout is exceeded, the future will be failed with a TimeoutException.
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

    public int size() {
        return cache.size();
    }

    private void cleanup() {
        final long now = System.currentTimeMillis();
        for (final Map.Entry<T, Long> e : timestamps.entrySet()) {
            final long expiryTime = e.getValue() + maxWait.toMillis();
            if (expiryTime <= now) {
                final T id = e.getKey();
                final CompletableFuture<ConsumerRecord<K,V>> record = cache.get(id);
                if (record != null)
                    record.completeExceptionally(new TimeoutException());
                remove(id);
            }
        }
    }

    private void remove(final T id) {
        cache.remove(id);
        timestamps.remove(id);
    }

}
