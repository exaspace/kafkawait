package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

public class KafkaWait<K,V,T> {

    private final IdExtractor<K, V, T> idExtractor;
    private final Duration maxWait;
    private final Map<T, CompletableFuture<ConsumerRecord<K,V>>> cache = new ConcurrentHashMap<>();
    private final Map<T, Long> timestamps = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public KafkaWait(IdExtractor<K,V,T> idExtractor, Duration maxWait) {
        this.idExtractor = idExtractor;
        this.maxWait = maxWait;
        scheduler.scheduleWithFixedDelay(this::cleanup, maxWait.toMillis(), maxWait.toMillis(), TimeUnit.MILLISECONDS);
    }

    public Future<ConsumerRecord<K,V>> waitFor(T id) {
        CompletableFuture<ConsumerRecord<K,V>> f = new CompletableFuture<>();
        cache.put(id, f);
        timestamps.put(id, System.currentTimeMillis());
        return f;
    }

    public void onMessage(ConsumerRecord<K,V> r) {
        T id = this.idExtractor.extractId(r);
        CompletableFuture<ConsumerRecord<K,V>> f = cache.get(id);
        if (f != null) {
            remove(id);
            f.complete(r);
        }
    }

    public int size() {
        return cache.size();
    }

    private void cleanup() {
        long now = System.currentTimeMillis();
        for (Map.Entry<T, Long> e : timestamps.entrySet()) {
            long expiryTime = e.getValue() + maxWait.toMillis();
            if (expiryTime <= now) {
                T id = e.getKey();
                CompletableFuture<ConsumerRecord<K,V>> record = cache.get(id);
                if (record != null)
                    record.completeExceptionally(new TimeoutException());
                remove(id);
            }
        }
    }

    private void remove(T id) {
        cache.remove(id);
        timestamps.remove(id);
    }

}
