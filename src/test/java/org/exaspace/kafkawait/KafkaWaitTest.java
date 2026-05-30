package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


class KafkaWaitTest {

    @Test
    void responseMessageIsReturned() throws ExecutionException, InterruptedException, TimeoutException {
        var kafkaWait = new KafkaWait<String, String, String>(ConsumerRecord::key, Duration.ofMillis(100));
        var responseFuture = kafkaWait.waitFor("someId");
        var rec = new ConsumerRecord<>("topic", 0, 0, "someId", "value");
        kafkaWait.onMessage(rec);
        assertThat(responseFuture.get(10, MILLISECONDS), equalTo(rec));
    }

    @Test
    void noResponseMessageCausesTimeout() {
        var kafkaWait = new KafkaWait<String, String, String>(ConsumerRecord::key, Duration.ofMillis(100));
        var responseFuture = kafkaWait.waitFor("someUnknownId");
        assertThrows(ExecutionException.class, () -> responseFuture.get(200, MILLISECONDS));
    }

    @Test
    void idsAreRemovedWhenResponseReceived() {
        var kafkaWait = new KafkaWait<String, String, String>(ConsumerRecord::key, Duration.ofMillis(100));
        kafkaWait.waitFor("someId");
        assertThat(kafkaWait.size(), equalTo(1));
        kafkaWait.onMessage(new ConsumerRecord<>("topic", 0, 0, "someId", "value"));
        assertThat(kafkaWait.size(), equalTo(0));
    }

    @Test
    void idsAreRemovedWhenNoResponseReceived() throws InterruptedException {
        var kafkaWait = new KafkaWait<String, String, String>(ConsumerRecord::key, Duration.ofMillis(100));
        kafkaWait.waitFor("someId");
        assertThat(kafkaWait.size(), equalTo(1));
        Thread.sleep(200);
        assertThat(kafkaWait.size(), equalTo(0));
    }

}
