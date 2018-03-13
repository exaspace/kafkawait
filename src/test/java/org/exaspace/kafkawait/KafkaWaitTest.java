package org.exaspace.kafkawait;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


public class KafkaWaitTest {

    @Test
    public void responseMessageIsReturned() throws ExecutionException, InterruptedException, TimeoutException {
        KafkaWait<String, String, String> kafkaWait = new KafkaWait<>(ConsumerRecord::key, Duration.ofMillis(100));
        Future<ConsumerRecord<String, String>> responseFuture = kafkaWait.waitFor("someId");
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("topic", 0, 0, "someId", "value");
        kafkaWait.onMessage(rec);
        assertThat(responseFuture.get(10, MILLISECONDS), equalTo(rec));
    }

    @Test(expected = TimeoutException.class)
    public void noResponseMessageCausesTimeout() throws TimeoutException, InterruptedException, ExecutionException {
        KafkaWait<String, String, String> kafkaWait = new KafkaWait<>(ConsumerRecord::key, Duration.ofMillis(100));
        Future<ConsumerRecord<String, String>> responseFuture = kafkaWait.waitFor("someUnknownId");
        responseFuture.get(10, MILLISECONDS);
    }

    @Test
    public void idsAreRemovedWhenResponseReceived() throws TimeoutException, InterruptedException, ExecutionException {
        KafkaWait<String, String, String> kafkaWait = new KafkaWait<>(ConsumerRecord::key, Duration.ofMillis(100));
        kafkaWait.waitFor("someId");
        assertThat(kafkaWait.size(), equalTo(1));
        kafkaWait.onMessage(new ConsumerRecord<>("topic", 0, 0, "someId", "value"));
        assertThat(kafkaWait.size(), equalTo(0));
    }

    @Test
    public void idsAreRemovedWhenNoResponseReceived() throws TimeoutException, InterruptedException, ExecutionException {
        KafkaWait<String, String, String> kafkaWait = new KafkaWait<>(ConsumerRecord::key, Duration.ofMillis(100));
        kafkaWait.waitFor("someId");
        assertThat(kafkaWait.size(), equalTo(1));
        Thread.sleep(200);
        assertThat(kafkaWait.size(), equalTo(0));
    }

}