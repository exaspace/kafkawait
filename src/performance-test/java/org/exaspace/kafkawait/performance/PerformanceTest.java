package org.exaspace.kafkawait.performance;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.collect.Iterables;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.exaspace.kafkawait.demo.CalculatorConfig;
import org.exaspace.kafkawait.demo.CalculatorEventProcessor;
import org.exaspace.kafkawait.demo.CalculatorWebServer;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;


public class PerformanceTest {
    private static final Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

    private static final int INITIAL_WAIT_MS = 10000;
    private static final int KAFKA_PORT = 19092;
    private static final int CALCULATOR_WEBSERVER_PORT = 18000;

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create(KAFKA_PORT));

    @Test
    public void testWebPerformance() {

        int brokerPort = kafkaRule.helper().kafkaPort();
        System.setProperty("KAFKA_PORT", String.valueOf(brokerPort));
        System.setProperty("HTTP_LISTEN_PORT", String.valueOf(CALCULATOR_WEBSERVER_PORT));
        LOG.info("Started Kafka on port {}", brokerPort);

        ScheduledExecutorService webserverScheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService processorScheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            webserverScheduler.submit(() -> new CalculatorWebServer().run());
            processorScheduler.submit(() -> new CalculatorEventProcessor().run());

            LOG.info("Waiting for web server to be available");
            sleep(INITIAL_WAIT_MS);
            LOG.info("...hopefully web server is available");

            runTests();

        } finally {
            processorScheduler.shutdown();
            webserverScheduler.shutdown();
        }
    }

    void runTests() {

        final List<Multiplication> dataSet = generateDataSet(1000);
        List<BatchDefinition<List<Multiplication>>> batches = Arrays.asList(
                new BatchDefinition<>(100, 100, 10, dataSet),
                new BatchDefinition<>(5000, 5000, 0, dataSet),
                new BatchDefinition<>(5000, 0, 0, dataSet)
        );
        List<BatchResult> results = new ArrayList<>(batches.size());
        for (BatchDefinition<List<Multiplication>> batch : batches) {
            LOG.info("Submitting batch of {} requests", batch.numRequests);
            BatchResult result = submitRequests(batch);
            Stats stats = new Stats(batch, result);
            results.add(result);
            LOG.info("Batch finished. Requests: {} Errors: {} Mean: {}ms ({} requests per second)",
                    stats.numRequests, result.errorCount.get(), stats.msPerRequest, stats.perSec);

            if (result.errorCount.get() > 0) {
                break;
            }
            if (batch.postBatchSleepMillis > 0) {
                sleep(batch.postBatchSleepMillis);
            }
        }
        for (BatchResult res : results) {
            assertThat(res.errorCount.get(), equalTo(0));
        }
        BatchDefinition<?> lastBatch = Iterables.getLast(batches);
        BatchResult lastResult = Iterables.getLast(results);
        Stats stats = new Stats(lastBatch, lastResult);
        assertThat(stats.perSec, greaterThan(100.0));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Stats<T> {
        final int numRequests;
        final double msPerRequest;
        final double perSec;

        Stats(BatchDefinition batch, BatchResult res) {
            this.numRequests = batch.numRequests;
            this.msPerRequest = res.totalTimeMillis / (double) batch.numRequests;
            this.perSec = res.totalTimeMillis == 0 ? 0 : (batch.numRequests / (double) res.totalTimeMillis) * 1000.0;
        }
    }

    private static class BatchDefinition<T> {
        final int numRequests;
        final long postBatchSleepMillis;
        final long interRequestSleepMillis;
        final T dataset;

        BatchDefinition(int numRequests, long postBatchSleepMillis, long interRequestSleepMillis, T dataset) {
            this.numRequests = numRequests;
            this.postBatchSleepMillis = postBatchSleepMillis;
            this.interRequestSleepMillis = interRequestSleepMillis;
            this.dataset = dataset;
        }
    }

    private static class BatchResult {
        final AtomicInteger errorCount;
        final long totalTimeMillis;

        BatchResult(long totalTimeMillis, AtomicInteger errorCount) {
            this.totalTimeMillis = totalTimeMillis;
            this.errorCount = errorCount;
        }
    }

    private static class Multiplication {
        final int x;
        final int y;
        final int res;

        Multiplication(int x, int y, int res) {
            this.x = x;
            this.y = y;
            this.res = res;
        }
    }

    private List<Multiplication> generateDataSet(int n) {
        int modulus = Double.valueOf(Math.floor(Math.sqrt(Integer.MAX_VALUE))).intValue();
        Random rand = new Random();
        List<Multiplication> data = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int x = rand.nextInt(modulus + 1);
            int y = rand.nextInt(modulus + 1);
            data.add(new Multiplication(x, y, x * y));
        }
        return data;
    }

    private BatchResult submitRequests(BatchDefinition<List<Multiplication>> batch) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
        httpclient.start();
        AtomicInteger errorCount = new AtomicInteger(0);
        long startTimeMs = System.currentTimeMillis();
        try {
            Collection<Multiplication> data = batch.dataset;
            CountDownLatch latch = new CountDownLatch(batch.numRequests);
            int dataSetSize = data.size();
            for (int i = 0; i < batch.numRequests; i++) {
                Multiplication m = batch.dataset.get(i % dataSetSize);
                String uri = CalculatorConfig.HTTP_URL + "/multiply?x=" + m.x + "&y=" + m.y;
                HttpGet request = new HttpGet(uri);
                httpclient.execute(request, new FutureCallback<HttpResponse>() {

                    @Override
                    public void completed(final HttpResponse response) {
                        try {
                            int code = response.getStatusLine().getStatusCode();
                            String content = EntityUtils.toString(response.getEntity());
                            if (code != 200) {
                                errorCount.incrementAndGet();
                                LOG.error("HTTP error received: {} {}", code, content);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        latch.countDown();
                    }

                    @Override
                    public void failed(final Exception ex) {
                        latch.countDown();
                        errorCount.incrementAndGet();
                        LOG.error(request.getRequestLine() + "->" + ex);
                    }

                    @Override
                    public void cancelled() {
                        latch.countDown();
                        errorCount.incrementAndGet();
                        LOG.error(request.getRequestLine() + " cancelled");
                    }

                });
                if (batch.interRequestSleepMillis > 0) Thread.sleep(batch.interRequestSleepMillis);
            }
            latch.await(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                httpclient.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        long endTimeMs = System.currentTimeMillis();
        long duration = endTimeMs - startTimeMs;
        return new BatchResult(duration, errorCount);
    }

}
