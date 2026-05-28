package org.exaspace.kafkawait.performance;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
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
    private static final int CALCULATOR_WEBSERVER_PORT = 18000;

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void testWebPerformance() {
        var brokerPort = kafkaRule.helper().kafkaPort();
        System.setProperty("KAFKA_PORT", String.valueOf(brokerPort));
        System.setProperty("HTTP_LISTEN_PORT", String.valueOf(CALCULATOR_WEBSERVER_PORT));
        LOG.info("Started Kafka on port {}", brokerPort);

        var webserverScheduler = Executors.newSingleThreadScheduledExecutor();
        var processorScheduler = Executors.newSingleThreadScheduledExecutor();
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
        var dataSet = generateDataSet(1000);
        var batches = Arrays.asList(
                new BatchDefinition<>(100, 100, 10, dataSet),
                new BatchDefinition<>(5000, 5000, 0, dataSet),
                new BatchDefinition<>(5000, 0, 0, dataSet)
        );
        var results = new ArrayList<BatchResult>(batches.size());
        for (var batch : batches) {
            LOG.info("Submitting batch of {} requests", batch.numRequests());
            var result = submitRequests(batch);
            var stats = new Stats(batch, result);
            results.add(result);
            LOG.info("Batch finished. Requests: {} Errors: {} Mean: {}ms ({} requests per second)",
                    stats.numRequests(), result.errorCount().get(), stats.msPerRequest(), stats.perSec());

            if (result.errorCount().get() > 0) {
                break;
            }
            if (batch.postBatchSleepMillis() > 0) {
                sleep(batch.postBatchSleepMillis());
            }
        }
        for (var res : results) {
            assertThat(res.errorCount().get(), equalTo(0));
        }
        var lastBatch = batches.get(batches.size() - 1);
        var lastResult = results.get(results.size() - 1);
        var stats = new Stats(lastBatch, lastResult);
        assertThat(stats.perSec(), greaterThan(100.0));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Stats(int numRequests, double msPerRequest, double perSec) {
        Stats(BatchDefinition<?> batch, BatchResult res) {
            this(
                    batch.numRequests(),
                    res.totalTimeMillis() / (double) batch.numRequests(),
                    res.totalTimeMillis() == 0 ? 0 : (batch.numRequests() / (double) res.totalTimeMillis()) * 1000.0
            );
        }
    }

    private record BatchDefinition<T>(int numRequests, long postBatchSleepMillis, long interRequestSleepMillis, T dataset) {
    }

    private record BatchResult(long totalTimeMillis, AtomicInteger errorCount) {
    }

    private record Multiplication(int x, int y, int res) {
    }

    private List<Multiplication> generateDataSet(int n) {
        var modulus = (int) Math.floor(Math.sqrt(Integer.MAX_VALUE));
        var rand = new Random();
        var data = new ArrayList<Multiplication>(n);
        for (var i = 0; i < n; i++) {
            var x = rand.nextInt(modulus + 1);
            var y = rand.nextInt(modulus + 1);
            data.add(new Multiplication(x, y, x * y));
        }
        return data;
    }

    private BatchResult submitRequests(BatchDefinition<List<Multiplication>> batch) {
        var requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        var httpclient = HttpAsyncClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
        httpclient.start();
        var errorCount = new AtomicInteger(0);
        var startTimeMs = System.currentTimeMillis();
        try {
            var data = batch.dataset();
            var latch = new CountDownLatch(batch.numRequests());
            var dataSetSize = data.size();
            for (var i = 0; i < batch.numRequests(); i++) {
                var m = batch.dataset().get(i % dataSetSize);
                var uri = CalculatorConfig.HTTP_URL + "/multiply?x=" + m.x() + "&y=" + m.y();
                var request = new HttpGet(uri);
                httpclient.execute(request, new FutureCallback<HttpResponse>() {

                    @Override
                    public void completed(final HttpResponse response) {
                        try {
                            var code = response.getStatusLine().getStatusCode();
                            var content = EntityUtils.toString(response.getEntity());
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
                if (batch.interRequestSleepMillis() > 0) Thread.sleep(batch.interRequestSleepMillis());
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
        var endTimeMs = System.currentTimeMillis();
        var duration = endTimeMs - startTimeMs;
        return new BatchResult(duration, errorCount);
    }

}
