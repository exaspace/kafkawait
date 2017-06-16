package org.exaspace.kafkawait.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.exaspace.kafkawait.KafkaWait;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.exaspace.kafkawait.demo.ApplicationConfig.*;

public class CalculatorWebServer {

    private static final Duration WAIT_TIME = Duration.ofSeconds(1);

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaWait<String, String, Long> kafkaWait;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final AtomicLong requestNumber;

    public CalculatorWebServer() {
        producer = newKafkaProducer();
        consumer = newKafkaConsumer();
        kafkaWait = new KafkaWait<>((r) -> extractIdFromJsonMessage(r.value()), WAIT_TIME);
        executorService.submit(this::listenForKafkaResponses);
        this.requestNumber = new AtomicLong(0);
    }

    public void run() throws Exception {
        Server server = new Server(HTTP_LISTEN_PORT);
        server.setHandler(new JettyRequestHandler());
        server.start();
        System.out.println("Started jetty web server on port " + HTTP_LISTEN_PORT);
        server.join();
    }

    private class JettyRequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest req,
                           HttpServletResponse res) throws IOException, ServletException {
            res.setContentType("text/plain");
            res.setStatus(HttpServletResponse.SC_OK);
            if ("/multiply".equals(target)) {
                Map<String, String> params = parseQueryString(req.getQueryString());
                String responseBody = handleWebRequest(req.getRequestURI(), params);
                res.getWriter().println(responseBody);
                baseRequest.setHandled(true);
            }
        }

        private Map<String, String> parseQueryString(String query) {
            Map<String, String> result = new HashMap<String, String>();
            if (query != null) {
                for (String param : query.split("&")) {
                    String pair[] = param.split("=");
                    if (pair.length > 1) {
                        result.put(pair[0], pair[1]);
                    } else {
                        result.put(pair[0], "");
                    }
                }
            }
            return result;
        }

    }

    private String handleWebRequest(String requestUri, Map<String, String> queryParams) {

        Long id = requestNumber.incrementAndGet();

        log("" + id + " " + requestUri);

        Integer x = Integer.parseInt(queryParams.get("x"));
        Integer y = Integer.parseInt(queryParams.get("y"));

        CalculatorMessage cm = new CalculatorMessage();
        cm.messageId = id;
        cm.operation = "multiply";
        cm.args = Arrays.asList(x, y);

        Future<ConsumerRecord<String, String>> responseFuture = sendRequestMessageToKafka(id, cm.toJson());

        try {
            String responseString = responseFuture.get().value();
            return CalculatorMessage.fromJson(responseString).result.toString() + "\n";
        } catch (Exception e) {
            if (e.getCause() instanceof TimeoutException) return "timeout - maybe event processor not running!\n";
            else return e.getMessage();
        }
    }

    private Long extractIdFromJsonMessage(String jsonInput) {
        CalculatorMessage msg = CalculatorMessage.fromJson(jsonInput);
        return msg.messageId;
    }

    private void listenForKafkaResponses() {
        consumer.subscribe(Arrays.asList(KAFKA_RESPONSE_TOPIC));
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : recs.records(KAFKA_RESPONSE_TOPIC)) {
                kafkaWait.onMessage(record);
            }
        }
    }

    private Future<ConsumerRecord<String, String>> sendRequestMessageToKafka(Long id, String requestMessage) {
        Future<ConsumerRecord<String, String>> res = kafkaWait.waitFor(id);
        try {
            producer.send(new ProducerRecord<String, String>(KAFKA_REQUEST_TOPIC, requestMessage))
                    .get(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log(e.getMessage());
        }
        return res;
    }

    private KafkaProducer<String, String> newKafkaProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    }

    private KafkaConsumer<String, String> newKafkaConsumer() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
    }

    private void log(String s) {
        System.out.println("[HTTP] " + System.currentTimeMillis() + " thread=" + Thread.currentThread().getId() + " " + s);
    }

    public static void main(String[] args) throws Exception {
        new CalculatorWebServer().run();
    }
}
