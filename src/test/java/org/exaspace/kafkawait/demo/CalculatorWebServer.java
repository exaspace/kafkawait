package org.exaspace.kafkawait.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.exaspace.kafkawait.KafkaWaitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.exaspace.kafkawait.demo.CalculatorConfig.*;

public class CalculatorWebServer {
    private static final Logger LOG = LoggerFactory.getLogger(CalculatorWebServer.class);

    private static final Duration MAX_WAIT_TIME = Duration.ofSeconds(1);

    private final AtomicLong requestId;
    private final KafkaWaitService<String, String, String, String, Long> kafkaWaitService;

    public CalculatorWebServer() {
        this.requestId = new AtomicLong(0);
        kafkaWaitService = new KafkaWaitService<>(
                KAFKA_BOOTSTRAP_SERVER,
                KAFKA_REQUEST_TOPIC,
                KAFKA_RESPONSE_TOPIC,
                this::extractIdFromMessage,
                MAX_WAIT_TIME,
                new StringSerializer(),
                new StringSerializer(),
                new HashMap<>(),
                new StringDeserializer(),
                new StringDeserializer(),
                new HashMap<>()
        );
    }

    public void run() throws Exception {
        Server server = new Server(HTTP_LISTEN_PORT);
        server.setHandler(new JettyRequestHandler());
        server.start();
        LOG.info("Started jetty web server on port " + HTTP_LISTEN_PORT);
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
            Map<String, String> result = new HashMap<>();
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

    /*
     * The IdExtractor for our demo application parses out the request ID from the message body
     */
    private Long extractIdFromMessage(ConsumerRecord<String, String> consumerRecord) {
        String jsonMessage = consumerRecord.value();
        CalculatorMessage msg = CalculatorMessage.fromJson(jsonMessage);
        return msg.messageId;
    }

    private String handleWebRequest(String requestUri, Map<String, String> queryParams) {

        Long id = requestId.incrementAndGet();

        LOG.info("{} {}", id, requestUri);

        Integer x = Integer.parseInt(queryParams.get("x"));
        Integer y = Integer.parseInt(queryParams.get("y"));

        CalculatorMessage cm = new CalculatorMessage();
        cm.messageId = id;
        cm.operation = "multiply";
        cm.args = Arrays.asList(x, y);

        /*
         * Submit this web request for processing in our Kafka based back end service.
         */
        Future<ConsumerRecord<String, String>> responseFuture = kafkaWaitService.processRequest(id, cm.toJson());

        try {
            /*
             * We can safely block on the returned future without passing an explicit timeout here as KafkaWait
             * will fail this future automatically for us after the timeout we passed to the KafkaWait constructor
             */
            String responseString = responseFuture.get().value();
            return CalculatorMessage.fromJson(responseString).result.toString() + "\n";
        } catch (Exception e) {
            if (e.getCause() instanceof TimeoutException) return "timeout - maybe event processor not running!\n";
            else return e.getMessage();
        }
    }

    public static void main(String[] args) throws Exception {
        new CalculatorWebServer().run();
    }
}
