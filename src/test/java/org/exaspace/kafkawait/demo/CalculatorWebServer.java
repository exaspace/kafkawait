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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
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
        LOG.info(" KAFKA_BOOTSTRAP_SERVER={}", KAFKA_BOOTSTRAP_SERVER);
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

    public void run() {
        var server = new Server(new InetSocketAddress(HTTP_LISTEN_HOST, HTTP_LISTEN_PORT));
        server.setHandler(new JettyRequestHandler());
        try {
            LOG.info("Starting jetty web server on port {}", HTTP_LISTEN_PORT);
            server.start();
            server.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class JettyRequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest req,
                           HttpServletResponse res) throws IOException {
            res.setContentType("text/plain");
            if ("/multiply".equals(target)) {
                var params = parseQueryString(req.getQueryString());
                var webResponse = handleWebRequest(req.getRequestURI(), params);
                res.setStatus(webResponse.code());
                res.getWriter().println(webResponse.body());
                baseRequest.setHandled(true);
            } else {
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }

        private Map<String, String> parseQueryString(String query) {
            Map<String, String> result = new HashMap<>();
            if (query != null) {
                for (var param : query.split("&")) {
                    var pair = param.split("=");
                    result.put(pair[0], pair.length > 1 ? pair[1] : "");
                }
            }
            return result;
        }

    }

    private Long extractIdFromMessage(ConsumerRecord<String, String> consumerRecord) {
        var jsonMessage = consumerRecord.value();
        var msg = CalculatorMessage.fromJson(jsonMessage);
        return msg.messageId();
    }

    private WebResponse handleWebRequest(String requestUri, Map<String, String> queryParams) {

        var id = requestId.incrementAndGet();

        var x = Integer.parseInt(queryParams.get("x"));
        var y = Integer.parseInt(queryParams.get("y"));

        var cm = new CalculatorMessage(id, "multiply", Arrays.asList(x, y), null, false);
        var requestJson = cm.toJson();

        LOG.debug("handleWebRequest uri={} id={}. Sending kafka request={}", requestUri, id, requestJson);

        var responseFuture = kafkaWaitService.processRequest(id, requestJson);

        try {
            var responseString = responseFuture.get().value();
            var responseMessage = CalculatorMessage.fromJson(responseString);
            return new WebResponse(HttpServletResponse.SC_OK, responseMessage.result() + "\n");
        } catch (Exception e) {
            final String msg;
            if (e.getCause() instanceof TimeoutException) {
                msg = "TIMEOUT id=" + id + ". Event processor not running?\n";
            } else {
                msg = e.getMessage();
            }
            LOG.warn("Returning error for id={} msg={}", id, msg);
            return new WebResponse(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        }
    }

    record WebResponse(int code, String body) {
    }

    public static void main(String[] args) {
        new CalculatorWebServer().run();
    }
}
