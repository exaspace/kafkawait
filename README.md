# KafkaWait

[![Tests](https://github.com/exaspace/kafkawait/actions/workflows/main.yml/badge.svg)](https://github.com/exaspace/kafkawait/actions/workflows/main.yml)

Tiny Java library to provide synchronous request-response behaviour on top of Kafka for applications that must 
publish a Kafka "request" message and then await a Kafka "response" message.

A common use case for this is providing an HTTP service at the boundary of an event driven Kafka architecture
(i.e. an HTTP request triggers asynchronous event processing mediated via Kafka and you need to wait for the ultimate
result of this computation to arrive on some "response" Kafka topic, using this data to complete the HTTP response).


## Usage (high level API):

If you want a high level abstraction, use the `KafkaWaitService` class which handles the Kafka send and receive messaging for you
(you pass in the request and response Kafka topic names and other parameters when creating the service).

```
  // Publish to Kafka request topic and await a response on a response topic
  // The `id` is a unique ID you supply for each message, and `messageValue` is the body of the message.
  
  Future<ConsumerRecord<...>> responseFuture = kafkaWaitService.processRequest(id, messageValue);
```



You get the response body with `responseFuture.get().value()`.


## Usage (low level API):

If you want more control, you'll manage the Kafka publishing and consuming yourself, and just use `KafkaWait` to keep track of the
 cache of message IDs.

1. In your application you create a `KafkaWait` instance supplying:
 
 * an `IdExtractor` (this is where you extract some form of ID from your messages 
that will be used for matching the correct Kafka response message to the Kafka request message) 
 * your desired timeout value 
 
2. After you publish a request to Kafka with a given `id`, call `waitFor(id)` on the `KafkaWait` instance. 
This returns you a future which you can block on and will be timed out if no response consumed within your configured timeout.

3. In your Kafka consumer thread, call `kafkaWait.onMessage` for each Kafka record you receive (your consumer most likely won't
need to do anything more than that).


## Demonstration Application

The test source tree contains a simple demo Calculator application made up of two services:

* a front end API service (CalculatorWebServer)
* a back end (Kafka consuming & producing) service (CalculatorEventProcessor)

To run the demo, first build the artifacts and start Kafka and Zookeeper via docker compose:

    ./gradlew clean jar testJar
    docker compose up -d

(If you don't have docker, you'll need to have your own Kafka server running, and then just start the demo services 
with `./gradlew runDemoEventProcessor` and `./gradlew runDemoWebServer`. If your Kafka is running somewhere other than
localhost, you can set the host name for it using the system property KAFKA_HOST)

Test out the API service (you should see 42 returned in the HTTP body)

```
curl 'localhost:8000/multiply?x=7&y=6'
```

In separate terminal windows, you can view the messages arriving on the Kafka topics:

    docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic requests
    docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic responses
    
    # or if you have kafkacat installed (NB add `127.0.0.1 kafka` to your hosts file if running kafka in docker)
    kcat -C -b localhost:9092 -t requests 
    kcat -C -b localhost:9092 -t responses 
 
The Calculator web server simply increments a global request counter to allocate each HTTP request a unique request ID,
and this ID is passed in the JSON request & response Kafka messages.

The performance of the demo is around 700 messages per second on a MacBook Air.

If you stop the event processor but leave the web server running, you'll see the desired behaviour: HTTP requests are 
timed out after the configured 1 second KafkaWait timeout (this timeout is set in MAX_WAIT_TIME in the demo server, CalculatorWebServer.java).


## General notes on synchronous request-response interactions on top of Kafka

First question if there is another way to do what you want: the asynchronous nature of event processing is part of its attraction and layering
 synchronous behaviour on top should usually be avoided if possible. e.g. if you are building a REST API you may be able to model your API to take account of asynchronous
 interactions by returning resource locations and getting the client to poll for availability.

However if you definitely need a "response" event to arrive, read on for possible configurations.

Example configurations (here "server" really means "server process"): 
 
* single response topic, default partitioner

You want to use Kafka's default partitioner to map request messages to partitions.
Each server process will be consuming from the same response topic partitions independently and will just ignore any messages which are responses to other servers' requests.

* single response topic, custom partitioner

You're using some field in your messages (or the key or perhaps even server specific topic) that corresponds to the requesting server,
and you write a custom partitioner to map response messages to certain partitions (i.e. in the response producers).
Each server consumes from its dedicated partitions on the response topic.
This is technically perhaps slightly more efficient on the consumption side but more complex to setup, scale and maintain as
partitions need to be carefully managed and mapped to specific servers.

* multiple response topics, default partitioner

You're using some field in your messages (or the key or perhaps even server specific topic) that corresponds to the requesting server.
Your Kafka response publishers send responses to a topic that corresponds to this server. Similar to above option, and has similar maintenance
overhead (need to be creating/deleting topics when you add/remove servers).

KafkaWait can be used in any of these use cases. Note that when building typical request-response synchronous interactions on top of
Kafka you don't need much retention time (apart from auditing) and you don't bother to commit offsets (but ensure you have auto offset
type set to `latest` as there's usually no value in consuming old out of date response messages).