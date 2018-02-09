# KafkaWait

[![Build Status](https://travis-ci.org/exaspace/kafkawait.svg?branch=master)](https://travis-ci.org/exaspace/kafkawait)

Simple Java library to overlay synchronous request-response behaviour on top of Kafka for applications that must wait for a "response" message to
arrive on one or more Kafka topics before doing something.
One common use case is providing an HTTP service at the boundary of an event driven Kafka architecture
(i.e. your HTTP request triggers asynchronous event processing in Kafka and you need to wait for the ultimate
result of this computation to arrive on some "response" Kafka topic, using this data to complete the HTTP response).


## Usage (high level API):

If you want a high level abstraction, use the `KafkaWaitService` class which handles the Kafka send and receive messaging for you.

```
  Future<ConsumerRecord<...>> responseFuture = kafkaWaitService.processRequest(id, messageValue);
```


## Usage (low level API):

* create a `KafkaWait` instance giving an `IdExtractor` (this is where you extract some form of ID from your messages that can be used for matching requests to responses)
and your desired timeout value
* create your KafkaConsumer (however you like) and configure it to call `kafkaWait.onMessage` on every record it receives (you'll likely start a thread for this task)
* every time you publish a Kafka "request" event that you expect to eventually receive a matching response event for, call `kafkawait.waitFor(id)`
* this returns you a future which you can block on when you are ready to wait (the future will be timed out after your configured timeout if no message is received)


## Demo

The test source tree contains a simple Calculator demo (to run this you will need a Kafka running on localhost
with two topics created: one called "requests" and one called "responses").

Start the Calculator back end service (receives input via Kafka events and outputs each answer as a Kafka event)

```
$ ./gradlew runDemoEventProcessor
```

Now start a Web Server as the HTTP API layer on top of our awesome back end service:

```
$ ./gradlew runDemoWebServer
```

Test it out (you should see 42 returned in the HTTP body)

```
curl 'localhost:8000/multiply?x=7&y=6'
```

You will see kafka messages on the requests and responses topics. The Calculator web server simply increments a global
request counter to allocate each HTTP request a unique request ID, and this ID is passed in the JSON
request & response Kafka messages.

The performance of the demo is around 700 messages per second on a MacBook Air.

If you stop the event processor but leave the web server running, you'll see HTTP requests are timed out after the configured
1 second KafkaWait timeout.


## Synchronous request-response interactions on top of Kafka

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