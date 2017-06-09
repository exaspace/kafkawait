# KafkaWait

Simple Java library to overlay synchronous request-response behaviour on top of Kafka for applications that must wait for a "response" message to arrive on one or more Kafka topics before doing something. One common use case is providing an HTTP service on top of Kafka (i.e. your HTTP request triggers asynchronous event processing in Kafka, you need to wait for the ultimate result of this computation to arrive on some "response" Kafka topic and you use this data to complete the HTTP response).

Usage:

* create a `KafkaWait` instance giving an `IdExtractor` (this is where you extract some form of ID from your messages that can be used for matching requests to responses) and your global `maxWait` time
* create your KafkaConsumer (however you like) and configure it to call `kafkaWait.onMessage` on every record it receives  
* every time you publish a Kafka "request" event that you expect to eventually receive a matching response event for, call `kafkawait.waitFor(id)`
* this returns you a future which you can block on for however long you like (in any case, you will be timed out after `maxWait`)
 
That's it!

## Building synchronous request-response interactions on top of Kafka
 
Example configurations (here "server" really means "server process"): 
 
* single response topic, default partitioner

You want to use Kafka's default partitioner to map request messages to partitions. Each server process will be consuming from the same response topic partitions independently and will just ignore any messages which are responses to other servers' requests. 

* single response topic, custom partitioner

You're using some field in your messages (or the key or perhaps even server specific topic) that corresponds to the requesting server, and you write a custom partitioner to map response messages to certain partitions (i.e. in the response producers). Each server consumes from its dedicated partitions on the response topic. This is technically perhaps slightly more efficient on the consumption side but more complex to setup, scale and maintain as partitions need to be carefully managed and mapped to specific servers.  

* multiple response topics, default partitioner

You're using some field in your messages (or the key or perhaps even server specific topic) that corresponds to the requesting server. Your Kafka response publishers send responses to a topic that corresponds to this server. Similar to above option, and has similar maintenance overhead (need to be creating/deleting topics when you add/remove servers). 

KafkaWait can be used in any of these use cases. Note that when building typical request-response synchronous interactions on top of Kafka you don't need much retention time (apart from auditing) and you don't bother to commit offsets (but ensure you have auto offset type set to latest as there's no value in consuming old out of date response messages).