# KafkaWait

Simple library to overlay synchronous behaviour onto Kafka for applications that want to emit events and wait for separate "response" messages. One use case is for fronting an HTTP service on top of Kafka based event processing where the result of the computation is needed before the HTTP response can be sent.

To use the library, your application supplies a KafkaConsumer instance to KafkaWait and a handler which is called on every "response" message delivered to the consumer. This handler must take the supplied ConsumerRecord and return a `matchId` (how it does this is entirely up to you). This `matchId` is then matched against the set of known matches that are being waited for, and, if any waiters are found, notifications are sent to the waiters.  

Usage:

* create a `KafkaWait` object with a KafkaConsumer you've built for the topic that contains your "response" messages, specifying a message `IdExtractor` which is a function from ConsumerRecord to ID    
* when publishing a Kafka event that should eventually receive some response event, call `kafkawait.waitFor(id)`
 
That's it!
