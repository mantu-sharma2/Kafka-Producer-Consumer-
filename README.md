# In-Memory PubSub (Kafka-like) Demo

Overview

- Simple in-memory publish/subscribe demo implemented in plain Java.
- Demonstrates topics, a broker, synchronous producer, and subscribers (optionally runnable as threads).
- No external Kafka dependency; this is a teaching/demo implementation.

Repository layout (relevant files)

- src/main/java/self/sd/kafka/
  - KafkaApplication.java — demo / entrypoint
  - Broker.java — topic registry, message store, publish/subscribe coordination
  - Topic.java — enum of topics (e.g., NEWS, TECH, SPORTS)
  - Producer.java — synchronous producer that calls Broker.publish(...)
  - Subscriber.java — subscriber implementation (can run as Runnable / thread)
- README.md — this file

Architecture (high level)

- Components:
  - Topic (enum): identifies channels/topics to which messages belong.
  - Broker: central component that
    - registers topics,
    - maintains per-topic message queues or storage,
    - manages subscriber lists per topic,
    - publishes messages to subscribers (synchronously in this demo).
  - Producer: synchronous client that calls Broker.publish(topic, message).
  - Subscriber: consumer that receives messages from Broker (either via direct callback/synchronous invocation or by polling a queue depending on implementation).
  - KafkaApplication (main): bootstrap code that registers topics, wires subscribers, starts producers/subscribers and demonstrates the flow.

Component Responsibilities and Interaction

- Topic
  - Simple enum representing available channels.
  - Used as key for registration, publishing, and subscribing.
- Broker
  - registerTopic(Topic): create internal data structures for a topic (subscriber list and queue).
  - subscribe(Topic, Subscriber): add a subscriber to a topic's subscriber list.
  - publish(Topic, String): store the message in the topic queue and notify/broadcast to registered subscribers.
  - getTopicQueue(Topic): (optional) expose stored messages for inspection in tests/demos.
  - Concurrency considerations: methods that modify shared state must be synchronized or use thread-safe collections. In this demo publish is synchronous — broker invokes subscribers from the publisher thread.
- Producer
  - produce(Topic, String): delegates to broker.publish(topic, message).
  - The provided Producer implementation is synchronous and simple (no buffering).
- Subscriber
  - Typically implements Runnable or provides a callback method that Broker invokes when a message arrives.
  - May maintain an internal queue of received messages and process them on its own thread.
  - Exposes shutdown() (demo expects this) so threads can be stopped cleanly.

Data flow (typical publish)

1. Producer.produce(topic, message) is called.
2. Broker.publish(topic, message) stores the message (optional) and iterates subscriber list.
3. For each subscriber of topic, broker invokes subscriber.onMessage(...) or enqueues the message for the subscriber to pick up.
4. Subscriber processes the message (synchronously if callback, or asynchronously from own queue).

Threading / Execution model

- Synchronous publish: producer's thread executes Broker.publish and will block while Broker notifies subscribers.
- Subscriber threads (if used) can:
  - be passive and rely on Broker callbacks (work happens on publisher thread), or
  - actively poll an internal queue that Broker enqueues messages into (work happens on subscriber thread).
- The demo application shows both direct publish calls and an example where Subscriber is run in its own thread (KafkaApplication creates Subscriber instances and may start threads).
- Proper use requires graceful shutdown: call shutdown() on producer/subscriber and interrupt threads. Broker should handle concurrent subscribe/publish safely.

KafkaApplication (usage in this repo)

- Bootstraps the broker and registers all topics:
  for (Topic t : Topic.values()) broker.registerTopic(t);
- Subscribes a Subscriber instance to a topic:
  broker.subscribe(Topic.NEWS, new Subscriber("manu"));
- Publishes a message synchronously:
  broker.publish(Topic.NEWS, "Hello NEWS");
- The main demonstrates the synchronous flow and prints status messages.

How to compile and run (Mac / terminal)

1. Open terminal and change to project root:
   cd /Users/darkshadow/Desktop/Kafka
2. Compile:
   javac -d out src/main/java/self/sd/kafka/\*.java
3. Run:
   java -cp out self.sd.kafka.KafkaApplication

Expected demo output (example)

- Console prints showing startup, subscription registration, publish event and delivery, and demo completion. Example:
  --- Starting In-Memory PubSub Demo (sync) ---
  manu subscribed to NEWS
  someone publishing: Hello NEWS
  [subscriber "manu"] received: Hello NEWS
  --- Demo Complete ---
