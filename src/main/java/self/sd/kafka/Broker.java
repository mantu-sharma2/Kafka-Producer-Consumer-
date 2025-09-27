package self.sd.kafka;
import java.util.*;
import java.util.concurrent.*;

/**
 * Simple synchronous broker.
 * - Keeps a list of subscribers per Topic.
 * - Optional storage queue per topic for audit/demo.
 * - broadcast(topic, message) delivers immediately to each subscriber's onMessage.
 */
public class Broker {
    // topic name -> queue of produced items (for simple storage/audit)
    private final Map<String, Deque<String>> topicQueues = new HashMap<>();
    // topic -> list of subscribers (fan-out targets)
    private final Map<Topic, List<Subscriber>> subscribers = new HashMap<>();
    /**
     * Register a topic upfront to create its backing queue.
     */
    public void registerTopic(Topic topic) {
        topicQueues.computeIfAbsent(topic.name(), k -> new ArrayDeque<>());
        subscribers.computeIfAbsent(topic, t -> new ArrayList<>());
    }
    /**
     * Subscribe the given subscriber to a topic. Subscriber will receive every message published to that topic.
     */
    public void subscribe(Topic topic, Subscriber subscriber) {
        registerTopic(topic); // ensure internal maps exist
        subscribers.get(topic).add(subscriber);
    }
    /**
     * Publish a message to a topic: store it in the topic queue, then fan-out to all subscribers of that topic.
     */
    public void publish(Topic topic, String message) {
        // keep for audit/demo
        Deque<String> q = topicQueues.computeIfAbsent(topic.name(), k -> new ArrayDeque<>());
        q.addLast(message);
        broadcast(topic, message);
    }
    /**
     * Immediately deliver to every subscriber synchronously.
     */
    public void broadcast(Topic topic, String message) {
        List<Subscriber> subs = subscribers.getOrDefault(topic, Collections.emptyList());
        for (Subscriber s : subs) {
            s.onMessage(topic, message);
        }
    }
    /**
     * Expose storage queue for inspection/testing/demo.
     */
    public Deque<String> getTopicQueue(Topic topic) {
        return topicQueues.get(topic.name());
    }
}