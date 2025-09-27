package self.sd.kafka;

/**
 * Simple synchronous producer. Call produce(topic, message) to deliver.
 */
public class Producer {
    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public void produce(Topic topic, String message) {
        broker.publish(topic, message); // publish stores and broadcasts synchronously
    }
}