package self.sd.kafka;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple synchronous subscriber with an in-memory received list.
 * Broker invokes onMessage directly during broadcast.
 */
public class Subscriber {
    private final String name;
    private final List<String> received = new ArrayList<>();

    public Subscriber(String name) {
        this.name = name;
    }

    public void onMessage(Topic topic, String message) {
        String decorated = "[" + name + "@" + topic.name() + "] " + message;
        received.add(decorated);
        System.out.println(decorated);
    }

    public List<String> getReceived() {
        return received;
    }
}