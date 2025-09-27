package self.sd.kafka;

// This is a simple in-memory Kafka-like simulation.

public class KafkaApplication {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- Starting In-Memory PubSub Demo (sync) ---");

        Broker broker = new Broker();

        // Register topics
        for (Topic t : Topic.values()) {
            broker.registerTopic(t);
        }


         broker.subscribe(Topic.NEWS, new Subscriber("manu"));
         System.out.println("manu subscribed to NEWS");
         Thread.sleep(1000);
         System.out.println("someone publishing: Hello NEWS");
         broker.publish(Topic.NEWS, "Hello NEWS");


//        broker.subscribe(Topic.NEWS, new Subscriber("manu"));
//        System.out.println("manu subscribed to NEWS");
//        broker.subscribe(Topic.NEWS, new Subscriber("sam"));
//        System.out.println("sam subscribed to NEWS");
//        Thread.sleep(1000);
//        System.out.println("publishing Hello to : NEWS");
//        broker.publish(Topic.NEWS, "Hello");


        
        // Inspect stored queues sizes
//        System.out.println("NEWS stored: " + broker.getTopicQueue(Topic.NEWS).size());
//        System.out.println("TECH stored: " + broker.getTopicQueue(Topic.TECH).size());
//        System.out.println("SPORTS stored: " + broker.getTopicQueue(Topic.SPORTS).size());

        System.out.println("--- Demo Complete ---");
    }
}