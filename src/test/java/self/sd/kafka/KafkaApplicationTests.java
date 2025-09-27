package self.sd.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaApplicationTests {

	@Test
	void synchronousBroadcastDeliversToAllSubscribers() {
		Broker broker = new Broker();
		for (Topic t : Topic.values()) broker.registerTopic(t);

		Subscriber sNews1 = new Subscriber("S-NEWS-1");
		Subscriber sNews2 = new Subscriber("S-NEWS-2");
		broker.subscribe(Topic.NEWS, sNews1);
		broker.subscribe(Topic.NEWS, sNews2);

		Producer producer = new Producer(broker);
		producer.produce(Topic.NEWS, "hello");

		assertEquals(1, broker.getTopicQueue(Topic.NEWS).size());
		assertEquals(1, sNews1.getReceived().size());
		assertEquals(1, sNews2.getReceived().size());
	}
}
