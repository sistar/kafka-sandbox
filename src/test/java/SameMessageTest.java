import com.google.common.collect.ImmutableList;
import example.producer.KafkaProperties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;

public class SameMessageTest {
    @Test
    public void testThatSameMessagesAreRetrievd() throws Exception {
        ImmutableList<String> messages = createMessages();
        System.out.println(messages);
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.NullPartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        final Producer<String, String> producer
                = new Producer<String, String>(config);
        String topic = "test.topic";
        for (String message : messages) {

            producer.send(new KeyedMessage<String, String>(topic, message));
        }

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
                KafkaProperties.createConsumerConfig());

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(3));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        while (it.hasNext()) {
            String msg = String.valueOf(it.next().message());
            builder.add(msg);
        }
        assertEquals(builder.build(), messages);

    }

    private ImmutableList<String> createMessages() {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        for (int i = 0; i < 3; i++) {
            builder.add(String.format("msg%s-%s", i, new Date()));
        }

        return builder.build();
    }
}
