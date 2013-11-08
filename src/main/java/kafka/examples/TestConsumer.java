
package kafka.examples;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import example.producer.KafkaProperties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestConsumer implements Runnable {

    private final Counter consumed;

    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }

    private final ConsumerConnector consumer;
    private final String topic;

    public TestConsumer(String topic, MetricRegistry metricRegistry) {
        consumed = metricRegistry.counter(KafkaProperties.TRACKING_CACHED_REQUESTS_TOPIC + ".consumed");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                KafkaProperties.createConsumerConfig());
        this.topic = topic;
    }



    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(3));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            MessageAndMetadata<byte[],byte[]> next = it.next();
            String msg = String.valueOf(next.message());
            consumed.inc();
            System.out.println(msg);
        }

    }


}