
package kafka.examples;

import example.producer.KafkaProperties;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerIterator;

import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class TestConsumer {

    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }
    private final ConsumerConnector consumer;
    private final String topic;

    public TestConsumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(3));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext())
            System.out.println(new String(it.next().message()));
    }


    public static void main(String[] args) throws Exception {

        TestConsumer simpleConsumer = new TestConsumer("tracking.cached.requests");
        simpleConsumer.run();

    }
}