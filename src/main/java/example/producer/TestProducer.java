package example.producer;

import java.util.*;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer implements Runnable {

    private long numberOfEventsToSend;
    private final Random rnd;
    private final ProducerConfig config;
    private final Counter counter;

    public TestProducer(long numberOfEventsToSend, MetricRegistry metricRegistry) {

        counter = metricRegistry.counter(KafkaProperties.TRACKING_CACHED_REQUESTS_TOPIC + ".produced");
        this.numberOfEventsToSend = numberOfEventsToSend;
        rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", KafkaProperties.METADATA_BROKER_LIST);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        config = new ProducerConfig(props);


    }

    public void close() {

    }


    @Override
    public void run() {
        final Producer<String, String> producer
                = new Producer<String, String>(config);
        for (long nEvents = 0; nEvents < numberOfEventsToSend; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(KafkaProperties.TRACKING_CACHED_REQUESTS_TOPIC, ip, msg);
            producer.send(data);
            counter.inc();

        }
        producer.close();
    }
}