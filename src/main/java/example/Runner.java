package example;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import example.producer.TestProducer;
import kafka.examples.TestConsumer;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;


public class Runner {
    public static void main(String[] args) throws Exception {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Graphite graphite = new Graphite(new InetSocketAddress("localhost", 2003));
        final GraphiteReporter reporter = GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith("kafka.sandbox")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter.start(5, TimeUnit.SECONDS);
        TestProducer testProducer = new TestProducer(1000000L, metricRegistry);
        TestConsumer simpleConsumer = new TestConsumer("tracking.cached.requests", metricRegistry);
        Thread consumerThread = new Thread(simpleConsumer);
        Thread producerThread = new Thread(testProducer);
        consumerThread.start();
        producerThread.start();
    }
}
