package com.test.simple;

import com.google.common.collect.ImmutableList;
import example.producer.KafkaProperties;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.helpers.LogLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleExample.class);

    public static void main(String args[]) {
        SimpleExample example = new SimpleExample();
        long maxReads = Long.parseLong(args[0]);
        String topic = args[1];
        int partition = Integer.parseInt(args[2]);
        ImmutableList<BrokerInfo> seeds = BrokerInfo.brokerInfos(KafkaProperties.METADATA_BROKER_LIST);
        try {
            example.run(maxReads, topic, partition, seeds);
        } catch (Exception e) {
            LOGGER.error("Oops:" + e);
            e.printStackTrace();
        }
    }

    private List<BrokerInfo> m_replicaBrokers = new ArrayList<BrokerInfo>();

    public void run(long maxReads, String topic, int partition, ImmutableList<BrokerInfo> seedBrokers)  {
        PartitionMetadata metadata = getPartitionMetadata(topic, partition, seedBrokers);

        String leadBroker = metadata.leader().host();
        Integer leadBrokerPort = metadata.leader().port();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, leadBrokerPort, 100000, 64 * 1024, clientName);


        long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        LOGGER.info(String .format("last offset for topic %s partition %s : %s ",topic,partition,readOffset));
        int numErrors = 0;


        while (maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, leadBrokerPort, 100000, 64 * 1024, clientName);
            }

            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                LOGGER.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                BrokerInfo oldLeader = new BrokerInfo(leadBroker, leadBrokerPort);
                leadBroker = findNewLeader(oldLeader, topic, partition);
                continue;
            }
            numErrors = 0;

            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    LOGGER.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String content = null;
                try {
                    content = new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new KafkaClientException(e);
                }
                LOGGER.info(String.valueOf(messageAndOffset.offset()) + ": " + content);
                numRead++;
                maxReads--;
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }

    /**
     *
     * @param topic
     * @param partition
     * @param seedBrokers
     * @return
     */
    private PartitionMetadata getPartitionMetadata(String topic, int partition, ImmutableList<BrokerInfo> seedBrokers) {
        // find the meta data about the topic and partition we are interested in
        //
        PartitionMetadata metadata = findLeader(seedBrokers, topic, partition);
        if (metadata == null) {
            throw new KafkaClientException("Can't find metadata for Topic and Partition.");
        }
        if (metadata.leader() == null) {
            throw new KafkaClientException("Can't find Leader for Topic and Partition.");
        }
        return metadata;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        LOGGER.info("last offset: "+response.toString());
        if (response.hasError()) {
            LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(BrokerInfo a_oldLeader, String a_topic, int a_partition)  {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.getHost().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        LOGGER.error("Unable to find new leader after Broker failure. Exiting");
        throw new KafkaClientException("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<BrokerInfo> seedBrokers, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        for (BrokerInfo seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed.getHost(), seed.getPort(), 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(new BrokerInfo(replica));
            }
        }
        return returnMetaData;
    }
}