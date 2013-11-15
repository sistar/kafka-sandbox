package kafka.consumer

import org.slf4j.{LoggerFactory, Logger}

import scala.Long
import scala.Predef.String
import com.test.simple.{KafkaClientException, BrokerInfo}
import example.producer.KafkaProperties
import kafka.api._
import kafka.api.{PartitionOffsetRequestInfo, FetchRequestBuilder, FetchRequest}
import kafka.common.{TopicAndPartition, ErrorMapping}
import java.nio.ByteBuffer
import com.google.common.collect.ImmutableList


class SimpleScalaExample {
  private final val LOGGER: Logger = LoggerFactory.getLogger(classOf[SimpleScalaExample])

  def main(args: Array[String]): Unit = {
    val example: SimpleScalaExample = new SimpleScalaExample
    val maxReads: Long = args(0).toLong
    val topic : String = args(1)
    val partition: Int = args(2).toInt
    val seeds: ImmutableList[BrokerInfo] = BrokerInfo.brokerInfos(KafkaProperties.METADATA_BROKER_LIST)
    try {
      example.run(maxReads, topic, partition, seeds)
    }
    catch {
      case e: Exception => {
        LOGGER.error("Oops:" + e)
        e.printStackTrace
      }
    }
  }

  private val m_replicaBrokers: List[BrokerInfo] = List()

  def run(maxReads: Long, topic: String, partition: Int, seedBrokers: ImmutableList[BrokerInfo]) {
    val metadata: PartitionMetadata = getPartitionMetadata(topic, partition, seedBrokers)
    //var leadBroker: String = metadata.leader.host
    //val leadBrokerPort: Integer = metadata.leader.port
    val clientName: String = "Client_" + topic + "_" + partition
    val consumer = new SimpleConsumer(leadBroker, leadBrokerPort, 100000, 64 * 1024, clientName)
    var readOffset: Long = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime, clientName)
    LOGGER.info(String.format("last offset for topic %s partition %s : %s ", topic, partition, readOffset))
    var numErrors: Int = 0
    while (maxReads > 0) {
      if (consumer == null) {
        consumer = new SimpleConsumer(leadBroker, leadBrokerPort, 100000, 64 * 1024, clientName)
      }
      val req: FetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build
      val fetchResponse: FetchResponse = consumer.fetch(req)
      if (fetchResponse.hasError) {
        numErrors += 1
        val code: Short = fetchResponse.errorCode(topic, partition)
        LOGGER.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code)
        if (numErrors > 5) break //todo: break is not supported
        if (code == ErrorMapping.OffsetOutOfRangeCode) {
          readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, clientName)
          continue //todo: continue is not supported
        }
        consumer.close
        consumer = null
        val oldLeader: BrokerInfo = new BrokerInfo(leadBroker, leadBrokerPort)
        leadBroker = findNewLeader(oldLeader, topic, partition)
        continue //todo: continue is not supported
      }
      numErrors = 0
      var numRead: Long = 0
      import scala.collection.JavaConversions._
      for (messageAndOffset <- fetchResponse.messageSet(topic, partition)) {
        val currentOffset: Long = messageAndOffset.offset
        if (currentOffset < readOffset) {
          LOGGER.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset)
          continue //todo: continue is not supported
        }
        readOffset = messageAndOffset.nextOffset
        val payload: ByteBuffer = messageAndOffset.message.payload
        val bytes: Array[Byte] = new Array[Byte](payload.limit)
        payload.get(bytes)
        var content: String = null
        try {
          content = new String(bytes, "UTF-8")
        }
        catch {
          case e: UnsupportedEncodingException => {
            throw new KafkaClientException(e)
          }
        }
        LOGGER.info(String.valueOf(messageAndOffset.offset) + ": " + content)
        numRead += 1
        maxReads -= 1
      }
      if (numRead == 0) {
        try {
          Thread.sleep(1000)
        }
        catch {
          case ie: InterruptedException => {
          }
        }
      }
    }
    if (consumer != null) consumer.close
  }

  /**
   *
   * @param topic
   * @param partition
   * @param seedBrokers
   * @return
   */
  private def getPartitionMetadata(topic: String, partition: Int, seedBrokers: ImmutableList[BrokerInfo]): PartitionMetadata = {
    val metadata: PartitionMetadata = findLeader(seedBrokers, topic, partition)
    if (metadata == null) {
      throw new KafkaClientException("Can't find metadata for Topic and Partition.")
    }
    if (metadata.leader == null) {
      throw new KafkaClientException("Can't find Leader for Topic and Partition.")
    }
    return metadata
  }

  def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientName: String): Long = {
    val topicAndPartition: TopicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = new HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))
    val request: OffsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response: OffsetResponse = consumer.getOffsetsBefore(request)
    LOGGER.info("last offset: " + response.toString)
    if (response.hasError) {
      LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
      return 0
    }
    val offsets: Array[Long] = response.offsets(topic, partition)
    return offsets(0)
  }

  private def findNewLeader(a_oldLeader: BrokerInfo, a_topic: String, a_partition: Int): String = {
    {
      var i: Int = 0
      while (i < 3) {
        {
          var goToSleep: Boolean = false
          val metadata: PartitionMetadata = findLeader(m_replicaBrokers, a_topic, a_partition)
          if (metadata == null) {
            goToSleep = true
          }
          else if (metadata.leader == null) {
            goToSleep = true
          }
          else if (a_oldLeader.getHost.equalsIgnoreCase(metadata.leader.host) && i == 0) {
            goToSleep = true
          }
          else {
            return metadata.leader.host
          }
          if (goToSleep) {
            try {
              Thread.sleep(1000)
            }
            catch {
              case ie: InterruptedException => {
              }
            }
          }
        }
        ({
          i += 1; i - 1
        })
      }
    }
    LOGGER.error("Unable to find new leader after Broker failure. Exiting")
    throw new KafkaClientException("Unable to find new leader after Broker failure. Exiting")
  }

  private def findLeader(seedBrokers: List[BrokerInfo], a_topic: String, a_partition: Int): PartitionMetadata = {
    var returnMetaData: PartitionMetadata = null
    import scala.collection.JavaConversions._
    for (seed <- seedBrokers) {
      var consumer: SimpleConsumer = null
      try {
        consumer = new SimpleConsumer(seed.getHost, seed.getPort, 100000, 64 * 1024, "leaderLookup")
        val topics: List[String] = List()
        topics.add(a_topic)
        val req: TopicMetadataRequest = new TopicMetadataRequest(topics)
        val resp: TopicMetadataResponse = consumer.send(req)
        val metaData: List[TopicMetadata] = resp.topicsMetadata
        import scala.collection.JavaConversions._
        for (item <- metaData) {
          import scala.collection.JavaConversions._
          for (part <- item.partitionsMetadata) {
            if (part.partitionId == a_partition) {
              returnMetaData = part
              break //todo: break is not supported
            }
          }
        }
      }
      catch {
        case e: Exception => {
          LOGGER.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: " + e)
        }
      }
      finally {
        if (consumer != null) consumer.close
      }
    }
    if (returnMetaData != null) {
      m_replicaBrokers.clear
      import scala.collection.JavaConversions._
      for (replica <- returnMetaData.replicas) {
        m_replicaBrokers.add(new BrokerInfo(replica))
      }
    }
    return returnMetaData
  }
}
