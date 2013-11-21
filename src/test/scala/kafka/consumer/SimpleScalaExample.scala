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
import kafka.cluster.Broker
import scala.collection.immutable.HashMap
import java.io.UnsupportedEncodingException


class SimpleScalaExample {
  private final val LOGGER: Logger = LoggerFactory.getLogger(classOf[SimpleScalaExample])

  def main(args: Array[String]): Unit = {
    val example: SimpleScalaExample = new SimpleScalaExample
    val maxReads: Long = args(0).toLong
    val topic: String = args(1)
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


  private var m_replicaBrokers: Seq[Broker]

  private def handleFetchResponseError(numErrors: Int, fetchResponse: FetchResponse) : Int = {

    val code: Short = fetchResponse.errorCode(topic, partition)
    LOGGER.error("Error fetching data from the Broker:" + metadata.leader.map {
      _.host
    }.get + " Reason: " + code)
    if (numErrors + 1 > 5) throw new RuntimeException("more than 5 errors")
    if (code == ErrorMapping.OffsetOutOfRangeCode) {
      readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, clientName)
      return numErrors + 1
    }
    consumer.close
    consumer = null

    leadBroker = findNewLeader(metadata.leader, topic, partition)
    return numErrors + 1
  }

  def run(maxReads1: Long, topic: String, partition: Int, seedBrokers: ImmutableList[BrokerInfo]) {
    var maxReadsCounter = maxReads1
    val metadata: PartitionMetadata = getPartitionMetadata(topic, partition, seedBrokers)

    //val leadBrokerPort: Integer = metadata.leader.port
    val clientName: String = "Client_" + topic + "_" + partition
    var consumer = new SimpleConsumer(metadata.leader.map {
      _.host
    }.get, metadata.leader.map {
      _.port
    }.get, 100000, 64 * 1024, clientName)
    var readOffset: Long = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime, clientName)
    LOGGER.info(String.format("last offset for topic %s partition %s : %s ", topic, partition, readOffset))
    var numErrors: Int = 0
    while (maxReadsCounter > 0) {
      if (consumer == null) {
        consumer = new SimpleConsumer(metadata.leader.map {
          _.host
        }.get, metadata.leader.map {
          _.port
        }.get, 100000, 64 * 1024, clientName)
      }
      val req: FetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build
      val fetchResponse: FetchResponse = consumer.fetch(req)
      if (fetchResponse.hasError) {
        numErrors = handleFetchResponseError(numErrors, fetchResponse)
      }
      numErrors = 0
      var numRead: Long = 0
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
        maxReadsCounter -= 1
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
    val requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = HashMap(topicAndPartition -> new PartitionOffsetRequestInfo(whichTime, 1))

    val request: OffsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response: OffsetResponse = consumer.getOffsetsBefore(request)
    LOGGER.info("last offset: " + response.toString)
    if (response.hasError) {
      LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
      return 0
    }
    val offsets: Array[Long] = response.offsetsGroupedByTopic offsets(topic, partition)
    return offsets(0)
  }

  private def getHost(broker: Option[Broker]): Option[String] = {
    broker.map {
      _.host
    }
  }

  private def findNewLeader(a_oldLeader: Option[Broker], a_topic: String, a_partition: Int): Option[String] = {
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
          else if (a_oldLeader.map {
            _.host.toLowerCase
          } == metadata.leader.map {
            _.host.toLowerCase
          } && i == 0) {
            goToSleep = true
          }
          else {
            return metadata.leader.map {
              _.host
            }
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
          i += 1;
          i - 1
        })
      }
    }
    LOGGER.error("Unable to find new leader after Broker failure. Exiting")
    throw new KafkaClientException("Unable to find new leader after Broker failure. Exiting")
  }

  def using[Closeable <: {def close() : Unit}, B](closeable: Closeable)(getB: Closeable => B): B =
    try {
      getB(closeable)
    } finally {
      closeable.close()
    }


  private def findLeader(seedBrokers: Seq[Broker], a_topic: String, correlationId: Int): PartitionMetadata = {
    var returnMetaData: PartitionMetadata = null
    seedBrokers.takeWhile(_ => returnMetaData == null).foreach(seed => try {
      using(new SimpleConsumer(seed.host, seed.port, 100000, 64 * 1024, "leaderLookup")) {
        consumer =>
          val topics: List[String] = List(a_topic)
          val req: TopicMetadataRequest = new TopicMetadataRequest(topics, correlationId)
          val resp: TopicMetadataResponse = consumer.send(req)
          val topicMetaData: Seq[TopicMetadata] = resp.topicsMetadata

          val topicMetadataFiltered: Seq[TopicMetadata] = topicMetaData.filter(topicM => topicM.partitionsMetadata.contains(correlationId))
      }
    } catch {
      case e: Exception => {
        LOGGER.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + correlationId + "] Reason: " + e)
      }
    })

    if (returnMetaData != null) {
      m_replicaBrokers = returnMetaData.replicas
    }
    return returnMetaData
  }

}
