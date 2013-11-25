package kafka.consumer

import org.slf4j.{LoggerFactory, Logger}

import scala.Long
import scala.Predef.String
import com.test.simple.KafkaClientException
import example.producer.KafkaProperties
import kafka.api._
import kafka.api.{FetchRequestBuilder, FetchRequest}
import kafka.common.{TopicAndPartition, ErrorMapping}
import java.nio.ByteBuffer
import kafka.cluster.Broker
import java.io.UnsupportedEncodingException
import scala.collection.immutable.HashMap


class SimpleScalaExample {
  private final val LOGGER: Logger = LoggerFactory.getLogger(classOf[SimpleScalaExample])

  def main(args: Array[String]): Unit = {
    val example: SimpleScalaExample = new SimpleScalaExample
    val maxReads: Long = args(0).toLong
    val topic: String = args(1)
    val partition: Int = args(2).toInt

    try {
      example.run(maxReads, topic, partition, KafkaProperties.METADATA_BROKER_LIST)
    }
    catch {
      case e: Exception => {
        LOGGER.error("Oops:" + e)
        e.printStackTrace
      }
    }
  }


  private def handleFetchResponseError(numErrors: Int, fetchResponse: FetchResponse, topic: String, partition: Int, metadata: Option[PartitionMetadata]): Short = {
    val code: Short = fetchResponse.errorCode(topic, partition)
    val host = for {m <- metadata
                    l <- m.leader
                    h <- l.host} yield h
    LOGGER.error("Error fetching data from the Broker:" + host + " Reason: " + code)
    if (numErrors + 1 > 5) throw new RuntimeException("more than 5 errors")
    return code
  }

  def run(maxReads1: Long, topic: String, partition: Int, seedBrokers: String) {
    var maxReadsCounter = maxReads1
    val metadata: Option[PartitionMetadata] = getPartitionMetadata(topic, partition, seedBrokers)

    //val leadBrokerPort: Integer = metadata.leader.port
    val clientName: String = "Client_" + topic + "_" + partition
    var consumer = simpleConsumer(metadata, clientName)
    var readOffset: Long = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime, clientName).get.offsets.head
    LOGGER.info(String.format("last offset for topic %s partition %s : %s ", topic, partition, readOffset))
    var numErrors: Int = 0
    while (maxReadsCounter > 0) {
      if (consumer == None) {
        consumer = simpleConsumer(metadata, clientName)
      }
      val req: FetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build
      val fetchResponse: FetchResponse = consumer.get.fetch(req)
      if (fetchResponse.hasError) {
        val code = handleFetchResponseError(numErrors, fetchResponse, topic, partition, metadata)
        if (code == ErrorMapping.OffsetOutOfRangeCode) {
          readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, clientName).get.offsets.head
          return numErrors + 1
        }
        consumer match {
          case None =>
          case Some(c) => c.close()
        }
        consumer = None

        findNewLeader(metadata.flatMap(_.leader), seedBrokers, topic, partition)
      } else numErrors = 0

      var numRead: Long = 0
      for (messageAndOffset <- fetchResponse.messageSet(topic, partition)) {
        val currentOffset: Long = messageAndOffset.offset
        if (currentOffset < readOffset) {
          LOGGER.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset)
          return
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
    consumer match {
      case None =>
      case Some(c) => c.close()
    }
  }


  def simpleConsumer(metadata: Option[PartitionMetadata], clientName: String): Option[SimpleConsumer] = {
    Option(new SimpleConsumer(host(metadata), port(metadata), 100000, 64 * 1024, clientName))
  }

  def port(metadata: Option[PartitionMetadata]): Int = {
    metadata.flatMap {
      _.leader.map {
        _.port
      }
    }.get
  }

  def host(metadata: Option[PartitionMetadata]): String = {
    metadata.flatMap {
      _.leader.map {
        _.host
      }
    }.get
  }

  /**
   *
   * @param topic
   * @param partition
   * @param seedBrokers
   * @return
   */
  private def getPartitionMetadata(topic: String, partition: Int, seedBrokers: String): Option[PartitionMetadata] = {
    val metadata: Option[PartitionMetadata] = findLeader(seedBrokers, topic, partition)

    metadata match {
      case None =>
        throw new KafkaClientException("Can't find metadata for Topic and Partition.")
    }

    metadata.map(_.leader) match {
      case None =>
        throw new KafkaClientException("Can't find Leader for Topic and Partition.")
    }

    return metadata
  }

  def getLastOffset(consumer: Option[SimpleConsumer], topic: String, partition: Int, whichTime: Long, clientName: String): Option[PartitionOffsetsResponse] = {
    val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partition)
    val requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = HashMap(topicAndPartition -> new PartitionOffsetRequestInfo(whichTime, 1))

    val request: OffsetRequest = OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, partition, clientName)
    val response: Option[OffsetResponse] = consumer.map(_.getOffsetsBefore(request))
    LOGGER.info("last offset: " + response.toString)
    if (response.map(_.hasError).getOrElse(false)) {
      LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.get.partitionErrorAndOffsets.get(topicAndPartition))
      return None
    }
    val offsets: Option[Map[TopicAndPartition, PartitionOffsetsResponse]] = response.get.offsetsGroupedByTopic.get(topic)
    return offsets.flatMap {
      _.get(topicAndPartition)
    }
  }

  private def getHost(broker: Option[Broker]): Option[String] = {
    broker.map {
      _.host
    }
  }

  private def sameLeader(partitionMetadata: Option[PartitionMetadata], a_oldLeader: Option[Broker]): Boolean = {
    return partitionMetadata.exists {
      _.leader.map {
        _.host.toLowerCase()
      } == a_oldLeader.map {
        _.host.toLowerCase()
      }
    }
  }

  private def findNewLeader(a_oldLeader: Option[Broker], seedBrokers: String, a_topic: String, a_partition: Int): Option[String] = {
    for (i <- 0 to 2) {
      false
      val metadata: Option[PartitionMetadata] = findLeader(seedBrokers, a_topic, a_partition)

      val goToSleepBecauseDidNotFindNewLeader: Boolean = metadata.exists {
        !_.leader.isDefined
      }
      val gotToSleepBecauseSameLeader = sameLeader(metadata, a_oldLeader) && (i == 0)
      if (goToSleepBecauseDidNotFindNewLeader || gotToSleepBecauseSameLeader) {
        try {
          Thread.sleep(1000)
        }
        catch {
          case ie: InterruptedException => {
          }
        }
      } else {
        return metadata.flatMap {
          _.leader.map {
            _.host
          }
        }
      }
    }
    LOGGER.error("Unable to find new leader after Broker failure. Exiting")
    throw new KafkaClientException("Unable to find new leader after Broker failure. Exiting")
  }

  def using[Closeable <: {
    def close() : Unit
  }, B](closeable: Closeable)(getB: Closeable => B): B =
    try {
      getB(closeable)
    } finally {
      closeable.close()
    }

  def find(partition: Int, topicsMetaData: Seq[TopicMetadata]): Option[PartitionMetadata] = {
    for (topicMetaData <- topicsMetaData) {
      for (x <- topicMetaData.partitionsMetadata) {
        if (x.partitionId.equals(partition))
          return Some(x)
      }
    }
    return None
  }

  private def findLeader(seedBrokers: String, a_topic: String, partition: Int): Option[PartitionMetadata] = {
    var returnMetaData: Option[PartitionMetadata] = None
    val sb: Array[Array[String]] = seedBrokers.split(",").map(_.split(":"))
    sb.takeWhile(_ => returnMetaData == None).foreach(seed =>
      try {
        using(new SimpleConsumer(seed {
          0
        }, seed {
          1
        }.toInt, 100000, 64 * 1024, "leaderLookup")) {
          consumer =>
            val topics: List[String] = List(a_topic)
            val req: TopicMetadataRequest = new TopicMetadataRequest(topics, partition)
            val resp: TopicMetadataResponse = consumer.send(req)
            val foundPartitionMetadata: Option[PartitionMetadata] = find(partition, resp.topicsMetadata)
            if (foundPartitionMetadata != None) {
              //m_replicaBrokers = foundPartitionMetadata.map(_.replicas).getOrElse(Seq())
              return foundPartitionMetadata
            }
        }
      }
      catch {
        case e: Exception => {
          LOGGER.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + partition + "] Reason: " + e)
        }
      })
    return None


  }

}
