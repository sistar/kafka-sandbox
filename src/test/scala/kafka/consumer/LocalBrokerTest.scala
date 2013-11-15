package kafka.consumer

import java.util.Properties
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.Predef._
import kafka.utils.Utils

class LocalBrokerTest extends JUnit3Suite {

  val props: Properties = new Properties
  props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "example.producer.NullPartitioner")
  props.put("request.required.acks", "1")
  val config: ProducerConfig = new ProducerConfig(props)
  val producer: Producer[String, String] = new Producer[String, String](config)

  val consumerProps: Properties = new Properties
  consumerProps.put("zookeeper.connect", "127.0.0.1:2181")
  consumerProps.put("zookeeper.connection.timeout.ms", "1000000")
  consumerProps.put("group.id", "test-consumer-group")
  consumerProps.put("auto.offset.reset", "smallest")


  val topic: String = "test.topic"

  override def setUp() {
    super.setUp
  }


  @Test
  def testSendThreeStrings() {

    sendMessages(List("bananas", "grapefruits", "mangoes"))

    sendMessages(List("apples", "oranges", "pears"))


  }

  def sendMessages(myMessages: List[String]) {
    for (message <- myMessages) {
      val messages1: KeyedMessage[String, String] = new KeyedMessage(topic, message)

      println("sending " + message)
      producer.send(messages1)
    }
  }


}
