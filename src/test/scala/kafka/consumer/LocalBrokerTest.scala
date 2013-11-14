package kafka.consumer

import java.util.Properties
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.Predef.String

class LocalBrokerTest extends JUnit3Suite {
  val props: Properties = new Properties
  props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "example.producer.NullPartitioner")
  props.put("request.required.acks", "1")

  val config: ProducerConfig = new ProducerConfig(props)
  val producer: Producer[String, String] = new Producer[String, String](config)
  val topic: String = "test.topic"


  override def setUp() {
    super.setUp
  }

  @Test
  def testSendThreeStrings() {
    val myMessages = List("apples", "oranges", "pears")
    for (message <- myMessages) {
      val messages1: KeyedMessage[String, String] = new KeyedMessage(topic, message)
      producer.send(messages1)
    }


  }
}
