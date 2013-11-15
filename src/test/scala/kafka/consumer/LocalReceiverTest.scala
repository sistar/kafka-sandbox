package kafka.consumer

import java.util.Properties
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.Predef._

class LocalReceiverTest extends JUnit3Suite {

  val consumerProps: Properties = new Properties
  consumerProps.put("zookeeper.connect", "127.0.0.1:2181")
  consumerProps.put("zookeeper.connection.timeout.ms", "1000000")
  consumerProps.put("group.id", "separate-test-consumer-group")
  consumerProps.put("auto.offset.reset", "smallest")


  val topic: String = "test.topic"

  override def setUp() {
    super.setUp
  }

  def startConsumer() {
    val consumerConfig = new ConsumerConfig(consumerProps)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> 1))
    var threadList = List[ConsumerThread]()
    for ((topic, streamList) <- topicMessageStreams) {
      println("topic: " + topic)
      for (stream <- streamList) {
        println("starting consumer thread " + stream)
        threadList ::= new ConsumerThread(stream)
      }
    }
    for (thread <- threadList)
      thread.start
  }

  @Test
  def testReceiveStrings() {
    startConsumer()
  }




}
