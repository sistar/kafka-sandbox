import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import collection.mutable.Stack
import java.util.Properties
import kafka.api.{PartitionMetadata, TopicMetadata, TopicMetadataResponse}
import kafka.consumer.Greeter
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.Logging
import org.scalatest._

class HelloWorld extends Actor {
  override def preStart(): Unit = {

  }

  var finished = false
  def isFinished(): Boolean = (finished)
  def receive = {
    case Greeter.Begin => val greeter = context.actorOf(Props[Greeter], "greeter")
      greeter ! Greeter.Greet
    case Greeter.Done => finished = true
  }
}

class ExampleSpec extends FlatSpec with Matchers with Logging {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be(2)
    stack.pop() should be(1)
  }

  "Reading from Kafka using SimpleScalaExample.scala" should "work right away" in {
    produceMessages
    val system = ActorSystem("KafkaSystem")


    val helloWorld: ActorRef = system.actorOf(Props[HelloWorld])
    helloWorld -> Greeter.Begin
    while (!helloWorld.is) {
      Thread.sleep(1000L)
    }
  }


  def produceMessages {
    val props1 = new Properties()
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("partitioner.class", "kafka.utils.FixedValuePartitioner")
    props1.put("request.required.acks", "2")
    props1.put("request.timeout.ms", "1000")
    props1.put("metadata.broker.list", example.producer.KafkaProperties.METADATA_BROKER_LIST)
    val producerConfig1 = new ProducerConfig(props1)
    val topic = "hello.topic"
    val producer1 = new Producer[String, String](producerConfig1)
    for (a <- 1 until 2000) {
      producer1.send(new KeyedMessage[String, String](topic, "test", 0, "test %s".format(a)))
    }
  }

  "From a List Of Lists" should "matching Subelement be returned" in {
    val correlationId = 0
    val partitionMetadata: PartitionMetadata = PartitionMetadata(0, None, Seq())
    val topicMetadata: TopicMetadata = TopicMetadata("myTopic", Seq(partitionMetadata))
    val topicMetadataResponse: TopicMetadataResponse = TopicMetadataResponse(Seq(topicMetadata), correlationId)
    val topicsMetadata: Seq[TopicMetadata] = topicMetadataResponse.topicsMetadata
    def find(partitionId: Int, topicsMetadata: Seq[TopicMetadata]): Seq[TopicMetadata] = {
      topicsMetadata.filter(t => t.partitionsMetadata.exists(p => p.partitionId.equals(partitionId)))
    }
    find(0, topicsMetadata).size should be(1)
  }
  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a[NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }
}