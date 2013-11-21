import collection.mutable.Stack
import kafka.api.{PartitionMetadata, TopicMetadata, TopicMetadataResponse}
import org.scalatest._

class ExampleSpec extends FlatSpec with Matchers {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be(2)
    stack.pop() should be(1)
  }



  "From a List Of Lists" should "matching Subelement be returned" in {
    val a_partition = 0
    val correlationId = 0
    val partitionMetadata: PartitionMetadata = PartitionMetadata(0, None, Seq())
    val topicMetadata: TopicMetadata = TopicMetadata("myTopic", Seq(partitionMetadata))
    val topicMetadataResponse: TopicMetadataResponse = TopicMetadataResponse(Seq(topicMetadata), correlationId)
    val topicsMetadata: Seq[TopicMetadata] = topicMetadataResponse.topicsMetadata
    find(0, topicsMetadata).size should be(1)
  }
  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a[NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }
}