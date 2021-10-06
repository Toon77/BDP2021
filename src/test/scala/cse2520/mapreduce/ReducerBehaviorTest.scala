package cse2520.mapreduce

import akka.actor.testkit.typed.scaladsl.{BehaviorTestKit, TestInbox}
import cse2520.mapreduce.ReducerNode.{ProcessNextBatch, ReducerEvent, ReducerFinished, ReducerStarted, StartReducer}
import cse2520.mapreduce.fs.InMemoryFileSystem
import cse2520.mapreduce.wordcount.WordCountReducer
import org.scalatest.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ReducerBehaviorTest extends AnyWordSpecLike with Matchers  {

  private val reducerClass = classOf[WordCountReducer].asInstanceOf[Class[Reducer]]

  "reducer" should {

    "2.1 - acknowledge its start to the supervisor" in {

      val mapper1 = List("key3" -> "1", "key1" -> "2", "key2" -> "1", "key3" -> "1", "key1" -> "1", "key2" -> "1")
      val mapper2 = List()
      val mapper3 = List("key1" -> "1", "key3" -> "2", "key2" -> "3", "key3" -> "1", "key1" -> "1", "key4" -> "1")

      val fileSystem = new InMemoryFileSystem(dataSets = Map("i-1.txt" -> mapper1, "i-2.txt" -> mapper2, "i-3.txt" -> mapper3))
      val dataSet = List("i-3.txt", "i-1.txt", "i-2.txt")

      val reducer = BehaviorTestKit(ReducerNode(1, reducerClass, fileSystem))

      val supervisorInbox = TestInbox[ReducerEvent]()
      reducer.run(StartReducer(dataSet, supervisorInbox.ref))

      // check message type
      val m = supervisorInbox.receiveMessage()
      m.isInstanceOf[ReducerStarted] shouldBe true
    }

    "2.2 - send message for triggering processing" in {
      val mapper1 = List("key3" -> "1", "key1" -> "2", "key2" -> "1", "key3" -> "1", "key1" -> "1", "key2" -> "1")
      val mapper2 = List()
      val mapper3 = List("key1" -> "1", "key3" -> "2", "key2" -> "3", "key3" -> "1", "key1" -> "1", "key4" -> "1")

      val fileSystem = new InMemoryFileSystem(dataSets = Map("i-1.txt" -> mapper1, "i-2.txt" -> mapper2, "i-3.txt" -> mapper3))
      val dataSet = List("i-3.txt", "i-1.txt", "i-2.txt")

      val reducer = BehaviorTestKit(ReducerNode(1, reducerClass, fileSystem))

      val supervisorInbox = TestInbox[ReducerEvent]()
      reducer.run(StartReducer(dataSet, supervisorInbox.ref))

      reducer.selfInbox().expectMessage(ProcessNextBatch)
    }

    "2.3 - send message for processing next batch if there are more data" in {
      val mapper1 = List("key3" -> "1", "key1" -> "2", "key2" -> "1", "key3" -> "1", "key1" -> "1", "key2" -> "1")
      val mapper2 = List()
      val mapper3 = List("key1" -> "1", "key3" -> "2", "key2" -> "3", "key3" -> "1", "key1" -> "1", "key4" -> "1")

      val fileSystem = new InMemoryFileSystem(dataSets = Map("i-1.txt" -> mapper1, "i-2.txt" -> mapper2, "i-3.txt" -> mapper3))
      val dataSet = List("i-3.txt", "i-1.txt", "i-2.txt")

      val reducer = BehaviorTestKit(ReducerNode(1, reducerClass, fileSystem))

      val supervisorInbox = TestInbox[ReducerEvent]()
      reducer.run(StartReducer(dataSet, supervisorInbox.ref))

      // run self-sent ProcessNextBatch
      reducer.runOne()

      // Check type of the message
      reducer.selfInbox().expectMessage(ProcessNextBatch)
    }

    "2.4 - send \"correct\" message type to supervisor when all lines in the input set are finished" in {
      val mapper1 = List("key3" -> "1", "key1" -> "2", "key2" -> "1", "key3" -> "1", "key1" -> "1", "key2" -> "1")
      val mapper2 = List()
      val mapper3 = List("key1" -> "1", "key3" -> "2", "key2" -> "3", "key3" -> "1", "key1" -> "1", "key4" -> "1")

      val fileSystem = new InMemoryFileSystem(dataSets = Map("i-1.txt" -> mapper1, "i-2.txt" -> mapper2, "i-3.txt" -> mapper3))
      val dataSet = List("i-3.txt", "i-1.txt", "i-2.txt")

      val reducer = BehaviorTestKit(ReducerNode(1, reducerClass, fileSystem))

      val supervisorInbox = TestInbox[ReducerEvent]()
      reducer.run(StartReducer(dataSet, supervisorInbox.ref))
      //supervisorInbox.expectMessage(ReducerStarted(1))
      supervisorInbox.receiveMessage() // receive ReducerStarted

      // run self-sent ProcessNextBatch
      reducer.runOne()
      // run self-sent ProcessNextBatch
      reducer.runOne()
      // run self-sent ProcessNextBatch
      reducer.runOne()
      // run self-sent ProcessNextBatch
      reducer.runOne()
      // run self-sent ProcessNextBatch
      reducer.runOne()

      val m = supervisorInbox.receiveMessage()
      m.isInstanceOf[ReducerFinished] shouldBe true
    }
  }

}