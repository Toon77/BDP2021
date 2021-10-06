package cse2520.mapreduce

import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import cse2520.mapreduce.MapperNode.{MapperEvent, MapperFinished, MapperStarted, ProcessNextBatch, StartMapper}
import cse2520.mapreduce.fs.InMemoryFileSystem
import cse2520.mapreduce.test.MockMapper
import org.scalatest.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MapperBehaviorTest extends AnyWordSpecLike with Matchers {

  private val mapperClass = classOf[MockMapper].asInstanceOf[Class[Mapper]]

  "mapper" should {

    "1.1 - acknowledge its start to the supervisor with correct message type" in {

      val hashPartitioner = Partitioner(3)
      val fileSystem = new InMemoryFileSystem(Map(
        "dataset-1.txt" -> List("emit-4", "emit-1", "emit-3", "emit-1", "emit-2", "emit-5")
      ))

      val mapper = BehaviorTestKit(MapperNode(1, mapperClass, hashPartitioner, fileSystem))

      val supervisorInbox = TestInbox[MapperEvent]()
      mapper.run(StartMapper(1, "dataset-1.txt", supervisorInbox.ref))

      // check message type
      val m = supervisorInbox.receiveMessage()
      m.isInstanceOf[MapperStarted] shouldBe true
    }


    "1.2 - send message for processing next batch if there are more data" in {

      val hashPartitioner = Partitioner(3)
      val fileSystem = new InMemoryFileSystem(Map(
        "dataset-1.txt" -> List("emit-4", "emit-1", "emit-3", "emit-1", "emit-2", "emit-5")
      ))

      val mapper = BehaviorTestKit(MapperNode(1, mapperClass, hashPartitioner, fileSystem))

      val supervisorInbox = TestInbox[MapperEvent]()
      mapper.run(StartMapper(1, "dataset-1.txt", supervisorInbox.ref))

      // run self-sent ProcessNextBatch
      mapper.runOne()

      // expect to receive ProcessNextBatch to continue
      mapper.selfInbox().expectMessage(ProcessNextBatch)
    }

    "1.3 - send \"correct\" message type to supervisor when all lines in the input set are finished" in {
      val hashPartitioner = Partitioner(3)
      val fileSystem = new InMemoryFileSystem(Map(
        "dataset-1.txt" -> List("emit-4", "emit-1", "emit-3")
      ))

      val mapper = BehaviorTestKit(MapperNode(1, mapperClass, hashPartitioner, fileSystem))

      val supervisorInbox = TestInbox[MapperEvent]()
      mapper.run(StartMapper(1, "dataset-1.txt", supervisorInbox.ref))

      supervisorInbox.expectMessage(MapperStarted(1, 1))

      // run self-sent ProcessNextBatch (processes data)
      mapper.runOne()
      // run self-sent ProcessNextBatch (processes data)
      mapper.runOne()
      // run self-sent ProcessNextBatch (processes data)
      mapper.runOne()
      // run self-sent ProcessNextBatch (no more data, behavior change)
      mapper.runOne()

      // expect message to supervisor of correct type
      val m = supervisorInbox.receiveMessage()
      m.isInstanceOf[MapperFinished] shouldBe true
    }
  }
}
