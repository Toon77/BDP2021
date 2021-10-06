package cse2520.mapreduce

import akka.actor.testkit.typed.scaladsl.{BehaviorTestKit, TestInbox}
import cse2520.mapreduce.MapperNode.{MapperCommand, MapperFinished, MapperStarted, StartMapper}
import cse2520.mapreduce.ReducerNode.{ReducerCommand, ReducerStarted, StartReducer}
import cse2520.mapreduce.SupervisorNode.{AssignNextTask, MapperResponse, MapperTerminated, ReducerResponse, ReducerTerminated}
import cse2520.mapreduce.fs.InMemoryFileSystem
import org.scalatest.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.event.Level

class SupervisorBehaviorTest extends AnyWordSpecLike with Matchers {

  "supervisor" should {

    "3.1 - start assigning tasks to mappers on initialization" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map("dataset-0.txt" -> List("Line 1", "Line 2")))

      val testMapper = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper.ref), IndexedSeq(testReducer.ref)))

      // sends itself AssignNewTask
      supervisor.selfInbox().expectMessage(AssignNextTask)
    }

    "3.2 - send message to the correct node - the first idle mapper to assign task" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map("dataset-0.txt" -> List("Line 1", "Line 2")))

      val testMapper1 = TestInbox[MapperCommand]()
      val testMapper2 = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper1.ref, testMapper2.ref), IndexedSeq(testReducer.ref)))

      supervisor.runOne()

      // first mapper receives the message
      testMapper1.hasMessages shouldBe true
    }

    "3.3 - assign next task when mapper is finished" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map(
        "dataset-0.txt" -> List("Line 1"),
        "dataset-1.txt" -> List("Line 2")
      ))

      val testMapper1 = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper1.ref), IndexedSeq(testReducer.ref)))

      // run AssignNextTask and distribute the first map task
      supervisor.runOne()

      /*supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Supervisor node is starting...",
        "Supervisor now has all node information and distributing tasks.",
        "Assign next input set to the next available mapper.",
        "Sending task 0 to mapper 0")
      supervisor.clearLog()*/

      // run AssignNextTask, but cannot dispatch since mapper is busy
      supervisor.runOne()
      /*supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Waiting for all input sets to be processed. Remaining: 1...")*/

      supervisor.run(MapperResponse(MapperFinished(0, 0, Map(0 -> "partition-0/task-0.txt"))))

      // it must send AssignNextTask message to itself
      supervisor.selfInbox().expectMessage(AssignNextTask)
    }

    "3.4 - assign reduce tasks to correct nodes" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map(
        "dataset-0.txt" -> List("Line 1"),
        "dataset-1.txt" -> List("Line 2")
      ))

      val testMapper1 = TestInbox[MapperCommand]()
      val testMapper2 = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper1.ref, testMapper2.ref), IndexedSeq(testReducer.ref)))

      // bring to reduce step: ----
      // run AssignNewTask
      supervisor.runOne()

      supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Supervisor node is starting...",
        "Supervisor now has all node information and distributing tasks.",
        "Assign next input set to the next available mapper.",
        "Sending task 0 to mapper 0")

      supervisor.clearLog()

      // run AssignNewTask
      supervisor.runOne()

      supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Assign next input set to the next available mapper.",
        "Sending task 1 to mapper 1")
      supervisor.clearLog()

      // run AssignNewTask
      supervisor.runOne()

      supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Waiting for all input sets to be processed. Remaining: 2...")
      supervisor.clearLog()

      // run MapperFinished
      supervisor.run(MapperResponse(MapperFinished(0, 0, Map(0 -> "partition-0/task-0.txt"))))
      // run self-sent AssignNewTask
      supervisor.runOne()

      /*
      supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Mapper 0 has finished execution of task 0",
        "Waiting for all input sets to be processed. Remaining: 1...")
      supervisor.clearLog() */

      //supervisor.selfInbox().expectMessage(AssignNextTask)
      //supervisor.selfInbox().expectMessage(AssignNextTask)
      //supervisor.selfInbox().hasMessages shouldBe false

      supervisor.selfInbox().hasMessages shouldBe false

      // run MapperFinished
      supervisor.run(MapperResponse(MapperFinished(1, 1, Map(0 -> "partition-0/task-1.txt"))))
      //supervisor.selfInbox().expectMessage(AssignNextTask)

      testReducer.hasMessages shouldBe false
      // run self-sent AssignNewTask
      supervisor.runOne()

      /*
      supervisor.logEntries()
        .filter(_.level == Level.INFO).map(_.message) shouldBe Seq(
        "Mapper 1 has finished execution of task 1",
        "All input sets are processed. Starting reduce operation..."
      ) */
      supervisor.clearLog()

      // check whether message sent to the correct receiver:
      testReducer.hasMessages shouldBe true
    }

    "4.1 - reschedule mapper if it mapper terminates" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map(
        "dataset-0.txt" -> List("Line 1"),
        "dataset-1.txt" -> List("Line 2")
      ))

      val testMapper1 = TestInbox[MapperCommand]()
      val testMapper2 = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper1.ref, testMapper2.ref), IndexedSeq(testReducer.ref)))

      // run the self-sent AssignNextTask
      supervisor.runOne()
      supervisor.clearLog() // drain the logs

      supervisor.run(MapperResponse(MapperStarted(0, 0)))
      supervisor.run(MapperTerminated(0))

      supervisor.selfInbox().expectMessage(AssignNextTask)
    }

    "4.2 - reschedule reducer if a reducer terminates - send message to correct reducer" in {
      val fileSystem = new InMemoryFileSystem(inputs = Map(
        "dataset-0.txt" -> List("Line 1")
      ))

      val testMapper1 = TestInbox[MapperCommand]()
      val testReducer = TestInbox[ReducerCommand]()

      val supervisor = BehaviorTestKit(SupervisorNode(fileSystem, IndexedSeq(testMapper1.ref), IndexedSeq(testReducer.ref)))

      // run the self-sent AssignNextTask
      supervisor.runOne()
      // run AssignNewTask
      supervisor.runOne()

      // run MapperFinished
      supervisor.run(MapperResponse(MapperFinished(0, 0, Map(0 -> "partition-0/task-0.txt"))))
      // run self-sent AssignNewTask
      supervisor.runOne()
      supervisor.selfInbox().hasMessages shouldBe false

      // The supervisor has reached the SupervisorReduceIntermediates behavior
      supervisor.currentBehavior shouldBe a[SupervisorReduceIntermediates]

      testReducer.receiveMessage()
      testReducer.hasMessages shouldBe false

      supervisor.run(ReducerResponse(ReducerStarted(0)))
      supervisor.run(ReducerTerminated(0))

      // If a reducer terminates:

      // check whether the message is sent to the correct reducer
      testReducer.hasMessages shouldBe true

      // check whether correct type of message is sent
      val m = testReducer.receiveMessage()
      m.isInstanceOf[StartReducer] shouldBe true

      // check whether correct dataset is sent
      val message = m.asInstanceOf[StartReducer]
      message.dataSet shouldBe List("partition-0/task-0.txt")

      // check whether the supervisor is in the correct state
      supervisor.currentBehavior shouldBe a[SupervisorReduceIntermediates]
    }
  }
}
