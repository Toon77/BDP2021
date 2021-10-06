package cse2520.mapreduce

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import cse2520.mapreduce.MapperNode.{InputSetState, MapperCommand, MapperEvent}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object MapperNode {

  sealed trait MapperCommand
  case class StartMapper(taskId: Int, inFile: String, supervisor: ActorRef[MapperEvent]) extends MapperCommand
  case object ProcessNextBatch extends MapperCommand

  sealed trait MapperEvent
  case class MapperStarted(id: Int, taskId: Int) extends MapperEvent
  case class MapperFinished(id: Int, taskId: Int, intSets: Map[Int, String]) extends MapperEvent

  final case class InputSetState(name: String, lines: Iterator[String], index: Int)

  def apply(id: Int, mapperClass: Class[Mapper],
            partitions: Partitioner, fileSystem: FileSystem): Behavior[MapperCommand] =
    Behaviors.setup(context => {
      val mapper = mapperClass.getConstructor().newInstance()
      context.log.info("Mapper {} is available.", id)

      new MapperIdle(context, new MapperNode(id, fileSystem, mapper, partitions))
    })
}

class MapperNode(val id: Int, val fileSystem: FileSystem, val mapper: Mapper, val partitions: Partitioner)


class MapperIdle(context: ActorContext[MapperCommand], node: MapperNode)
  extends AbstractBehavior[MapperCommand](context) {

  import MapperNode._

  // TODO
  // Q 1.1
  override def onMessage(msg: MapperCommand): Behavior[MapperCommand] = msg match {
    case StartMapper(taskId, inputSet, supervisor) =>
      context.log.info("Starting a MapperNode")
      val iterator = node.fileSystem.readInputSet(inputSet).iterator
      supervisor.tell(MapperStarted(this.node.id, taskId))
      this.context.self.tell(ProcessNextBatch)
      new MapperInProgress(context, node, taskId, InputSetState(inputSet, iterator, 0), new PartitioningBufferedEmitter(node.partitions), supervisor)
    case _ => Behaviors.unhandled
  }

  override def onSignal: PartialFunction[Signal, Behavior[MapperCommand]] = {
    case PostStop =>
      context.log.info("Mapper {} stopped [from idle]", node.id)
      Behaviors.stopped
  }
}

class MapperInProgress(context: ActorContext[MapperCommand], node: MapperNode, taskId: Int, inputSet: InputSetState,
                       emitter: PartitioningBufferedEmitter, supervisor: ActorRef[MapperEvent])
  extends AbstractBehavior[MapperCommand](context) {

  import MapperNode._

  context.log.info("Mapper {} is starting task {} ...", node.id, taskId)

  // TODO
  // Q1.2
  override def onMessage(msg: MapperCommand): Behavior[MapperCommand] = msg match {
    case ProcessNextBatch if inputSet.lines.hasNext =>
      context.log.info("Mapper {} is processing task {} [line={}] ...", node.id, taskId, inputSet.index)
      // do the task! - (emitter, key, value)

      val mapResult = node.mapper.map(emitter, inputSet.name, inputSet.lines.next())
      mapResult match {
        case Success(_) =>
          context.self.tell(ProcessNextBatch)
          new MapperInProgress(context, node, taskId, inputSet.copy(index = inputSet.index + 1), emitter, supervisor)
        case Failure(e) =>
          context.log.info("Mapper {} error processing {}.", node.id, inputSet, e)
          //Behaviors.stopped
          throw e
      }
    // TODO
    case ProcessNextBatch =>
      context.log.info("Mapper {} has completed processing task {}.", node.id, taskId)

      // maps partition indices to file names
      val intermediates = (0 until node.partitions.count)
        // take buffered data
        .map(i => (i, emitter.getData(i)))
        .filter(_._2.nonEmpty)
        .map(p => p._1 -> s"partition-${p._1}/task-$taskId.txt")
        .toMap

      intermediates.foreach {
        //  for each partition index, write data to corresponding file
        case (i, outFileName) => node.fileSystem.writeLocalSet(outFileName, emitter.getData(i))
      }
      new MapperIdle(context, node)
    case _ => Behaviors.unhandled
  }
}