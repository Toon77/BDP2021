package cse2520.mapreduce

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import cse2520.mapreduce.ReducerNode.{AggregateSetState, ProcessNextBatch, ReducerCommand, ReducerEvent, ReducerFinished}

import scala.util.{Failure, Success, Try}

object ReducerNode {

  sealed trait ReducerCommand

  case class StartReducer(dataSet: List[String], supervisor: ActorRef[ReducerEvent]) extends ReducerCommand
  case object ProcessNextBatch extends ReducerCommand

  sealed trait ReducerEvent
  case class ReducerStarted(id: Int) extends ReducerEvent
  case class ReducerFinished(id: Int, dataSet: String) extends ReducerEvent

  case class AggregateSetState(lines: Iterator[(String, List[String])], index: Int)

  def apply(id: Int, reducerClass: Class[Reducer], fileSystem: FileSystem): Behavior[ReducerCommand] =
    Behaviors.setup(context => {
      val reducer = reducerClass.getConstructor().newInstance()
      context.log.info("Reducer {} is available.", id)

      new ReducerIdle(context, new ReducerNode(id, fileSystem, reducer))
    })
}

class ReducerNode(val id: Int, val fileSystem: FileSystem, val reducer: Reducer)

class ReducerIdle(context: ActorContext[ReducerCommand], node: ReducerNode)
  extends AbstractBehavior[ReducerCommand](context) {

  import ReducerNode._

    // TODO
    // Q 2.1
    override def onMessage(msg: ReducerCommand): Behavior[ReducerCommand] = msg match {
      case StartReducer(intSet, supervisor) =>
    context.log.info("Reducer {} is starting processing {} data sets...", node.id, intSet.size)
          supervisor.tell(ReducerStarted(node.id))
      aggregateDataSets(context, node, intSet, supervisor)
  }

    // TODO
    private def aggregateDataSets(context: ActorContext[ReducerCommand], node: ReducerNode,
                                dataSets: List[String], supervisor: ActorRef[ReducerEvent]): ReducerReduce = {
      context.log.info("Reducer {} is preparing aggregation of intermediate sets.", node.id)

      // bring together the (key, value) datasets from mappers
    val aggregateMap = dataSets.flatMap(node.fileSystem.readRemoteSet)
    .groupBy(_._1)
    .toSeq
    .sortBy(_._1)
    .map(kvs => kvs._1 -> kvs._2.map(kv => kv._2).sorted)

      context.log.info("Reducer {} is starting reduce job ...", node.id)
    new ReducerReduce(context, node, AggregateSetState(aggregateMap.iterator, 0), new BufferedEmitter(), supervisor)
  }

  override def onSignal: PartialFunction[Signal, Behavior[ReducerCommand]] = {
      case PostStop =>
        context.log.info("Reducer {} stopped [from collect-intermediates]", node.id)
        Behaviors.stopped
  }
}

class ReducerReduce(context: ActorContext[ReducerCommand], node: ReducerNode,
                    setState: AggregateSetState, emitter: BufferedEmitter, supervisor: ActorRef[ReducerEvent])
  extends AbstractBehavior[ReducerCommand](context: ActorContext[ReducerCommand]) {

    // TODO
    override def onMessage(msg: ReducerCommand): Behavior[ReducerCommand] = msg match {
      case ProcessNextBatch if setState.lines.hasNext =>
    context.log.info("Reducer {} is processing key [key={}] ...", node.id, setState.index)
        Try(setState.lines.next())
      .flatMap(kvs => node.reducer.reduce(emitter, kvs._1, kvs._2)) match {
          case Success(_) =>

          new ReducerReduce(context, node, setState.copy(index = setState.index + 1), emitter, supervisor)
        case Failure(e) =>
          context.log.info("Reducer {} error processing values.", node.id, e)
          throw e
      }
    // TODO
    case ProcessNextBatch =>
    context.log.info("Reducer {} has completed processing.", node.id)
        val dataSet = s"reducer-${node.id}.txt"
        node.fileSystem.writeOutputSet(dataSet, emitter.getData.map(s => s"${s._1}: ${s._2}"))
      new ReducerIdle(context, node)
  }
}