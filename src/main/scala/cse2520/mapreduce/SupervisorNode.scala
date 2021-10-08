package cse2520.mapreduce

import akka.actor.TypedActor.self
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import cse2520.mapreduce.MapperNode.StartMapper
import cse2520.mapreduce.ReducerNode.StartReducer
import cse2520.mapreduce.SupervisorNode.SupervisorCommand
import cse2520.mapreduce.task.TaskDispatcher

object SupervisorNode {

  sealed trait SupervisorCommand
  object AssignNextTask extends SupervisorCommand
  case class MapperResponse(event: MapperNode.MapperEvent) extends SupervisorCommand
  case class ReducerResponse(event: ReducerNode.ReducerEvent) extends SupervisorCommand

  case class MapperTerminated(id: Int) extends SupervisorCommand with ClusterMonitor.MonitorCommand
  case class ReducerTerminated(id: Int) extends SupervisorCommand with ClusterMonitor.MonitorCommand

  def apply(fileSystem: FileSystem,
            mappers: IndexedSeq[ActorRef[MapperNode.MapperCommand]],
            reducers: IndexedSeq[ActorRef[ReducerNode.ReducerCommand]]): Behavior[SupervisorCommand] = {

    Behaviors.setup[SupervisorCommand](context => {
      // adapt incoming message MapperEvent to SupervisorCommand
      val mapperAdapter: ActorRef[MapperNode.MapperEvent] = context.messageAdapter[MapperNode.MapperEvent](e => MapperResponse(e))
      // adapt incoming message ReducerEvent to SupervisorCommand
      val reducerAdapter: ActorRef[ReducerNode.ReducerEvent] = context.messageAdapter[ReducerNode.ReducerEvent](e => ReducerResponse(e))

      context.log.info("Supervisor node is starting...")

      val inputSets = fileSystem.getInputSets
      if (inputSets.isEmpty) {
        context.log.error("No input files found in file system. Exiting...")
        Behaviors.stopped
      } else {
        new SupervisorDistributeTasks(context, new SupervisorNode(mapperAdapter, reducerAdapter, fileSystem, inputSets, mappers, reducers))
      }
    })
  }
}

class SupervisorNode(val mapperAdapter: ActorRef[MapperNode.MapperEvent], val reducerAdapter: ActorRef[ReducerNode.ReducerEvent],
                     val fileSystem: FileSystem, val inputSets: List[String],
                     val mappers: IndexedSeq[ActorRef[MapperNode.MapperCommand]],
                     val reducers: IndexedSeq[ActorRef[ReducerNode.ReducerCommand]])

class SupervisorDistributeTasks(context: ActorContext[SupervisorCommand], node: SupervisorNode)
  extends AbstractBehavior[SupervisorCommand](context) {

  import SupervisorNode._

  context.log.info("Supervisor now has all node information and distributing tasks.")

  val dispatcher = new TaskDispatcher(node.mappers.indices, node.inputSets.indices)
  var intermediatesByPartition = Map[Int, List[String]]()

  // TODO: Supervisor behavior in Q3.1 (5 pts)
  context.self.tell(AssignNextTask)

  // TODO (for onMessage)
  // Q 3.2
  override def onMessage(msg: SupervisorCommand): Behavior[SupervisorCommand] = msg match {
    case AssignNextTask if dispatcher.canDispatch =>
      context.log.info("Assign next input set to the next available mapper.")
      val task = dispatcher.dispatch.get
      node.mappers(task.processor).tell(StartMapper(task.id, node.inputSets(task.id), node.mapperAdapter))
      this.context.self.tell(AssignNextTask)

      context.log.info("Sending task {} to mapper {}", task.id, task.processor)
      Behaviors.same
    case AssignNextTask if dispatcher.allSetsProcessed =>
      context.log.info("All input sets are processed. Starting reduce operation...")
      intermediatesByPartition.foreach {
        // TODO
        // Q 3.4
        case (id, sets) => node.reducers(id).tell(StartReducer(sets, node.reducerAdapter))
      }
      new SupervisorReduceIntermediates(context, node, intermediatesByPartition, Map())
    case AssignNextTask =>
      context.log.info("Waiting for all input sets to be processed. Remaining: {}...", dispatcher.busyProcessors)
      //this.context.self.tell(AssignNextTask)
      Behaviors.same
    // TODO
    // Q 3.3
    case MapperResponse(event) =>
      event match {
        case MapperNode.MapperStarted(id, taskId) if dispatcher.taskInProgress(id, taskId) =>
          context.log.info("Confirmed mapper {} assignment to task {}", id, taskId)
          Behaviors.same
        case MapperNode.MapperFinished(id, taskId, intermediates) if dispatcher.taskInProgress(id, taskId) =>
          context.log.info("Mapper {} has finished execution of task {}", id, taskId)
          intermediatesByPartition = intermediates
            .foldRight(intermediatesByPartition) {
              case ((pId, set), map) =>
                map + (pId -> (set +: map.getOrElse(pId, List())))
            }
          dispatcher.processFinished(id)
          context.self.tell(AssignNextTask)
          Behaviors.same
        case _ => Behaviors.unhandled
      }

    // TODO
    case MapperTerminated(id) if dispatcher.getProcess(id).isDefined =>
    context.log.info("Mapper {} didn't finish successfully, trying again...", id)
        dispatcher.processFailed(id)
      Behaviors.same
    case _ => Behaviors.unhandled
  }

  def mapperAdapter: ActorRef[MapperNode.MapperEvent] = node.mapperAdapter

  def reducerAdapter: ActorRef[ReducerNode.ReducerEvent] = node.reducerAdapter
}

class SupervisorReduceIntermediates(context: ActorContext[SupervisorCommand], node: SupervisorNode,
                                    intermediatesByPartition: Map[Int, List[String]], results: Map[Int, String])
  extends AbstractBehavior[SupervisorCommand](context) {

  import SupervisorNode._

    // TODO
    override def onMessage(msg: SupervisorCommand): Behavior[SupervisorCommand] = msg match {
      case _ if (results.size == node.reducers.size) =>
      context.log.info("Job completed!")
          Behaviors.stopped
      case AssignNextTask =>
      Behaviors.same
      case ReducerTerminated(id) =>
       context.log.info("Reducer {} encountered an error. Re-sending data sets...", id)
          if (intermediatesByPartition.contains(id)) {
            val dataSet = intermediatesByPartition(id)
      }
      Behaviors.same
    case ReducerResponse(event) =>
      event match {
        case ReducerNode.ReducerStarted(id) =>
          context.log.info("Confirmed reducer {} starting reduce operation", id)
          Behaviors.same
        case ReducerNode.ReducerFinished(id, dataSet) =>
          context.log.info("Reducer {} has completed work. Results are available at {}", id, dataSet)
          if (results.size == node.reducers.size)  {
            context.log.info("Job completed!")
            Behaviors.stopped
          } else {
            new SupervisorReduceIntermediates(context, node, intermediatesByPartition, results + (id -> dataSet))
          }
        case _ =>
          Behaviors.unhandled
      }
  }

  def reducerAdapter: ActorRef[ReducerNode.ReducerEvent] = node.reducerAdapter
}
