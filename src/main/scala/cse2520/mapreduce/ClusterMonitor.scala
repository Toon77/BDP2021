package cse2520.mapreduce

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, ChildFailed, SupervisorStrategy, Terminated}

object ClusterMonitor {

  trait MonitorCommand
  private final case object NodesRegistered extends MonitorCommand

  def apply(fileSystem: FileSystem, nodes: Nodes): Behavior[MonitorCommand] =
    Behaviors.setup[MonitorCommand](context => {
      context.log.info("Initializing Map-Reduce application...")

      val partitions = Partitioner(nodes.reducerCount)

      val mappers = (0 until nodes.mapperCount)
        .map(i => {
          val behavior = Behaviors.supervise(MapperNode(i, nodes.mapperClass, partitions, fileSystem))
            .onFailure[Exception](SupervisorStrategy.restart)
          val ref = context.spawn(behavior, s"mapper-$i")
          context.watchWith(ref, SupervisorNode.MapperTerminated(i))
          ref
        })

      val reducers = (0 until nodes.reducerCount)
        .map(i => {
          val behavior = Behaviors.supervise(ReducerNode(i, nodes.reducerClass, fileSystem))
            .onFailure[Exception](SupervisorStrategy.restart)
          val ref = context.spawn(behavior, s"reducer-$i")
          context.watchWith(ref, SupervisorNode.ReducerTerminated(i))
          ref
        })

      val supervisor = Behaviors.supervise(SupervisorNode(fileSystem, mappers, reducers))
        .onFailure[Exception](SupervisorStrategy.stop)

      val supervisorNode = context.spawn(supervisor, "supervisor")
      context.watch(supervisorNode)

      Behaviors.receiveMessage[MonitorCommand] {
        case NodesRegistered =>
          context.log.info("Mapper and reducer nodes are registered to supervisor")
          Behaviors.same
        case mt @ SupervisorNode.MapperTerminated(_) =>
          supervisorNode ! mt
          Behaviors.same
        case rt @ SupervisorNode.ReducerTerminated(_) =>
          supervisorNode ! rt
          Behaviors.same
      }.receiveSignal {
        case (_, ChildFailed(sn, _)) if sn.equals(supervisorNode) =>
          Behaviors.stopped(() => context.system.terminate())
        case (_, Terminated(_)) =>
          context.log.info("Cluster monitor shutting down!")
          Behaviors.stopped
      }
    })
}
