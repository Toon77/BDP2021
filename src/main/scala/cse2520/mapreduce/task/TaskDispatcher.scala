package cse2520.mapreduce.task

import scala.collection.immutable.SortedSet

class TaskDispatcher(val processors: Seq[Int],
                     private val initialTasks: Seq[Int]) {

  private var freeProcessors = processors.to(SortedSet)
  private var unassignedTasks = initialTasks.to(SortedSet)

  private var assignedTasksByProcessor = Map[Int, Task]()

  /**
   * Used to check if all input sets are processed. Doesn't change the state of dispatcher.
   *
   * @return `true` if all input sets are processed.
   */
  def allSetsProcessed: Boolean = (freeProcessors.size == processors.size) && unassignedTasks.isEmpty

  /**
   * Used to check if this dispatcher can assign a task to a processor. Doesn't change the state of dispatcher.
   *
   * @return `true` if there's a task and a free processor to process it.
   */
  def canDispatch: Boolean = freeProcessors.nonEmpty && unassignedTasks.nonEmpty

  /**
   * Assigns a task to the next available processor, if there are enough processors. This method modifies the state of
   * the dispatcher.
   *
   * @return The assignment or `None` if no processors are available.
   */
  def dispatch: Option[Task] = if (canDispatch) {
    val task = Task(id = unassignedTasks.head, processor = freeProcessors.head)

    assignedTasksByProcessor = assignedTasksByProcessor + (task.processor -> task)
    freeProcessors = freeProcessors.tail
    unassignedTasks = unassignedTasks.tail

    Some(task)
  } else {
    None
  }

  /**
   * Used to check if the given parameters represent a valid task, which is still in progress. Doesn't change the state
   * of dispatcher.
   *
   * @param process processor id
   * @param task task id
   * @return Returns `true` when the given `process` and `task` both exist, and are assigned to each other.
   */
  def taskInProgress(process: Int, task: Int): Boolean = assignedTasksByProcessor.get(process).exists(_.id == task)

  /**
   * Marks the given processor as failed. We assume that the tasks are side-effect free and can be re-tried. The
   * processor and the task queued again. This method changes the state of dispatcher.
   *
   * @param process processor id
   */
  def processFailed(process: Int): Unit = {
    assignedTasksByProcessor.get(process)
      .foreach {
        case Task(id, _) =>
          freeProcessors = freeProcessors + process
          unassignedTasks = unassignedTasks + id
          assignedTasksByProcessor = assignedTasksByProcessor.removed(process)
      }
  }

  /**
   * Marks the given processor as succeeded, giving the processor back to the pool and forgetting the task. This method
   * changes the state of dispatcher.
   *
   * @param process processor id
   */
  def processFinished(process: Int): Unit = {
    assignedTasksByProcessor.get(process)
      .foreach {
        case Task(_, _) =>
          freeProcessors = freeProcessors + process
          assignedTasksByProcessor = assignedTasksByProcessor.removed(process)
      }
  }

  /**
   * Returns the task that given processor is working on. Doesn't change the state of dispatcher.
   *
   * @param process processor id
   * @return Some task if processor is not idle, None otherwise
   */
  def getProcess(process: Int): Option[Task] = assignedTasksByProcessor.get(process)

  /**
   * Returns the count of free processors that this dispatcher knows about. Doesn't change the state of dispatcher.
   * @return the number of free processors
   */
  def busyProcessors: Int = processors.size - freeProcessors.size
}

