# Assignment: Map-Reduce with Akka Typed

In this assignment, we present you with an Actor System implementing a simplified Map-Reduce framework. Your task is to complete the system with correct behaviour in the nodes.

## What is Map-Reduce?

Map-Reduce is a programming model, is useful for processing batches of large datasets on a cluster. The user-defined "map" operation processes input sets and generates intermediate lists of key-value pairs.
The Map-Reduce framework sorts and aggregates the intermediate lists, and calls the user-defined "reduce" operation.
"Reduce" operation merges all related values that are associated with the given intermediate key, producing the output.

This model was first introduced in 2004 [in this paper](http://static.googleusercontent.com/media/research.google.com/es/us/archive/mapreduce-osdi04.pdf)
and used in Google extensively, most notably to build the index for World Wide Web.

## Actor Model and Akka Typed

The Actor model defines **actor**s to be the primitive units of concurrent computation. Actors can only interact with other actors by sending and receiving **messages** forming an **actor system**. It's not possible to access and/or change the state of other actors - the actor instances are inaccessible. Each actor processes messages serially, one after another.

[Akka](https://akka.io/) is a popular implementation of the actor model of computation in Scala and Java. In Akka, each actor has a partial function called `receive()`. This function accepts any type of message and is effectively the behaviour of the actor. Akka framework enforces communication through messaging with the `ActorRef` class.

<!-- A drawback of Akka is that it's not possible to verify the message types (also called the "protocol") between any two actors to be correct - not without extensive testing. The `receive()` function is of type `PartialFunction[Any, Unit]` and
`ActorRef` class is not generic. -->

In this assignment, we will use [Akka Typed](https://doc.akka.io/docs/akka/current/typed/index.html) which extends Akka by specifying the types of input messages,
making the compiler point out protocol inconsistencies during build time. Typed actors send messages of type `T` with the help of the `ActorRef[T]` class. The actor behaviour has type information too, with the `Behavior[T]` type. Akka Typed library ensures that the generic parameter `T` of `ActorRef[T]` and `Behavior[T]` always match - by providing appropriate,
generic functions.

## Assignment

In this assignment, we have a simplified map-reduce framework, similar to the structure in the original [Map-Reduce
paper](http://static.googleusercontent.com/media/research.google.com/es/us/archive/mapreduce-osdi04.pdf). We implemented
the WordCount example, which is also presented in the paper. You can find the _map_ and _reduce_ operations in the `wordcount`
package.

The following figure gives an overview of the actors and their tasks in this application:

![System Overview](./docs/mr-overview.png)

We model this cluster setup with four types of actors:

- `MapperNode` actors perform `map` operation.

- `ReducerNode` actors perform `reduce` operation.

- `Supervisor` actor assigns `map` and `reduce` tasks to the mapper and reducers.

- `ClusterMonitor` actor sets up the other actors.

Note: This representation of actors use the [functional style](https://doc.akka.io/docs/akka/current/typed/style-guide.html#functional-versus-object-oriented-style)
because it's easier to reference required parameters from the closure context.

## Before Starting...
- TODOs are placed above blocks of code which you are expected to complete.
- You need to figure out which question corresponds to which TODO (except 3.1 because it's labeled for you).
- To find TODOs, IntelliJ has a button called TODO at bottom left.
- **Protip: Invest time in understanding the template provided.**

## Assignment 

In this assignment, you will have to figure out what [fire and forget](https://doc.akka.io/docs/akka/current/typed/interaction-patterns.html#fire-and-forget) messages must be exchanged between the `MapperNode`,  `ReducerNode` and `SupervisorNode` and fill them in the program. You can understand the behaviour of the application by looking at the log messages. 

Your solution should follow these communication requirements for each node:

1. **Mapper Behavior** _(25 pts)_

   A **Mapper** actor is in the _idle_ state, until the **Supervisor** actor sends a `StartMapper` message. With this message, the **Supervisor** explicitly assigns an input set to process. Then, the mapper node should read the input set line by line, calling the `mapper.map()` method for each line.

   **Q1.1. (10 pts)** When the `StartMapper` message arrives, the **Mapper** acknowledges by sending the `MapperStarted` message to the **Supervisor** . Then, the **Mapper** triggers reading the input file by sending itself a `ProcessNextBatch` message. 

   **Q1.2. (5 pts)** When the `ProcessNextBatch` message arrives, and there are more lines to process, **Mapper** calls the` mapper.map()` method. If the operation is successful, it triggers reading the rest of the input file by sending itself a `ProcessNextBatch` message. It does only continue if there is no failure in the operation.

   **Q1.3. (10 pts)** When **Mapper** processes all lines in the input set, it sends a `MapperFinished` message to the **Supervisor** and goes back to _idle_ state.

2. **Reducer Behavior** _(25 pts)
   **Reducer** nodes wait for the completion of all mapper nodes. Then, **Supervisor** should send a `StartReducer` message to every reducer along with the inputs sets designated for each reducer. Different from **Mapper**, **Reducer** has to aggregate all keys from each of the intermediate data sets.

	**Reducer** is in the _idle_ state, ignoring all other messages, until a `StartReducer` message arrives.

   **Q2.1. (5 pts)** When the `StartReducer` message arrives, **Reducer** acknowledges by sending a `ReducerStarted` message to the **Supervisor**.

   **Q2.2. (5 pts)** **Reducer** reads all intermediate sets and collect all values with the same keys. It also sorts these keys.
It sends `ProcessNextBatch` message to itself to start processing aggregated keys and values.

   **Q2.3. (5 pts)** When the `ProcessNextBatch` message arrives and there is an unprocessed key, **Reducer** calls the` reducer.reduce()` method. Similar to the mapper case, the **Reducer** only continues if there are no errors.

   **Q2.4. (10 pts)** When **Reducer** processes all keys and values, it sends a `ReducerFinished` message back to **Supervisor** and goes back to _idle_ state.

3. **Supervisor Behavior** _(35 pts)_

   The **Supervisor** actor selects an idle node and sends it a map or reduce task. The **Supervisor** also acts as a channel between the file system and the worker nodes.

   The **Supervisor** starts by registering the nodes in an _initializing_ state and moves to the _distributeTasks_ state.

   **Q3.1. (5 pts)** Then the **Supervisor** starts assigning input sets by sending itself `AssignNextTask`. 
   
   **Q3.2. (15 pts)** When the `AssignNextTask ` message arrives and there are mapping tasks to dispatch, **Supervisor** assigns an idle mapper a mapping task by sending it a `StartMapper` message with the task parameters. Then, it sends `AssignNextTask ` to itself to assign the next task.
   
   **Q3.3. (5 pts)** When a **Mapper** is finishes its task, the **Supervisor** sends `AssignNextTask ` to itself to assign the next task to the **Mapper**.

   **Q3.4. (10 pts)** When the `AssignNextTask ` message arrives and there are no more mapping tasks to dispatch, **Supervisor** assigns reducing tasks to reducer nodes by sending them a `StartReducer` messages with the task parameters. 
   
   Then, the **Supervisor** moves to the `SupervisorReduceIntermediates` state. In this state, the intermediate key/value pairs generated by the **Mapper**s are reduced using the particular reduce function.
   

4. **Fault-tolerance** _(15 pts)_
      
	The system tolerates faults that may lead to termination of mapper or reducer nodes. If a mapper or reducer actor terminates, these actors are restarted. We configure this while we create the actors on lines 19-20 and 29-30 in `ClusterMonitor.scala`:

	```scala
	val behavior = Behaviors.supervise(MapperNode(i, nodes.mapperClass, partitions, fileSystem))
   	         	.onFailure[Exception](SupervisorStrategy.restart)
	```
	
	```scala
	val behavior = Behaviors.supervise(ReducerNode(i, nodes.reducerClass, fileSystem))
            .onFailure[Exception](SupervisorStrategy.restart)
   ```

	In case a **Mapper** or a **Reducer** terminates, the **ClusterMonitor** sends `MapperTerminated` or `ReducerTerminated` message to the **Supervisor** respectively, so that it can restart the terminated tasks.

   **Q4.1. (5 pts)** When the **Supervisor** receives `MapperTerminated` message, it sends `AssignNextTask ` to itself to assign the task to a **Mapper**.
   
   **Q4.2. (10 pts)** When the **Supervisor** receives `ReducerTerminated` message, it sends `StarReducer ` message to the itself to assign the task to the **Reducer**.

## Submission
Zip scala directory to submit on [CPM](https://cpm.ewi.tudelft.nl/logIn.php) the same way as in previous assignments.

## Tools and setup
* Scala: **2.13.3**
* Java: **11** (suggested, 11 is minimum)

### IDE
We recommend using IntelliJ for this assignment. If you do not already have this, you can apply for a student license on the [Jetbrains website](https://www.jetbrains.com/student/) and afterwards download IntelliJ there.


### Setup process
1. Install IntelliJ.
2. Install the Scala plugin (IntelliJ -> Settings/Preferences -> Plugins -> Marketplace, search for "Scala"). This will restart IntelliJ.
3. Import the template folder as a Maven project.
4. You are ready to start working :) 

### CPM Submissions
Note that CPM considers a grade of 0 as `script dissaproved`. Ignore this warning.