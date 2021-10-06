package cse2520.mapreduce

import akka.actor.typed.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import cse2520.mapreduce.fs.ActualFileSystem
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object App {

  private val usage =
    """
      |Usage:
      |   mapreduce-app <input-dir>
      |""".stripMargin

  private val log = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val overrideConfig = if (args.length == 1)
      ConfigFactory.parseString(s"map-reduce.directories.input=${args.apply(0)}")
    else
      ConfigFactory.empty()

    val config = overrideConfig.withFallback(ConfigFactory.load())
    loadJobConfig(config) match {
      case Success(jobConfig) =>
        log.info("Starting actor system...")
        val fileSystem = new ActualFileSystem(jobConfig.dirs)
        ActorSystem[ClusterMonitor.MonitorCommand](ClusterMonitor(fileSystem, jobConfig.nodes), "map-reduce-system", config)
      case Failure(e) =>
        println(e.getMessage)
        println(usage)
    }
  }

  private def directories(config: Config): Try[Directories] = Try {
    Directories(config.getString("inputs"), config.getString("work"), config.getString("outputs"))
  }

  private def nodes(config: Config): Try[Nodes] = Try {
    val mapperClass = Class.forName(config.getString("mapper-class"))
      .asInstanceOf[Class[Mapper]]
    val reducerClass = Class.forName(config.getString("reducer-class"))
      .asInstanceOf[Class[Reducer]]

    Nodes(config.getInt("mapper-count"), mapperClass,
      config.getInt("reducer-count"), reducerClass)
  }

  def loadJobConfig(config: Config): Try[JobConfig] = Try {
    config.getConfig("map-reduce")
  }.flatMap { root =>
    for {
      dirs <- directories(root.getConfig("directories"))
      nodes <- nodes(root.getConfig("nodes"))
    } yield JobConfig(dirs, nodes)
  }
}