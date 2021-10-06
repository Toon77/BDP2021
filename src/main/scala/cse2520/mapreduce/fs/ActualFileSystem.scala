package cse2520.mapreduce.fs

import java.nio.file.{Files, Path, StandardOpenOption}

import cse2520.mapreduce.{Directories, FileSystem}

import scala.jdk.StreamConverters._
import scala.util.Using

class ActualFileSystem(config: Directories) extends FileSystem {

  private val inDirectory = resolveDirectory(config.inputs)
  private val workDirectory = Path.of(config.work)
  private val outputsDirectory = Path.of(config.outputs)

  private def resolveDirectory(dirPath: String): Path = {
    val path = Path.of(dirPath)
    if (!Files.isDirectory(path))
      throw new IllegalArgumentException(s"Not a directory: ${path.toAbsolutePath.normalize().toString}")

    path
  }

  /**
   * Returns input sets found in the input directory. Input directory is specified using <code>map-reduce.directories.inputs</code>
   * property in <code>application.conf</code> file.
   *
   * Only text files with <code>.txt</code> extension are processed.
   */
  override def getInputSets: List[String] =
    Files.list(inDirectory)
      .filter(p => p.toString.endsWith(".txt"))
      .map(_.toString)
      .toScala(List)

  override def readInputSet(inFile: String): LazyList[String] =
    Files.lines(Path.of(inFile).normalize())
      .toScala(LazyList)

  override def writeLocalSet(outFile: String, values: Seq[(String, String)]): Unit = {
    val outPath = workDirectory.resolve(outFile)
    Files.deleteIfExists(outPath)
    if (outPath.getParent != null && !Files.exists(outPath.getParent)) {
      Files.createDirectories(outPath.getParent)
    }

    Using(Files.newBufferedWriter(outPath, StandardOpenOption.CREATE_NEW)) { writer =>
      values.foreach {
        case (k, v) => writer.write(k + ";" + v + "\n")
      }
    }.get
  }

  override def readRemoteSet(inFile: String): LazyList[(String, String)] =
    Files.lines(workDirectory.resolve(inFile))
      .map(line => {
        val sp = line.split(";")
        sp(0) -> sp(1)
      })
      .toScala(LazyList)

  /**
   * Writes the values to the output directory. The location of output files are specified using
   * <code>map-reduce.directories.outputs</code> property in <code>application.conf</code> file.
   */
  override def writeOutputSet(outFile: String, values: List[String]): Unit = {
    val outPath = outputsDirectory.resolve(outFile)
    Files.deleteIfExists(outPath)
    if (outPath.getParent != null && !Files.exists(outPath.getParent)) {
      Files.createDirectories(outPath.getParent)
    }

    Using(Files.newBufferedWriter(outPath, StandardOpenOption.CREATE_NEW)) { writer =>
      values.foreach(line => writer.write(line + "\n"))
    }.get
  }

}
