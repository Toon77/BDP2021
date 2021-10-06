package cse2520.mapreduce.fs

import cse2520.mapreduce.FileSystem

// Used for tests
class InMemoryFileSystem(val inputs: Map[String, List[String]] = Map(),
                         val dataSets: Map[String, List[(String, String)]] = Map()) extends FileSystem {

  var intermediates: Map[String, List[(String, String)]] = Map()
  var outputs: Map[String, List[String]] = Map()

  override def getInputSets: List[String] = inputs.keys.toList

  override def readInputSet(inFile: String): LazyList[String] = inputs(inFile).to(LazyList)

  override def writeLocalSet(outFile: String, values: Seq[(String, String)]): Unit =
    intermediates = intermediates + (outFile -> values.toList)

  override def readRemoteSet(inFile: String): LazyList[(String, String)] = dataSets.getOrElse(inFile, intermediates(inFile)).to(LazyList)

  override def writeOutputSet(outFile: String, values: List[String]): Unit = outputs = outputs + (outFile -> values)
}
