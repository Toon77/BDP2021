package cse2520.mapreduce.wordcount

import cse2520.mapreduce.{Emitter, Mapper}

import scala.util.Try

class WordCountMapper extends Mapper {

  override def map(mapEmitter: Emitter, key: String, value: String): Try[Unit] = Try {
    value.replaceAll("[^\\p{L} ]", "").toLowerCase.split("\\s+")
      .filter(_.length > 0)
      .foreach(s => mapEmitter.emit(s, valueOut = "1"))
  }
}
