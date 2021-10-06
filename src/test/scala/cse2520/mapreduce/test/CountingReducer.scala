package cse2520.mapreduce.test

import cse2520.mapreduce.{Emitter, Reducer}

import scala.util.Try

class CountingReducer extends Reducer {

  override def reduce(reduceEmitter: Emitter, key: String, values: Iterable[String]): Try[Unit] = Try {
    reduceEmitter.emit(key, values.map(_.toInt).sum.toString)
  }
}
