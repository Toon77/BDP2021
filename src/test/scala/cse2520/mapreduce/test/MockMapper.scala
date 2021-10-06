package cse2520.mapreduce.test

import cse2520.mapreduce.{Emitter, Mapper}

import scala.util.{Failure, Success, Try}

class MockMapper extends Mapper {
  override def map(mapEmitter: Emitter, key: String, value: String): Try[Unit] =
    value match {
      case v if v.startsWith("emit") => Try {
        mapEmitter.emit(value, if (value.equals("emit")) "e" else value.split("-")(1))
      }
      case "fail" => Failure(new RuntimeException("fail"))
      case _ => Success(())
    }
}
