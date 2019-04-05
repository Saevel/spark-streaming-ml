package prv.saevel.spark.streaming.ml.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

import scala.util.{Failure, Success, Try}

trait Spark { self: Suite =>

  protected def withSparkSession[T](appName: String)(f: SparkSession => T): T ={
    Try {
      SparkSession.builder().appName(appName).master("local[*]").getOrCreate()
    } match {
      case Success(session) => f(session)
      case Failure(e) => fail("Failed to create SparkSession", e)
    }
  }
}
