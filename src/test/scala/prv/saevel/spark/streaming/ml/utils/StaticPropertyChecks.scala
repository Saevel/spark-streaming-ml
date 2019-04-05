package prv.saevel.spark.streaming.ml.utils

import org.scalacheck.Gen
import org.scalatest.Suite

/**
  * A util for testing
  */
trait StaticPropertyChecks { self: Suite =>

  protected val maxRetries = 10

  def forOneOf[X, Y](generator: Gen[X])(f: X => Unit): Unit = {
    var result: Option[X] = None
    var i = 0

    while(i < maxRetries && result == None) {
      result = generator.sample
      i += 1
    }

    result match {
      case Some(x) => f(x)
      case None => fail(s"Failed to generate data in $maxRetries retries.")
    }
  }
}
