package prv.saevel.spark.streaming.ml.utils

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.util.Random

trait StreamGenerators {

  // protected val sampleFraction: Double = 0.1

  protected def withStreamFrom[X, Y](elements: Seq[X])(f: Dataset[X] => Y)
                                    (implicit session: SparkSession, encoder: Encoder[X]): Y = {
    f(
      session
      .readStream
      .format("rate")
      .load
      .map(_ => elements(Random.nextInt(elements.size)))
    )
  }

  /*
  protected def randomElement[X](elements: Seq[X]): X = {
    val i = Random.nextInt(elements.length)
    elements(i)
  }
  */
}
