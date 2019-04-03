package prv.saevel.spark.streaming.ml

import org.apache.spark.sql.{Dataset, SparkSession}

object Accuracy {

  def apply(data: Dataset[_])(accuracy: Double)(implicit session: SparkSession): Double = {
    import session.implicits._
    (data
      .filter(($"preference".as[Double] - $"preference_prediction".as[Double] <= accuracy))
      .count).toDouble / (data.count.toDouble)
  }
}
