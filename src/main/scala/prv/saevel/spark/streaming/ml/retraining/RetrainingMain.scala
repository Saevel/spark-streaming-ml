package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.sql.SparkSession

object RetrainingMain extends App {

  // Run with -Dspark.master = <master>
  implicit val session = SparkSession.builder().appName("Retraining").getOrCreate

  // TODO: Run pipeline!

}
