package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.sql.{Dataset, SparkSession}

object RetrainedPipeline {

  case class RetrainedPipelineConfig(modelPath: String, threshold: Double)

  // TODO: Implement!
  def apply(trainingData: Dataset[_], actualData: Dataset[_])(implicit session: SparkSession): Dataset[_] = ???
}
