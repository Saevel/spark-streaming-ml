package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Dataset, SparkSession}
import prv.saevel.spark.streaming.ml.pipeline.PredictionPipeline

object BatchTraining {

  def apply(pipeline: Pipeline, trainingData: Dataset[_], writeUrl: String)
           (implicit session: SparkSession): Unit =
    PredictionPipeline()
      .fit(trainingData)
      .write
      .overwrite
      .save(writeUrl)
}
