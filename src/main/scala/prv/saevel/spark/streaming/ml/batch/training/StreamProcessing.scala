package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Dataset, SparkSession}

object StreamProcessing {

  def apply(streamingData: Dataset[_])(modelUrl: String)(implicit session: SparkSession): Dataset[_] =
    PipelineModel
      .load(modelUrl)
      .transform(streamingData)
}
