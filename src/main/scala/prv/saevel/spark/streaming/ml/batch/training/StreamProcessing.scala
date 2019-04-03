package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.{Dataset, SparkSession}

object StreamProcessing {

  def apply(streamingData: Dataset[_])(modelUrl: String)(implicit session: SparkSession): Dataset[_] =
    RandomForestClassificationModel
      .load(modelUrl)
      .transform(streamingData)
}
