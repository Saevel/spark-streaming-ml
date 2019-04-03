package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.{Dataset, SparkSession}

object BatchTraining {

  def apply(trainingData: Dataset[_])(writeUrl: String)(implicit session: SparkSession): Unit =
    new RandomForestClassifier()
      .setLabelCol("preference")
      .setPredictionCol("preference_prediction")
      .setFeaturesCol("features")
      .setNumTrees(10)
      .write
      .overwrite()
      .save(writeUrl)
}
