package prv.saevel.spark.streaming.ml.embedding

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.{Dataset, SparkSession}

object EmbeddedSuggestions {

  def apply(trainingData: Dataset[_], actualData: Dataset[_])(implicit session: SparkSession): Dataset[_] =
     new RandomForestClassifier()
       .setLabelCol("preference")
       .setPredictionCol("preference_prediction")
       .setFeaturesCol("features")
       .setNumTrees(10)
       .fit(trainingData)
       .transform(actualData)
}
