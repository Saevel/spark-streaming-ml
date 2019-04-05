package prv.saevel.spark.streaming.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object PredictionPipeline {

  def apply(): Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("sex").setHandleInvalid("skip").setOutputCol("sex_indexed"),
    new StringIndexer().setInputCol("educationLevel").setHandleInvalid("skip").setOutputCol("education_indexed"),
    new StringIndexer().setInputCol("profession").setHandleInvalid("skip").setOutputCol("profession_indexed"),
    new VectorAssembler()
      .setInputCols(Array("sex_indexed", "education_indexed", "profession_indexed", "age"))
      .setHandleInvalid("skip")
      .setOutputCol("features"),
    new RandomForestClassifier()
      .setNumTrees(30)
      .setLabelCol("preference")
      .setPredictionCol("preference_prediction")
      .setFeaturesCol("features")
  ))
}
