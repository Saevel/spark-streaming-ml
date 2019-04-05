package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}
import prv.saevel.spark.streaming.ml.PredictionPipeline
import prv.saevel.spark.streaming.ml.batch.training.BatchTraining
import prv.saevel.spark.streaming.ml.model.Client

object RetrainingMain extends App {

  case class RetrainingConfig(threshold: Double, inputDataPath: String, labelledDataPath: String, modelPath: String, outputPath: String)

  // Run with -Dspark.master = <master>
  implicit val session = SparkSession.builder().appName("Retraining").getOrCreate

  import org.apache.spark.sql.functions._
  import session.implicits._

  val config = new RetrainingConfig(0.25, "build/input", "build/labelled", "build/model", "build/output")

  run(config)

  private[retraining] def run(config: RetrainingConfig)(implicit session: SparkSession): Unit = {

    val inputStream = readInput(config.inputDataPath)

    buildModel(config.labelledDataPath, config.modelPath)

    val model = readModel(config.modelPath)

    val results = model
      .transform(inputStream)
      .checkpoint(true)
      .cache

    val query = results
      .writeStream
      .outputMode(OutputMode.Append())
      .format("json")
      .start(config.outputPath)

    accuracyStream(results, config.threshold)
      .writeStream
      .foreachBatch{ (x: Dataset[_], y: Long) =>
        buildModel(config.labelledDataPath, config.modelPath)
        query.stop
        run(config)
      }.start
  }

  private[retraining] def readInput(streamingDataPath: String)(implicit session: SparkSession): Dataset[_] =
    session
      .readStream
      .format("json")
      .load(streamingDataPath)
      .as[Client]
      .cache

  // TODO: MODIFY
  private[retraining] def buildModel(labelledDataPath: String, modelPath: String)(implicit session: SparkSession): Unit =
    BatchTraining(PredictionPipeline(), session.read.json(labelledDataPath), modelPath)

  private[retraining] def readModel(modelUrl: String): PipelineModel =
    PipelineModel.load(modelUrl)

  private[retraining] def accuracyStream(labelledStream: Dataset[_], threshold: Double): Dataset[_] =
    labelledStream
      .select($"time", abs($"preference" - $"preference_prediction") as "error")
      .groupBy(window($"time", "1 minute"))
      .sum("error")
      .filter($"error" >= threshold)
}