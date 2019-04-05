package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, SparkSession}
import prv.saevel.spark.streaming.ml.PredictionPipeline
import prv.saevel.spark.streaming.ml.batch.training.{BatchTraining, StreamProcessing}

object RetrainingMain extends App {

  case class RetrainingConfig(threshold: Double, modelPath: String, outputTable: String, accuracyWindow: Int, evaluationDelayMs: Long)

  // Run with -Dspark.master = <master>
  implicit val session = SparkSession.builder().appName("Retraining").getOrCreate

  import org.apache.spark.sql.functions._
  import session.implicits._

  // val config = new RetrainingConfig(0.25, "build/model", "build/output")

  // run(config)

  private[retraining] def run(trainingDataProducer: () => Dataset[_],
                              actualData: Dataset[_],
                              config: RetrainingConfig)(implicit session: SparkSession): StreamingQuery = {


    BatchTraining(PredictionPipeline(), trainingDataProducer(), config.modelPath)

    val query = StreamProcessing(actualData)(config.modelPath)
      .writeStream
      .outputMode(OutputMode.Append())
      .format("memory")
      .queryName(config.outputTable)
      .start

    Thread.sleep(config.evaluationDelayMs)

    val baseErrors = session
      .read
      .table(config.outputTable)
      .limit(config.accuracyWindow)
      .cache()

    val accuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("preference")
      .setPredictionCol("preference_prediction")
      .evaluate(baseErrors)

    if(accuracy < config.threshold){
      query.stop
      run(trainingDataProducer, actualData, config)
    } else {
      query
    }
  }

  private[retraining] def accuracyStream(labelledStream: Dataset[_], threshold: Double): Dataset[_] =
    labelledStream
      .select($"time", abs($"preference" - $"preference_prediction") as "error")
      .groupBy(window($"time", "1 minute"))
      .sum("error")
      .filter($"error" >= threshold)
}