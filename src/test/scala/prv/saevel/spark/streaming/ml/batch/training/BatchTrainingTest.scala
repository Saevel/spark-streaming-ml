package prv.saevel.spark.streaming.ml.batch.training

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{Column, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.utils._
import org.apache.spark.sql.functions.{isnull, not => !!}
import prv.saevel.spark.streaming.ml.pipeline.PredictionPipeline

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class BatchTrainingTest extends WordSpec with StaticPropertyChecks with Matchers with ScenariosGenerators with FileUtils
  with Spark with StreamGenerators {

  private val modelPath = "build/batchTrainingModel"

  "BatchTraining" should {

    "learn on a batch and save a model to a given location" in forOneOf(allTypesOfClients(100)){ clients =>
      without(modelPath){
        withSparkSession("BatchTrainingTest"){ implicit session: SparkSession =>
          import session.implicits._
          val trainingDataset = clients.toDS()
          BatchTraining(PredictionPipeline(), trainingDataset, modelPath)

          // Loading the model to check if it was properly saved.
          PipelineModel.read.load(modelPath) should not be(null)
        }
      }
    }
  }

  "StreamProcessing" should {

    "read the model and use it for predictions" in forOneOf(allTypesOfClients(100)){ trainingData =>
      forOneOf(allTypesOfClients(100)){ testData =>
        without(modelPath){
          withSparkSession("BatchTrainingTest") { implicit session: SparkSession =>

            import session.implicits._

            withStreamFrom(testData){ testStream =>

              val trainingDataset = trainingData.toDS()

              BatchTraining(PredictionPipeline(), trainingDataset, modelPath)

              val predictionStream =  StreamProcessing(testStream)(modelPath)

              // Stream all records exiting the pipeline to an in-memory table
              val countAllQuery = predictionStream
                .writeStream
                .format("memory")
                .queryName("all")
                .start

              //Stream records with "preference_prediction" present to an in-memory tabld
              val predictionQuery = predictionStream
                .where(!!(isnull($"preference_prediction")))
                .writeStream
                .format("memory")
                .queryName("predictions")
                .start

              // Wait for the stream to run a while
              Thread.sleep(3000 * 10)

              // Kill the streams
              countAllQuery.stop()
              predictionQuery.stop()

              // Count the contents of the in memory table
              val predictionCount = session.sql("SELECT COUNT(*) AS count FROM predictions")
                .select($"count".as[Long])
                .first

              // Count all records that existed the ML Pipeline
              val totalCount = session.sql("SELECT COUNT(*) AS count FROM all")
                .select($"count".as[Long])
                .first

              // Check if nearly all (query cancel is not transactional, so some records can slip) stream entries were labelled, duplicates included
              predictionCount should be >=(totalCount - 5)
              predictionCount should not be >(totalCount + 5)

              val finalAccuracy = new MulticlassClassificationEvaluator()
                .setLabelCol("preference")
                .setPredictionCol("preference_prediction")
                .evaluate(session.read.table("predictions"))

              // NOTE: That value is pretty low, but it's lower that it ever gets and the patterns in the data are only
              // statistically true. You can do much better if you change my "forOneOf" method to standard ScalaCheck
              // "forAll" and averaging accuracy over many runs, but the tests will run way longer.
              finalAccuracy should be >= (0.35)
            }
          }
        }
      }
    }
  }
}