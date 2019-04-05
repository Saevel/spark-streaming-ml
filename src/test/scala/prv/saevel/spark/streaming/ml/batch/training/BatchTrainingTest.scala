package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.PredictionPipeline
import prv.saevel.spark.streaming.ml.utils._

import org.apache.spark.sql.functions.{not => !!, isnull}

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

              // Check if nearly all (query cancel is not transactional, so some records can slip) stream entries were labelled
              predictionCount should be >=(totalCount - 5)
              predictionCount should not be >(totalCount)

              val accuracy = session.sql("SELECT * FROM predictions")
            }
          }
        }
      }
    }
  }
}