package prv.saevel.spark.streaming.ml.batch.training

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.PredictionPipeline
import prv.saevel.spark.streaming.ml.utils._

import scala.util.Random

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
            import org.apache.spark.sql.functions._

            withStreamFrom(testData){ testStream =>

              val trainingDataset = trainingData.toDS()
              BatchTraining(PredictionPipeline(), trainingDataset, modelPath)

              val testStream = session
                .readStream
                .format("rate")
                .load
                .map{ _ => testData(Random.nextInt(testData.size))}

              val predictionStream = PipelineModel.load(modelPath).transform(testStream)

              val predictionQuery = predictionStream
                .where(org.apache.spark.sql.functions.not(isnull($"preference_prediction")))
                .writeStream
                .format("memory")
                .queryName("predictions")
                .start

              Thread.sleep(3000 * 10)

              predictionQuery.stop()

              val predictionCount = session.sql("SELECT COUNT(*) AS count FROM predictions")
                .select($"count".as[Long])
                .first

              predictionCount should be > 0L
            }
          }
        }
      }
    }

    "provide reasonable accuracy" in forOneOf(allTypesOfClients(100)){ client =>
      // TODO: Implement and check
    }
  }

}
