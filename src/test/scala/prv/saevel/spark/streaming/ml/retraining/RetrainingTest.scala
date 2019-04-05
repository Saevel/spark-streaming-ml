package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.retraining.Retraining.RetrainingConfig
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, Spark, StaticPropertyChecks, StreamGenerators}

@RunWith(classOf[JUnitRunner])
class RetrainingTest extends WordSpec with Matchers with StaticPropertyChecks with ScenariosGenerators
  with StreamGenerators with Spark {

  private val modelPath = "build/retrainedModel"

  "Retraining" when {

    "accuracy drops below the given threshold" should {

      "retrain and overwrite model" in forOneOf(allTypesOfClients(100)){ trainingData =>
        forOneOf(allTypesOfClients(100)){ testData =>
          withSparkSession("RetrainingTest"){ implicit session =>

            import session.implicits._

            // Generate stream of test client data.
            withStreamFrom(testData){ testStream =>

              // Batch data for training
              val trainingDataset = trainingData.toDS()

              val config = RetrainingConfig(0.35, modelPath, "predictions", 10, 6000)
              val query = Retraining.run(() => trainingDataset, testStream, config)

              // Letting the stream run for a while.
              Thread.sleep(3000 * 10)
              query.stop()

              // Evaluation of accuracy on all streamed data.
              val finalAccuracy = new MulticlassClassificationEvaluator()
                .setLabelCol("preference")
                .setPredictionCol("preference_prediction")
                .evaluate(session.read.table(config.outputTable))

              // NOTE: That value is pretty low, but it's lower that it ever gets and the patterns in the data are only
              // statistically true. You can do much better if you change my "forOneOf" method to standard ScalaCheck
              // "forAll" and averaging accuracy over many runs, but the tests will run way longer.
              finalAccuracy should be >= (0.55)
            }
          }
        }
      }
    }
  }

}
