package prv.saevel.spark.streaming.ml.retraining

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.retraining.RetrainingMain.RetrainingConfig
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, Spark, StaticPropertyChecks, StreamGenerators}

@RunWith(classOf[JUnitRunner])
class RetrainingTest extends WordSpec with Matchers with StaticPropertyChecks with ScenariosGenerators
  with StreamGenerators with Spark {

  private val modelPath = "build/retrainedModel"

  // TODO: Trend break generator!

  "Retraining" when {

    "accuracy drops below the given threshold" should {

      "retrain and overwrite model" in forOneOf(allTypesOfClients(100)){ trainingData =>
        forOneOf(allTypesOfClients(100)){ testData =>
          withSparkSession("RetrainingTest"){ implicit session =>

            import session.implicits._

            withStreamFrom(testData){ testStream =>

              val trainingDataset = trainingData.toDS()

              val config = RetrainingConfig(0.35, modelPath, "predictions", 10, 6000)

              // TODO: MODIFY TO BREAK TREND
              val query = RetrainingMain.run(() => trainingDataset, testStream, config)

              Thread.sleep(3000 * 10)

              query.stop()

              val finalAccuracy = new MulticlassClassificationEvaluator()
                .setLabelCol("preference")
                .setPredictionCol("preference_prediction")
                .evaluate(session.read.table(config.outputTable))

              finalAccuracy should be >= (0.6)
            }
          }
        }
      }
    }
  }

}
