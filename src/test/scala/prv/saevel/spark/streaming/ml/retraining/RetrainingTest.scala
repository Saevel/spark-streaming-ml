package prv.saevel.spark.streaming.ml.retraining

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class RetrainingTest extends WordSpec with Matchers with StaticPropertyChecks with ScenariosGenerators {

  "Retraining" when {

    "accuracy drops below the given threshold" should {

      "retrain and overwrite model" in {
        // TODO: Trend break generator!
        // TODO: Implement and check
      }
    }
  }

}
