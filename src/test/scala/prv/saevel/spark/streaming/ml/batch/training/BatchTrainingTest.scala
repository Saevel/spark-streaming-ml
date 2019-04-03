package prv.saevel.spark.streaming.ml.batch.training

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class BatchTrainingTest extends WordSpec with StaticPropertyChecks with Matchers with ScenariosGenerators {

  "BatchTraining" should {

    "learn on a batch and save a model to a given location" in forOneOf(allTypesOfClients(100)){ clients =>
      // TODO: Implement and check!
    }
  }

  "StreamProcessing" should {

    "read the model and use it for predictions" in forOneOf(allTypesOfClients(100)){ clients =>
      // TODO: Implement and check!
    }

    "provide reasonable accuracy" in forOneOf(allTypesOfClients(100)){ client =>
      // TODO: Implement and check
    }
  }

}
