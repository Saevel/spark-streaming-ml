package prv.saevel.spark.streaming.ml.embedding

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class EmbeddingSuggestionsTest extends WordSpec with Matchers with StaticPropertyChecks with ScenariosGenerators {

  "EmbeddingSugestions" when {

    "given a stream of mixed records" should {

      "still have fairly low accuracy" in forOneOf(allTypesOfClients(100)){ clients =>

      }
    }
  }
}
