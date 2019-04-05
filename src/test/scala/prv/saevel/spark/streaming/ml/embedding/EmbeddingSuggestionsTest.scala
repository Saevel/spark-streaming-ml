package prv.saevel.spark.streaming.ml.embedding

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.spark.streaming.ml.utils.{ScenariosGenerators, Spark, StaticPropertyChecks, StreamGenerators}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import prv.saevel.spark.streaming.ml.PredictionPipeline

@RunWith(classOf[JUnitRunner])
class EmbeddingSuggestionsTest extends WordSpec with Matchers with StaticPropertyChecks with ScenariosGenerators
  with StreamGenerators with Spark{

  "EmbeddingSugestions" when {

    "set up to train and run on a stream at the same time" should {

      "fail to run because of no output operation on the training " in {
        forOneOf(allTypesOfClients(100)){ trainingClients =>
          forOneOf(allTypesOfClients(100)){ testClients =>
            withSparkSession("EmbeddingSuggestionsTest"){ implicit session =>

              import session.implicits._

              withStreamFrom(trainingClients){ trainingStream =>
                withStreamFrom(testClients){ testStream =>

                  val query = PredictionPipeline()
                    .fit(trainingStream)
                    .transform(testStream)
                    .writeStream
                    .outputMode(OutputMode.Append())
                    .format("memory")
                    .start

                  Thread.sleep(3000)

                  query.stop();
                }
              }
            }
          }
        }
      }
    }
  }
}