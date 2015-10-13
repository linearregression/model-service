package modelservice.core

import modelservice.storage.MockStorageActors

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.actor.{Props, ActorSystem}
import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import spray.http.HttpEntity
import modelservice.core.prediction.TreePredictionNode
import modelservice.prediction.MockPredictionActors

/**
 * Test the core actors
 */
class CoreActorsSpec extends TestKit(ActorSystem("CoreTestActorSystem")) with DefaultTimeout with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll
with Core with MockPredictionActors with MockStorageActors with CoreActorSet {
  import CoreTests._
  import TreePredictionNode._

  // Shut down the test actor system upon completion of tests
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A ModelParser" should {
    "Parse a model" in {
      val dummyActor1 = system actorOf Props[DummyActor]
      val dummyActor2 = system actorOf Props[DummyActor]
      val mReq = ModelParser.ParseModelAndStore(HttpEntity(testModelJSON), Some("m_key"), dummyActor1, dummyActor2)
      modelParser ! mReq
      storageTestProbe.expectMsg(500 millis, Success)
    }
  }

  "A FeatureParser" should {
    "Parse a feature vector" in {
      val dummyActor1 = system actorOf Props[DummyActor]
      val dummyActor2 = system actorOf Props[DummyActor]
      val fReq = FeatureParser.ParseFeatures(HttpEntity(testFeatureSetJSON), Some("m_key"), Some("p_key"), dummyActor1, dummyActor2)
      parseActor ! fReq
      predictionTestProbe.expectMsg(500 millis, Success)
    }
  }

  "A TreePredictionNode" should {
    "Cross product child node flattened feature sets" in {
      crossProduct(combineChildNodes)(childNodesTestPropertiesFlattened) shouldEqual
        childNodesTestPropertiesFlattenedCrossProductExpectedResult
    }

    "Predict a logistic function" in {
      val logistic_1 = logisticFunction(1)
      val logistic_neg_1 = logisticFunction(-1)

      logisticFunction(0) shouldEqual 0.5
      math.abs(0.7310585786 - logistic_1) < math.pow(10, -8) shouldBe true
      math.abs(0.2689414214 - logistic_neg_1) < math.pow(10, -8) shouldBe true
    }
  }
}