import modelservice.core.prediction.TreePredictionActor.PredictTree

import scala.concurrent.duration._

import akka.actor._
import modelservice.storage.ModelBroker.{BasicFeatureManager, FeatureManagerWithKey, StoreFeatureManagerWithKey}
import modelservice.core.prediction.TreePredictionNode
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit, TestActorRef, TestProbe}
import spray.http.{HttpData, HttpEntity}


/**
 * Test the core actors
 */
class CoreTests extends TestKit(ActorSystem("CoreTestActorSystem")) with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  import modelservice.core.{FeatureParser, ModelParser}
  import CoreTests._
  import TreePredictionNode._

  // Shut down the test actor system upon completion of tests
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val modelBrokerProbe1 = TestProbe()
  val predictionProbe1 = TestProbe()

  val modelParser = system actorOf Props(classOf[ModelParser], DummyModelBrokerActor.createActor(modelBrokerProbe1.ref))
  val featureParser = system actorOf Props(classOf[FeatureParser], DummyPredictionActor.createActor(predictionProbe1.ref))


  "A ModelParser" should {
    "Parse a model" in {
      val dummyActor1 = system actorOf Props[DummyActor]
      val dummyActor2 = system actorOf Props[DummyActor]
      val mReq = ModelParser.ParseModelAndStore(HttpEntity(testModelJSON), Some("m_key"), dummyActor1, dummyActor2)
      modelParser ! mReq
      modelBrokerProbe1.expectMsg(500 millis, Success)
    }
  }

  "A FeatureParser" should {
    "Parse a feature vector" in {
      val dummyActor1 = system actorOf Props[DummyActor]
      val dummyActor2 = system actorOf Props[DummyActor]
      val fReq = FeatureParser.ParseFeatures(HttpEntity(testFeatureSetJSON), Some("m_key"), Some("p_key"), dummyActor1, dummyActor2)
      featureParser ! fReq
      predictionProbe1.expectMsg(500 millis, Success)
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

object CoreTests {
  import TreePredictionNode._

  case class Success()

  val testFeatureSetJSON =
    """
      |{
      |  "publisher_id": "Test Inc",
      |  "application_id": "GPS Dog Walker",
      |  "advertiser_id": "Koala Cola",
      |  "location_city": "San Francisco",
      |  "location_region": "California",
      |  "location_country": "United State of America",
      |  "company": {
      |    "Pfizer": {
      |      "campaign_id": {
      |        "drug1": {
      |          "ad_group": {
      |            "video_w_currency": {
      |              "asset_id": [
      |                "asset123",
      |                "asset456"
      |              ]
      |            }
      |          }
      |        }
      |      }
      |    },
      |    "Ford": {
      |      "campaign_id": {
      |        "Fusion": {
      |          "ad_group": {
      |            "impression_based": {
      |              "asset_id": [
      |                "asset10000",
      |                "asset10003",
      |                "asset10051"
      |              ]
      |            },
      |            "app_install": {
      |              "asset_id": [
      |                "asset2000011",
      |                "asset2020100",
      |                "asset2200000"
      |              ]
      |            }
      |          }
      |        },
      |        "Focus": {
      |          "ad_group": {
      |            "social_media_123": {
      |              "asset_id": [
      |                "asset3000",
      |                "asset3001"
      |              ]
      |            }
      |          }
      |        }
      |      }
      |    }
      |  },
      |  "page_position": [
      |    "top",
      |    "bottom",
      |    "left",
      |    "right"
      |  ],
      |  "bg_color": [
      |    "red",
      |    "blue"
      |  ]
      |}
    """.stripMargin


  val testFeatureSetMap = Map[String, Any](
    "application_id" -> "GPS Dog Walker",
    "location_city" -> "San Francisco",
    "location_region" -> "California",
    "location_country" -> "United States of America",
    "publisher_id" -> "Test Inc",
    "advertiser_id" -> "Koala Cola",
    "company" -> Map[String, Any](
      "Ford" -> Map[String, Any](
        "campaign_id" -> Map[String, Any](
          "Fusion" -> Map[String, Any](
            "ad_group" -> Map[String, Any](
              "impression_based" -> Map[String, Any](
                "asset_id" -> List[String](
                  "asset10000",
                  "asset10003",
                  "asset10051"
                )
              ),
              "app_install" -> Map[String, Any](
                "asset_id" -> List[String](
                  "asset2000011",
                  "asset2020100",
                  "asset2200000"
                )
              )
            )
          ),
          "Focus" -> Map[String, Any](
            "ad_group" -> Map[String, Any](
              "social_media_123" -> Map[String, Any](
                "asset_id" -> List[String](
                  "asset3000",
                  "asset3001"
                )
              )
            )
          )
        )
      ),
      "Pfizer" -> Map[String, Any](
        "campaign_id" -> Map[String, Any](
          "drug1" -> Map[String, Any](
            "ad_group" -> Map[String, Any](
              "video_w_currency" -> Map[String, Any](
                "asset_id" -> List[String](
                  "asset123",
                  "asset456"
                )
              )
            )
          )
        )
      )
    ),
    "bg_color" ->  List[String](
      "red",
      "blue"
    ),
    "page_position" -> List[String](
      "top",
      "bottom",
      "left",
      "right"
    )
  )

  val testModelJSON =
    """
      |{
      |  "k": 21,
      |  "label": "target",
      |  "single_features": [
      |    "application_id",
      |    "campaign_id",
      |    "connection_type",
      |    "ad_group",
      |    "location_country",
      |    "location_region",
      |    "location_city",
      |    "impression_slot"
      |  ],
      |  "quadratic_features": [
      |    [
      |      [
      |        "application_id"
      |      ],
      |      [
      |        "campaign_id",
      |        "ad_group",
      |        "connection_type",
      |        "location_country"
      |      ]
      |    ],
      |    [
      |      [
      |        "campaign_id"
      |      ],
      |      [
      |        "location_country",
      |        "impression_slot"
      |      ]
      |    ]
      |  ]
      |}
    """.stripMargin

  val childNodesTestPropertiesFlattened = Seq(
    Seq(
      ChildNodeMapKV(Map[String, String]("color" -> "red"), Map[String, Any](), Map[String, Any]()),
      ChildNodeMapKV(Map[String, String]("color" -> "blue"), Map[String, Any](), Map[String, Any]())
    ),
    Seq(
      ChildNodeMapKV(Map[String, String]("shape" -> "round"), Map[String, Any](), Map[String, Any]()),
      ChildNodeMapKV(Map[String, String]("shape" -> "square"), Map[String, Any](), Map[String, Any]())
    ),
    Seq(
      ChildNodeMapKV(
        Map[String, String]("publisher_id" -> "abc123"),
        Map[String, Any]("property_id" -> Map[String, Any]("property_1" -> List("a", "b", "c"))),
        Map[String, Any]()
      )
    )
  )

  val childNodesTestPropertiesFlattenedCrossProductExpectedResult = List(
    ChildNodeMapKV(
      Map[String, String]("color" -> "red", "shape" -> "round", "publisher_id" -> "abc123"),
      Map[String, Any]("property_id" -> Map[String, Any]("property_1" -> List("a", "b", "c"))),
      Map[String, Any]()
    ),
    ChildNodeMapKV(
      Map[String, String]("color" -> "red", "shape" -> "square", "publisher_id" -> "abc123"),
      Map[String, Any]("property_id" -> Map[String, Any]("property_1" -> List("a", "b", "c"))),
      Map[String, Any]()
    ),
    ChildNodeMapKV(
      Map[String, String]("color" -> "blue", "shape" -> "round", "publisher_id" -> "abc123"),
      Map[String, Any]("property_id" -> Map[String, Any]("property_1" -> List("a", "b", "c"))),
      Map[String, Any]()
    ),
    ChildNodeMapKV(
      Map[String, String]("color" -> "blue", "shape" -> "square", "publisher_id" -> "abc123"),
      Map[String, Any]("property_id" -> Map[String, Any]("property_1" -> List("a", "b", "c"))),
      Map[String, Any]()
    )
  )
}

class DummyActor extends Actor {
  def receive = {
    case () => {}
  }
}

class DummyModelBrokerActor(nextActor: ActorRef) extends Actor with ActorLogging {
  def receive = {
    case StoreFeatureManagerWithKey(FeatureManagerWithKey(key: String, basicModel: BasicFeatureManager),
    modelStorage: ActorRef, client: ActorRef) => {
      log.info("DummyModelBrokerActor received StoreFeatureManagerWithKey")
      if (key == "m_key" & basicModel.label == "target" & basicModel.quads.nonEmpty) {
        nextActor ! CoreTests.Success
      }
    }
  }
}

object DummyModelBrokerActor {
  def createActor(nextActor: ActorRef) = (actorRefFactory: ActorRefFactory) => {
    actorRefFactory actorOf Props(classOf[DummyModelBrokerActor], nextActor)
  }
}

class DummyPredictionActor(nextActor: ActorRef) extends Actor with ActorLogging {
  def receive = {
    case PredictTree(parsedContext, Some(modelKey), Some(paramKey), modelStorage, client) => {
      log.info("DummyPredictionActor received PredictTree")
      if (modelKey == "m_key" & paramKey == "p_key" & parsedContext.nonEmpty) {
        nextActor ! CoreTests.Success
      }
    }
  }
}

object DummyPredictionActor {
  def createActor(nextActor: ActorRef) = (actorRefFactory: ActorRefFactory) => {
    actorRefFactory actorOf Props(classOf[DummyPredictionActor], nextActor)
  }
}