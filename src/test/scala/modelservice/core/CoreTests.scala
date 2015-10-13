package modelservice.core

import akka.actor._
import modelservice.storage.ModelBroker.{BasicFeatureManager, FeatureManagerWithKey, StoreFeatureManagerWithKey}
import modelservice.core.prediction.{PredictionActors, TreePredictionNode}


/**
 * Support for core tests
 */
object CoreTests {
  import TreePredictionNode._

  case class Success()

  class DummyActor extends Actor {
    def receive = {
      case () => {}
    }
  }

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