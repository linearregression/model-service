package modelservice.prediction

import akka.actor._
import akka.testkit.TestProbe
import modelservice.core.Core
import modelservice.core.CoreTests.{DummyActor, Success}
import modelservice.core.prediction.PredictionActors
import modelservice.core.prediction.TreePredictionActor.PredictTree
import modelservice.storage.ModelBroker.{BasicFeatureManager, FeatureManagerWithKey, StoreFeatureManagerWithKey}

trait MockPredictionActors extends PredictionActors {
  this: Core =>

  val predictionActor = system actorOf Props(new DummyPredictionActor(this))
  val treePredictionNodes = system actorOf Props[DummyActor]
  val predictionTestProbe = TestProbe()
}

class DummyPredictionActor(predictionActors: MockPredictionActors) extends Actor with ActorLogging {
  def receive = {
    case PredictTree(parsedContext, Some(modelKey), Some(paramKey), modelStorage, client) => {
      log.info("DummyPredictionActor received PredictTree")
      if (modelKey == "m_key" & paramKey == "p_key" & parsedContext.nonEmpty) {
        predictionActors.predictionTestProbe.ref ! Success
      }
    }
  }
}
