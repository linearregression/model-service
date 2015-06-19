package modelservice.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import modelservice.storage.ModelStorage

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Turn parsed features into sparse vectors
 */
class VectorizeFeatures extends Actor with ActorLogging {
  import ModelStorage._
  import PredictActor._
  import VectorizeFeatures._

  implicit val timeout: Timeout = 3.second
  import context.dispatcher

  def receive = {
    case FeaturesToVector(rec, modelStorage, client) => {
      val model = modelStorage ? GetLatest()
      model onComplete {
        case Success(m) => {
          val mod = m.asInstanceOf[Model]
          val weights = mod.weights
          val featureManager = mod.featureManager
          val featureVector = featureManager.parseRow(rec)
          val predictActor = context actorOf Props(new PredictActor)
          predictActor ! Predict(weights, featureVector, client)
        }
        case Failure(e) => log.info(e.getLocalizedMessage)
      }
    }
  }
}

object VectorizeFeatures {
  case class FeaturesToVector(parsedContext: Map[String, String], modelStorage: ActorRef, client: ActorRef)
}
