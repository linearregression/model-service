package modelservice.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import modelservice.storage.{ParameterStorage, ModelStorage}
import spray.http.{HttpEntity, HttpResponse}

//import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Turn parsed features into sparse vectors
 */
class VectorizeFeatures extends Actor with ActorLogging {
  import ModelStorage._
  import PredictActor._
  import VectorizeFeatures._
  import ParameterStorage._

  implicit val timeout: Timeout = 3.second
  import context.dispatcher

  def receive = {
    case FeaturesToVector(rec, modelKey, paramKey, modelStorage, client) => {
      val model = modelStorage ? Get(modelKey, paramKey)
      model onComplete {
        case Success(m) => {
          val mod = m.asInstanceOf[Model]
          mod match {
            case Model(Some(weights), Some(featureManager)) => {
              val featureVector = featureManager.parseRow(rec)
              val predictActor = context actorOf Props(new PredictActor)
              predictActor ! Predict(weights, featureVector, client)
            }
            case _ => client ! HttpResponse(404, entity=HttpEntity("Invalid model and / or parameter set key"))
          }
        }
        case Failure(e) => {
          log.info(e.getLocalizedMessage)
          client ! HttpResponse(500, entity=HttpEntity(e.getLocalizedMessage))
        }
      }
    }
  }
}

object VectorizeFeatures {
  case class FeaturesToVector(parsedContext: Map[String, String], modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
}
