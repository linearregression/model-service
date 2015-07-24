package modelservice.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.http.HttpEntity

/**
 * Actor to parse features
 */
class FeatureParser extends Actor with ActorLogging {
  import FeatureParser._
  import VectorizeFeatures._
  import TreePredictionActor._

  def receive = {
    case ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef) =>
      try {
        // Start prediction tree
        val parsedContext = PredictTree(parse(record.asString).values.asInstanceOf[Map[String, Any]],
          modelKey, paramKey, modelStorage, client)
        log.info(parsedContext.toString)
        val treePrediction = context actorOf Props(new TreePredictionActor)
        treePrediction ! parsedContext
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case _ => log.info("Cannot parse request")
  }
}

object FeatureParser {
  case class ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
}
