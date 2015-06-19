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

  def receive = {
    case ParseFeatures(record: HttpEntity, modelStorage: ActorRef, client: ActorRef) =>
      try {
        val parsedContext = FeaturesToVector(parse(record.asString).values.asInstanceOf[Map[String, String]],
                                             modelStorage,
                                             client)
        log.info(parsedContext.toString)
        val vectorizeFeatures = context actorOf Props(new VectorizeFeatures)
        vectorizeFeatures ! parsedContext
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case _ => log.info("Cannot parse request")
  }
}

object FeatureParser {
  case class ParseFeatures(record: HttpEntity, modelStorage: ActorRef, client: ActorRef)
}
