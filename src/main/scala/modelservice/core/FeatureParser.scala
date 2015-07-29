package modelservice.core

import akka.actor._
import modelservice.core.prediction.{Prediction, TreePredictionActor}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.http.HttpEntity

/**
 * Actor to parse features
 * @param createPredictionActor Create actor using current the context.  This should be the prediction actor in
 *                              production or a probe actor for tests.  Defaults to tree prediction
 */
class FeatureParser(createPredictionActor: (ActorRefFactory) => ActorRef = TreePredictionActor.createActor)
  extends Actor with ActorLogging {
  import FeatureParser._
//  import VectorizeFeatures._
  import TreePredictionActor._

  def receive = {
    case ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef) =>
      try {
        // Start prediction tree
        val parsedContext = PredictTree(parse(record.asString).values.asInstanceOf[Map[String, Any]],
          modelKey, paramKey, modelStorage, client)
        val predictionActor = createPredictionActor(context)
        predictionActor ! parsedContext
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case _ => log.info("Cannot parse request")
  }
}

object FeatureParser {
  case class ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
}
