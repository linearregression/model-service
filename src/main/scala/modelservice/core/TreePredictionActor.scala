package modelservice.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import breeze.linalg.SparseVector
import modelservice.core.TreePredictionNode.{PredictionResult, NodePredict}
import modelservice.storage.ModelStorage
import modelservice.storage.ParameterStorage
import spray.http.{HttpEntity, HttpResponse}
import org.json4s._
import org.json4s.jackson.Serialization
import spray.http.{HttpEntity, HttpResponse}

//import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Turn parsed features into sparse vectors
 */
class TreePredictionActor extends Actor with ActorLogging {
  import ModelStorage._
  import PredictActor._
  import TreePredictionActor._
  import TreePredictionNode.{PredictionResult, NodePredict}
  import ParameterStorage._

  implicit val timeout: Timeout = 5.second
  import context.dispatcher

  implicit val formats = DefaultFormats

  def freeVarList(freeVars: Any): List[Map[String, Any]] = {
    freeVars match {
      case r: Map[Any, Any] => List[Map[String, Any]](r.asInstanceOf[Map[String, Any]])
      case r: List[Map[Any, Any]] => r.asInstanceOf[List[Map[String, Any]]]
    }
  }

  def toBoundVars(varMap: Map[String, Any]): Map[String, String] = {
    varMap.flatMap(r =>
      r._2 match {
        case s: String => Some(r.asInstanceOf[(String, String)])
        case _ => None
      }
    )
  }

  def toFreeVars(varMap: Map[String, Any]): Map[String, Any] = {
    varMap.flatMap(r =>
      r._2 match {
        case s: Map[String, Any] => Some(r)
        case s: List[String] => Some(r)
        case _ => None
      }
    )
  }

  def receive = {
    case PredictTree(rec, modelKey, paramKey, modelStorage, client) => {
      val model = modelStorage ? Get(modelKey, paramKey)
      val boundVariables = toBoundVars(rec)
      val freeVariables = toFreeVars(rec)

      model onComplete {
        case Success(m) => {
          val mod = m.asInstanceOf[Model]
          mod match {
            case Model(Some(weights), Some(featureManager)) => {

              val validModel = ValidModel(weights, featureManager)

              val predictionFuture = (context actorOf Props(new TreePredictionNode)) ?
                NodePredict(freeVariables, boundVariables, Map[String, String](), validModel)

              predictionFuture onSuccess {
                case results: Seq[PredictionResult] => {
                  val resultList = results.map(x =>
                    x.varMap ++ Map[String, Double]("prediction" -> x.prediction)
                  )

                  client ! HttpResponse(200, entity=HttpEntity(Serialization.write(resultList)))
                }
              }
//
//              val featureVector = featureManager.parseRow(rec)
//              val predictActor = context actorOf Props(new PredictActor)
//              predictActor ! Predict(weights, featureVector, client)
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

object TreePredictionActor {
  case class PredictTree(parsedContext: Map[String, Any], modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
  case class ValidModel(weights: SparseVector[Double], hashFeatureManager: HashFeatureManager)
}
