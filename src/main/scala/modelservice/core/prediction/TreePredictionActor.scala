package modelservice.core.prediction

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import spray.http.{HttpEntity, HttpResponse}
import org.json4s._
import org.json4s.jackson.Serialization
import breeze.linalg.SparseVector

import modelservice.core.HashFeatureManager
import modelservice.storage.{ModelStorage, ParameterStorage}

/**
 * Root for prediction on free variable trees
 *
 * Retrieves the appropriate model from storage and kicks off tree prediction
 */
class TreePredictionActor extends Actor with ActorLogging {
  import ModelStorage._
  import ParameterStorage._
  import TreePredictionActor._
  import TreePredictionNode.{NodePredict, PredictionResult}

  implicit val timeout: Timeout = 1.second
  import context.dispatcher

  implicit val formats = DefaultFormats

  def freeVarList(freeVars: Any): List[Map[String, Any]] = {
    freeVars match {
      case r: Map[Any, Any] => List[Map[String, Any]](r.asInstanceOf[Map[String, Any]])
      case r: List[Map[Any, Any]] => r.asInstanceOf[List[Map[String, Any]]]
    }
  }

  /**
   * Filter variable map for bound variables (ie variables that factor into the prediction but cannot be changed)
   * @param varMap variable map
   * @return map of bound variables
   */
  def toBoundVars(varMap: Map[String, Any]): Map[String, Any] = {
    varMap.flatMap(r =>
      r._2 match {
        case s: String => Some(r.asInstanceOf[(String, String)])
        case d: Double => Some(r.asInstanceOf[(String, Double)])
        case _ => None
      }
    )
  }

  /**
   * Filter variable map for free variables (arguments to be chosen to optimize the objective function)
   * @param varMap variable map
   * @return map of free variables
   */
  def toFreeVars(varMap: Map[String, Any]): Map[String, Any] = {
    varMap.flatMap(r =>
      r._2 match {
        case s: Map[String, Any] if s.size > 0 => Some(r)
        case s: List[String] if s.size > 0 => Some(r)
        case _ => None
      }
    )
  }

  def receive = {
    case PredictTree(rec, modelKey, paramKey, modelStorage, client) => {
      // Retrieve the model from model storage
      val model = modelStorage ? Get(modelKey, paramKey)

      // Parse the free and bound variables from the parsed features
      val boundVariables = toBoundVars(rec)
      val freeVariables = toFreeVars(rec)

      model onComplete {
        case Success(m) => {
          val mod = m.asInstanceOf[Model]
          mod match {
            case Model(Some(weights), Some(featureManager)) => {

              val validModel = ValidModel(weights, featureManager)

              // Launch tree prediction
              val predictionFuture = (context actorOf Props(new TreePredictionNode)) ?
                NodePredict(freeVariables, boundVariables, Map[String, String](), validModel)

              // Collect predictions and serve to client
              predictionFuture onSuccess {
                case results: Seq[PredictionResult] => {
                  val resultList = results.map(x =>
                    x.varMap ++ Map[String, Double]("prediction" -> x.prediction)
                  )

                  client ! HttpResponse(200, entity=HttpEntity(Serialization.write(resultList)))
                }
              }
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
  case class PredictTree(parsedContext: Map[String, Any], modelKey: Option[String], paramKey: Option[String],
                         modelStorage: ActorRef, client: ActorRef)
  case class ValidModel(weights: SparseVector[Double], hashFeatureManager: HashFeatureManager)

  def createActor(actorRefFactory: ActorRefFactory): ActorRef = {
    actorRefFactory actorOf Props(classOf[TreePredictionActor])
  }
}
