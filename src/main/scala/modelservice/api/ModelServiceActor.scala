package modelservice.api

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.util.Timeout
//import modelservice.core.ModelParser.ParseParametersAndStore
import modelservice.core.{FeatureParser, ModelParser}
import modelservice.storage.ModelStorage
import spray.can.Http
import spray.http.HttpMethods._
import spray.http._
import scala.concurrent.duration._

/**
 * REST actor to pass requests, initialize and maintain storage
 */
class ModelServiceActor extends Actor with ActorLogging {
  import ModelParser._
  import FeatureParser._

  var modelStorage: Option[ActorRef] = None

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3,
    withinTimeRange = 5 seconds) {
    case _: ModelStorage.StorageException => Restart
  }

  implicit val timeout: Timeout = 5.second

  def initModelStorage(): Unit = {
    modelStorage = Some(context watch(context actorOf(Props[ModelStorage], name="ModelStorage")))
  }

  override def preStart(): Unit = {
    initModelStorage()
  }

  def receive = {
    case _: Http.Connected => {
      sender ! Http.Register(self)
      log.info("attached")
    }

    case HttpRequest(POST, Uri.Path(putPath), _, entity, _) if putPath.startsWith("/predict") => {
      val splitPath = putPath.split("/")
      val modelKey = try {
        Some(splitPath(2))
      } catch {
        case e: Exception => None
      }
      val paramKey = try {
        Some(splitPath(3))
      } catch {
        case e: Exception => None
      }

      modelStorage match {
        case Some(modelStorageActor) => {
          val parseActor = context actorOf Props(new FeatureParser())
          parseActor ! ParseFeatures(entity, modelKey, paramKey, modelStorageActor, sender)
        }
        case None => sender ! HttpResponse(entity = "Model storage not yet initialized")
      }
    }

    case HttpRequest(POST, Uri.Path("/models"), _, entity, _) =>
      val parseActor = context actorOf Props(new ModelParser())
      modelStorage match {
        case Some(modelStorageActor) => parseActor ! ParseModelAndStore(entity, None, modelStorageActor, sender)
        case None => sender ! HttpResponse(entity="Model storage not yet initialized")
      }

    case HttpRequest(PUT, Uri.Path(putPath), _, entity, _) if putPath.startsWith("/models") => {
      val modelKey = try {
        Some(putPath.split("/")(2))
      } catch {
        case e: Exception => None
      }
      modelKey match {
        case Some(key) => {
          val parseActor = context actorOf Props(new ModelParser())
          modelStorage match {
            case Some(modelStorageActor) => {
              val paramKey = try {
                Some(putPath.split("/")(3))
              } catch {
                case e: Exception => None
              }
              paramKey match {
                case Some(pKey) => parseActor ! ParseParametersAndStore(entity, key, Some(pKey), modelStorageActor, sender)
                case None => parseActor ! ParseModelAndStore(entity, Some(key), modelStorageActor, sender)
              }
            }
            case None => sender ! HttpResponse(entity = "Model storage not yet initialized")
          }
        }
        case None => {
          sender ! HttpResponse(entity = s"Could not parse PUT request with path: $putPath")
        }
      }
    }
  }
}
