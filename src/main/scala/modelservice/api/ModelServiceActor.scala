package modelservice.api

import akka.routing.RoundRobinRouter
import modelservice.Boot
import org.json4s._
import org.json4s.jackson.Serialization
import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import spray.can.Http
import spray.http.ContentTypes._
import spray.http.HttpMethods._
import spray.http.HttpHeaders._
import spray.http._
import scala.concurrent.duration._
//import modelservice.core.ModelParser.ParseParametersAndStore
import modelservice.core.{FeatureParser, ModelParser}
import modelservice.storage.{ModelBroker, ModelStorage}

/**
 * REST actor to pass requests, initialize and maintain storage
 */
class ModelServiceActor extends Actor with ActorLogging {
  import ModelParser._
  import FeatureParser._
  import ModelStorage._
  import ModelBroker._

  var modelStorage: Option[ActorRef] = None

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3,
    withinTimeRange = 5 seconds) {
    case e: ModelStorage.StorageException => {
      log.info("Restarting storage actor: " + e.getMessage)
      Restart
    }
    case e: Exception => {
      log.info("Actor failed, stopping it: " + e.getMessage)
      Stop
    }
  }

  implicit val timeout: Timeout = 5.second
  import context.dispatcher
  implicit val formats = DefaultFormats

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

    /**
     * POST /predict
     * Accepts a JSON representation of a set of features
     * Returns predictions for the features and the given model
     */
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
//          val parseActor = context actorOf Props(new FeatureParser(Boot.treePredictionActors)).withRouter(RoundRobinRouter(nrOfInstances = 8))
          Boot.parseActor ! ParseFeatures(entity, modelKey, paramKey, modelStorageActor, sender)
        }
        case None => sender ! HttpResponse(
          entity = "Model storage not yet initialized",
          headers = List(Connection("close"))
        )
      }
    }

    /**
     * POST /models
     * Accepts a JSON representation of a model specification in the form of a feature manager
     * Returns a key for the given model
     */
    case HttpRequest(POST, Uri.Path("/models"), _, entity, _) =>
      val parseActor = context actorOf Props(new ModelParser())
      modelStorage match {
        case Some(modelStorageActor) => parseActor ! ParseModelAndStore(entity, None, modelStorageActor, sender)
        case None => sender ! HttpResponse(
          entity = "Model storage not yet initialized",
          headers = List(Connection("close"))
        )
      }

    /**
     * POST /models/<model_key>
     * Accepts a JSON representation of a model's parameters
     * Returns a key for the given set of parameters
     */
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
            case None => sender ! HttpResponse(
              entity = "Model storage not yet initialized",
              headers = List(Connection("close"))
            )
          }
        }
        case None => {
          sender ! HttpResponse(
            entity = s"Could not parse PUT request with path: $putPath",
            headers = List(Connection("close"))
          )
        }
      }
    }

    /**
     * GET /models
     * Returns a list of currently stored models
     */
    case HttpRequest(GET, Uri.Path("/models"), _, entity, _) =>
      val modelBroker = ModelBroker.createActor(context)
      modelStorage match {
        case Some(modelStorageActor) => modelBroker ! GetAllKeysInStorage(modelStorageActor, sender)
        case None => sender ! HttpResponse(
          entity = "Model storage not yet initialized",
          headers = List(Connection("close"))
        )
      }

    /**
     * GET /server-stats
     * Returns statistics for the HTTP server
     */
    case HttpRequest(GET, Uri.Path("/server-stats"), _, _, _) =>
      val client = sender
//      context.parent ? Http.GetStats onSuccess {
      context.actorSelection("/user/IO-HTTP/listener-0") ? Http.GetStats onSuccess {
        case x: spray.can.server.Stats => client ! HttpResponse(
          entity = HttpEntity(
            `application/json`,
            Serialization.write(x)
          ),
          headers = List(Connection("close"))
        )
      }
  }
}
