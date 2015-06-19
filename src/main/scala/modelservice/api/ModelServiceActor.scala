package modelservice.api

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.util.Timeout
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

    case HttpRequest(POST, Uri.Path("/predict"), _, entity, _) =>
      val parseActor = context actorOf Props(new FeatureParser())
      modelStorage match {
        case Some(modelStorageActor) => parseActor ! ParseFeatures(entity, modelStorageActor, sender)
        case None => sender ! HttpResponse(entity="Model storage not yet initialized")
      }

    case HttpRequest(POST, Uri.Path("/models"), _, entity, _) =>
      val parseActor = context actorOf Props(new ModelParser())
      modelStorage match {
        case Some(modelStorageActor) => parseActor ! ParsedModelAndStore(entity, modelStorageActor, sender)
        case None => sender ! HttpResponse(entity="Model storage not yet initialized")
      }
  }
}

//object ModelServiceActor {
//  case class Entity
//}

// curl -v -X POST http://127.0.0.1:8080/predict -d "{ \"property\" : \"value\" }"