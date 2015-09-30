package modelservice.storage

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.SupervisorStrategy.Restart
import akka.util.Timeout
import akka.pattern.ask
import akka.pattern.pipe
import org.joda.time.DateTime
import akka.actor._
import akka.event.LoggingReceive
import modelservice.core.HashFeatureManager
import spray.http.{HttpEntity, HttpResponse}
import spray.http.HttpHeaders._
import spray.http.ContentTypes._

/**
 * Stores and retrieves models
 */
class ModelStorage extends Actor with ActorLogging {
  import ModelStorage._
  import ParameterStorage._

  val models = new ModelVault

  var parameterStorage: Option[Map[String, ActorRef]] = None

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3,
    withinTimeRange = 5.seconds) {
    case _: ParameterStorage.StorageException => Restart
  }

  implicit val timeout: Timeout = 5.second
  import context.dispatcher

  def initParamStorage() = {
    parameterStorage = Some(Map[String, ActorRef]())
//    context watch(context actorOf(Props[ParameterStorage], name="ParameterStorage"))
  }

  override def preStart() = {
    initParamStorage()
    log.info("Storage initialized")
  }

  def parameterStorageFactory(key: String, featureManager: HashFeatureManager): ActorRef = {
    context watch(
      context actorOf(
        Props(classOf[ParameterStorage], featureManager), name=s"ParameterStorage_$key"))
  }

//  def getParams(paramStorageActor: Option[ActorRef], paramVersion: Option[String],
//                sender: ActorRef) = {
//    paramStorageActor match {
//      case Some(pActor) => {
//        paramVersion match {
//          case Some(pVersion) => {
//            pActor ? GetParams(pVersion) onComplete {// TODO: add onFailure
//              case Success(p) => sender ! p.asInstanceOf[ParameterStorage.Model]
//            }
//          }
//          case None => {
//            pActor ? GetLatestParams() onComplete {// TODO: add onFailure
//              case Success(p) => sender ! p.asInstanceOf[ParameterStorage.Model]
//            }
//          }
//        }
//      }
//      case None => ParameterStorage.Model(None, None)
//    }
//  }

  def postFM(key: String, featureManager: HashFeatureManager, client: ActorRef): Unit = {
    this.models.get(key) match {
      case None => {
        models.post(key, parameterStorageFactory(key, featureManager))
        this.postFM(key, featureManager, client)
      }
      case Some(paramActor) => {
        val paramStorageAck = paramActor ? ConfirmInit()
        paramStorageAck onComplete {
          case Success(p) => {
            val paramTimes = p.asInstanceOf[AckParamStorage]
            val createdAt = paramTimes.createdAt.toString
            client ! HttpResponse(200, entity=HttpEntity(s"""{"model_namespace": "$key", "created_at": "$createdAt"}"""),
              headers = List(`Content-Type`(`application/json`)))
          }
          case Failure(e) => log.info(e.getLocalizedMessage)
        }
      }
    }
  }

  def receive = LoggingReceive {
    case Get(modelKey, paramKey) => {
//      log.info("ModelStorage received GET")
      val model = modelKey match {
        case Some(mK) => models.get(mK)
        case None => models.getLatest()
      }
      model match {
        case Some(m) => {
          val modelFuture = paramKey match {
            case Some(pK) => m ? GetParams(pK)
            case None => m ? GetLatestParams()
          }
//          log.info(s"MODELREF: $m")
          modelFuture pipeTo sender
        }
        case None => {
          log.info("Invalid model key")
          sender ! Model(None, None)
        }
      }
    }

    case GetLatest() => {
//      log.info("ModelStorage received GET")
      sender ! models.getLatest
    }

    case Post(key, featureManager, client) => {
      postFM(key, featureManager, client)
    }

    case Put(modelKey, modelParameters, client) => {
      models.get(modelKey) match {
        case Some(paramActor) => {
          paramActor ! PutParams(modelParameters, client)
        }
        case None => client ! HttpResponse(entity=HttpEntity("Invalid model key"))
      }
    }

    case GetAllKeys() => {
      models.getAllKeys() pipeTo sender
    }
  }
}

object ModelStorage {
  import ParameterStorage._

  final case class Get(modelKey: Option[String], paramKey: Option[String])
  final case class GetLatest()
  final case class Post(key: String, featureManager: HashFeatureManager, client: ActorRef)
  final case class Put(modelKey: String, modelParameters: ParameterEntry, client: ActorRef)
  final case class AckParamStorage(createdAt: DateTime, modifiedAt: DateTime)
  final case class ParameterKeySet(pKeys: Map[String, Any])
  final case class GetAllKeys()
  class StorageException(msg: String) extends RuntimeException(msg)
}

sealed class ModelVault {
  import ParameterStorage._

  implicit val formats = DefaultFormats

  private var kv = Map[String, ActorRef]()
  private var lastAdded: String = _

  def post(key: String, paramStorageActor: ActorRef) = synchronized {
    kv = kv + (key -> paramStorageActor)
    lastAdded = key
  }

  def get(key: String): Option[ActorRef] = synchronized {
    kv.get(key)
  }

  def getLatest(): Option[ActorRef] = synchronized {
    kv.get(lastAdded)
  }

  def getLatestString(): String = {
    val lastAddedStringTmp = lastAdded
    lastAddedStringTmp match {
      case s if s != null => s
      case _ => ""
    }
  }

  def getAllKeys()(implicit  ec: ExecutionContext, timeout: akka.util.Timeout) = {
    val mKeysPKeys = kv.map {
      case (pKey, pActor) => pActor ? GetKeySet(pKey)
    }
    Future.fold(mKeysPKeys)(Map[String, Any]()) {
      case (a: Map[String, Any], b: ModelStorage.ParameterKeySet) => a ++ b.pKeys
    }
  }
}
