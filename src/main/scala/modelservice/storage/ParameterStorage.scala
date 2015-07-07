package modelservice.storage

import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.event.LoggingReceive
import breeze.linalg.SparseVector
import modelservice.core.HashFeatureManager
import modelservice.storage.ModelStorage.AckParamStorage
import org.joda.time._
import spray.http.{HttpEntity, HttpResponse}

/**
 * Stores and retrieves parameters
 */
class ParameterStorage(fManager: HashFeatureManager) extends Actor with ActorLogging {
  import ParameterStorage._

  val featureManager = fManager
  val parameters = ParameterVault

  def receive = LoggingReceive {
    case PutParams(entry: ParameterEntry, client) => {
      val paramKey = updateParameters(entry)
      client ! HttpResponse(entity=HttpEntity(s"Parameters stored with key: $paramKey"))
    }

    case GetParams(key) => {
      sender ! Model(parameters.get(key), Some(featureManager))
    }

    case GetLatestParams() => {
      sender ! Model(parameters.getLatest(), Some(featureManager))
    }

    case ConfirmInit() => {
      sender ! parameters.getTimes()
    }
  }

  def updateParameters(entry: ParameterEntry): String = synchronized {
    entry.key match {
      case Some(k) => {
        parameters.put(k, entry.modelParameters)
        k
      }
      case None => {
        val newKey = (new DateTime).toString
        parameters.put(newKey, entry.modelParameters)
        newKey
      }
    }
  }
}

object ParameterStorage {
  final case class Model(weights: Option[SparseVector[Double]],
                         featureManager: Option[HashFeatureManager])
//  final case class ModelEntry(key: String, value: Model)
  final case class ParameterEntry(key: Option[String], modelParameters: SparseVector[Double])
  final case class GetParams(key: String)
  final case class GetLatestParams()
  final case class PutParams(entry: ParameterEntry, client: ActorRef)
  final case class ConfirmInit()

//  final case class Post(key: Option[String], )
  class StorageException(msg: String) extends RuntimeException(msg)
}

object ParameterVault {
  import ModelStorage._
  import ParameterStorage._

  private val createdAt: DateTime = new DateTime()
  private var modifiedAt: DateTime = new DateTime()
  private val kv = ModelLRU[String, SparseVector[Double]]()
//  private var model: Option[HashFeatureManager] = None// model feature manager
  private var lastAdded: String = _

  def put(key: String, value: SparseVector[Double]): Unit = synchronized {
    kv.put(key, value)
    lastAdded = key
    modifiedAt = new DateTime()
  }

  def get(key: String): Option[SparseVector[Double]] = synchronized {
    kv.get(key) match {
      case null => None
      case r: SparseVector[Double] => Some(r)
    }
  }

  def getLatest(): Option[SparseVector[Double]] = synchronized {
    kv.get(lastAdded) match {
      case null => None
      case r: SparseVector[Double] => Some(r)
    }
  }

  def getTimes(): AckParamStorage = {
    AckParamStorage(createdAt, modifiedAt)
  }
}

object ModelLRU {
  def apply[K, V](maxSize: Int = 16): java.util.LinkedHashMap[K, V] = {
    new java.util.LinkedHashMap[K, V]((maxSize.toFloat * (4.0/3.0)).toInt, 0.75f, true) {
      override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = {
        size() > maxSize
      }
    }
  }
}
