package modelservice.storage

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingReceive
import breeze.linalg.SparseVector
import modelservice.core.HashFeatureManager
import spray.http.{HttpEntity, HttpResponse}

/**
 * Stores and retrieves models
 */
class ModelStorage extends Actor with ActorLogging {
  import ModelStorage._

  val models = ModelVault

  log.info("Storage initialized")

  def receive = LoggingReceive {
    case Get(key, client) => {
      log.info("ModelStorage received GET")
      sender ! models.get(key)
    }
    case GetLatest() => {
      log.info("ModelStorage received GET")
      sender ! models.getLatest
    }
    case Put(ModelEntry(key, value), client) => {
      models.put(key, value)
      client ! HttpResponse(entity=HttpEntity("Model stored with key: " + key.toString))
    }
  }
}

object ModelStorage {
  final case class Model(weights: SparseVector[Double],
                         featureManager: HashFeatureManager)
  final case class ModelEntry(key: String, value: Model)
  final case class Get(key: String, client: ActorRef)
  final case class GetLatest()
  final case class Put(entry: ModelEntry, client: ActorRef)
  class StorageException(msg: String) extends RuntimeException(msg)
}

object ModelVault {
  import ModelStorage._

  private val kv = ModelLRU[String, Model]()
  private var lastAdded: String = _

  def put(key: String, value: Model): Unit = synchronized {
    kv.put(key, value)
    lastAdded = key
  }

  def get(key: String): Model = synchronized {
    kv.get(key)
  }

  def getLatest(): Model = synchronized {
    kv.get(lastAdded)
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

object ModelFactory {
  import ModelBroker._
  import ModelStorage._

  def apply(basicModel: BasicModel) = {
    basicModel match {
      case BasicModel(BasicSparseVector(index, data, maxFeatures, _),
      BasicFeatureManager(k, label, singleFeatures, quads)) => {
        val weightsSparseVector = new SparseVector[Double](index, data, index.length, maxFeatures)
        val featureManager = (new HashFeatureManager)
          .withK(k)
          .withLabel(label)
          .withSingleFeatures(singleFeatures)
        val featureManagerComplete = quads match {
          case Some(q) => featureManager.withQuadraticFeatures(q)
          case None => featureManager
        }
        Model(weightsSparseVector, featureManagerComplete)
      }
    }
  }
}