package modelservice.storage

import akka.actor.{Actor, ActorLogging, ActorRef}

/**
 * Created by cdgore on 6/16/15.
 */
class ModelBroker extends Actor with ActorLogging {
  import ModelBroker._
  import ModelStorage._

  def receive = {
    case StoreModel(basicModel: BasicModel, modelStorage: ActorRef, client: ActorRef) => {
      val model = ModelFactory(basicModel)
      val key = model.hashCode().toString
      modelStorage ! Put(ModelEntry(key, model), client)
    }
    case StoreModelWithKey(ModelWithKey(key: String, basicModel: BasicModel), modelStorage: ActorRef, client: ActorRef) => {
      val model = ModelFactory(basicModel)
      modelStorage ! Put(ModelEntry(key, model), client)
    }
    // TODO: serve models to HTTP client
    //    case GetLatestModel(modelStorage: ActorRef) =>
  }
}

object ModelBroker {
  final case class BasicSparseVector(index: Array[Int], data: Array[Double], maxFeatures: Int,
                                     numFeatures: Option[Int] = None)
  final case class BasicFeatureManager(k: Int, label: String, singleFeatures: List[String],
                                       quads: Option[Seq[Seq[Seq[String]]]] = None)
  final case class BasicModel(basicWeights: BasicSparseVector, basicFeatureManager: BasicFeatureManager)
  final case class ModelWithKey(key: String, basicModel: BasicModel)
  final case class StoreModel(basicModel: BasicModel, modelStorage: ActorRef, client: ActorRef)
  final case class StoreModelWithKey(modelWithKey: ModelWithKey, modelStorage: ActorRef, client: ActorRef)
  final case class GetLatestModel(modelStorage: ActorRef)
  final case class GetModelByKey(key: String, modelStorage: ActorRef)
}