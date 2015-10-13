package modelservice.storage

import akka.actor._
import akka.testkit.TestProbe
import modelservice.core.Core
import modelservice.core.CoreTests.{DummyActor, Success}
import modelservice.storage.ModelBroker.{BasicFeatureManager, FeatureManagerWithKey, StoreFeatureManagerWithKey}

/**
 * Mocked storage layer actors
 */
trait MockStorageActors extends StorageActors {
  this: Core =>
  val modelBroker = system actorOf Props(new DummyModelBrokerActor(this))
//  def modelStorageFactory = (system) => system actorOf Props[DummyActor]
  val modelStorageFactory = (ac: ActorContext) => {system actorOf Props[DummyActor]}
  val storageTestProbe = TestProbe()
}

class DummyModelBrokerActor(storageActors: MockStorageActors) extends Actor with ActorLogging {
  def receive = {
    case StoreFeatureManagerWithKey(FeatureManagerWithKey(key: String, basicModel: BasicFeatureManager),
    modelStorage: ActorRef, client: ActorRef) => {
      log.info("DummyModelBrokerActor received StoreFeatureManagerWithKey")
      if (key == "m_key" & basicModel.label == "target" & basicModel.quads.nonEmpty) {
        storageActors.storageTestProbe.ref ! Success
      }
    }
  }
}
