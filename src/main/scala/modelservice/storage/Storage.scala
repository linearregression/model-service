package modelservice.storage

import akka.actor.{ActorContext, ActorRefFactory, Props, ActorRef}
import akka.routing.RoundRobinRouter
import modelservice.core.Core

/**
 * Storage layer
 */
trait StorageActors {
  val modelBroker: ActorRef
  val modelStorageFactory: (ActorContext) => ActorRef
}

trait StorageActorSet extends StorageActors {
  this: Core =>

  val modelBroker = system actorOf Props(classOf[ModelBroker]).withRouter(RoundRobinRouter(nrOfInstances = 4))
  val modelStorageFactory = (context: ActorContext) => {
    context actorOf(Props[ModelStorage], name="ModelStorage")
  }
}