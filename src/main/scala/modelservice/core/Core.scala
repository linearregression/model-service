package modelservice.core

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.routing.RoundRobinRouter

/**
 * Core
 */
trait Core {
  implicit def system: ActorSystem
}

trait InitCore extends Core {
  // Initialize the ActorSystem
  implicit lazy val system = ActorSystem("model-service-actors")

  // Shutdown the JVM when ActorSystem shuts down
  sys.addShutdownHook(system.shutdown())
}

trait ActorSet {
  val treePredictionActors: ActorRef
  val treePredictionNodes: ActorRef
  val parseActor: ActorRef
  val actorSetRef: ActorSet
}

trait ModelServiceActors extends ActorSet {
  this: Core =>
  import modelservice.core.prediction.{TreePredictionActor, TreePredictionNode}

  val actorSetRef = this
  val treePredictionActors = system actorOf Props(new TreePredictionActor(actorSetRef)).withRouter(RoundRobinRouter(nrOfInstances = 8))
  val treePredictionNodes = system actorOf Props(new TreePredictionNode(actorSetRef)).withRouter(RoundRobinRouter(nrOfInstances = 32))
  val parseActor = system actorOf Props(new FeatureParser(treePredictionActors)).withRouter(RoundRobinRouter(nrOfInstances = 8))
}