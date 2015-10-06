package modelservice.core

import akka.actor.{Props, ActorSystem}
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

trait ModelServiceActors {
  this: Core =>
  import modelservice.core.prediction.{TreePredictionActor, TreePredictionNode}

  val treePredictionActors = TreePredictionActor.createActor(system)
  val treePredictionNodes = system actorOf Props[TreePredictionNode].withRouter(RoundRobinRouter(nrOfInstances = 32))
  val parseActor = system actorOf Props(new FeatureParser(treePredictionActors)).withRouter(RoundRobinRouter(nrOfInstances = 8))
}