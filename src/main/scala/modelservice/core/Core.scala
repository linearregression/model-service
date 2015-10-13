package modelservice.core

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.routing.RoundRobinRouter
import modelservice.core.prediction.PredictionActors
import modelservice.storage.StorageActors

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

trait CoreActors {
  val parseActor: ActorRef
  val modelParser: ActorRef
}

trait CoreActorSet extends CoreActors {
  this: Core with PredictionActors with StorageActors =>

  val parseActor = system actorOf Props(new FeatureParser(this)).withRouter(RoundRobinRouter(nrOfInstances = 8))
  val modelParser = system actorOf Props(new ModelParser(this)).withRouter(RoundRobinRouter(nrOfInstances = 4))
}