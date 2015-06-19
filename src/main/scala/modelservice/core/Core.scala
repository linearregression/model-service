package modelservice.core

import akka.actor.ActorSystem

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
