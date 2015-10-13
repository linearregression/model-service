package modelservice.api

import akka.actor.Props
import modelservice.core._
import modelservice.storage.StorageActors

/**
 * Api
 */
trait Api {
  this: Core with CoreActors with StorageActors =>

  private implicit val _ = system.dispatcher
  val modelService = system actorOf Props(new ModelServiceActor(this, this))

}
