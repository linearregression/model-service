package modelservice.api

import akka.actor.Props
import modelservice.core._

/**
 * Api
 */
trait Api {
  this: Core with CoreActors =>

  private implicit val _ = system.dispatcher
  val modelService = system actorOf Props(new ModelServiceActor(this))

}
