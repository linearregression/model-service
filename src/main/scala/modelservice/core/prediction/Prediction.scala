package modelservice.core.prediction

import akka.actor._
import akka.routing.RoundRobinRouter
import modelservice.core.{Core, FeatureParser}

/**
 * Prediction layer trait
 */
trait PredictionActors {
  val predictionActor: ActorRef
  val treePredictionNodes: ActorRef
//  val parseActor: ActorRef
}

trait PredictionActorSet extends PredictionActors {
  this: Core =>
  import modelservice.core.prediction.{TreePredictionActor, TreePredictionNode}

  val predictionActor = system actorOf Props(new TreePredictionActor(this)).withRouter(RoundRobinRouter(nrOfInstances = 8))
  val treePredictionNodes = system actorOf Props(new TreePredictionNode(this)).withRouter(RoundRobinRouter(nrOfInstances = 32))
//  val parseActor = system actorOf Props(new FeatureParser(treePredictionActors)).withRouter(RoundRobinRouter(nrOfInstances = 8))
}