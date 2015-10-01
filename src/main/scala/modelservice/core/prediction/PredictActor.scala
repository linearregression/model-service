package modelservice.core.prediction

import akka.actor.{Actor, ActorLogging, ActorRef}
import breeze.linalg.SparseVector
import org.json4s._
import org.json4s.jackson.Serialization
import spray.http.ContentTypes._
import spray.http.{HttpEntity, HttpResponse}
import spray.http.HttpHeaders._

/**
 * Make the predictions and return to client
 */
class PredictActor extends Actor with ActorLogging {
  import PredictActor._

  implicit val formats = DefaultFormats

  def receive = {
    case Predict(weights, featureVector, client) => {
      val prediction = lrPredict(weights, featureVector.mapActiveValues(_.toDouble))
      client ! HttpResponse(
        entity = HttpEntity(
          `application/json`,
          Serialization.write(Map("prediction" -> prediction))
        ),
        headers = List(Connection("close"))
      )
    }
  }

  def logisticFunction(t: Double): Double = t match {
    case x if x > 0.0 => 1.0 / (1.0 +  scala.math.exp(-t))
    case _ =>  scala.math.exp(t) / (1.0 +  scala.math.exp(t))
  }

  def lrPredict(w: SparseVector[Double], x: SparseVector[Double]): Double = {
    logisticFunction(w.t * x)
  }
}

object PredictActor {
  final case class Predict(weights: SparseVector[Double], featureVector: SparseVector[Int], client: ActorRef)

}