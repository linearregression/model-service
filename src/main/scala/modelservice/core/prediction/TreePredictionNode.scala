package modelservice.core.prediction

import akka.actor.{Actor, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import breeze.linalg.SparseVector
import modelservice.core.prediction.TreePredictionActor.ValidModel

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Node actor in prediction tree
 */
class TreePredictionNode extends Actor {
  import TreePredictionNode._

  implicit val timeout: Timeout = 5.second
  import context.dispatcher

  def receive = {
    case NodePredict(childrenFreeVars, boundVars, nodeFreeVars, model) => {
      // Base case: childrenFreeVars is empty (ie this is a leaf), so predict
      // on nodeFreeVars + bound vars and return the result
      if (childrenFreeVars.size == 0) {
        val leafVars = boundVars ++ nodeFreeVars
        val featureVector = model.hashFeatureManager.parseRow(leafVars)
        val prediction = lrPredict(model.weights, featureVector.mapActiveValues(_.toDouble))
        sender() ! Seq(PredictionResult(leafVars, prediction))
      } else {

        //  Generate all the possible futures from this node (with cross products of any branches beginning here)
        val nodeKV = childrenFreeVars.toSeq.flatMap(x =>
          x._2 match {
            case s: Map[String, Map[String, Any]] => Some(s.toSeq.map(t => ChildNodeMapKV(Map(x._1 -> t._1), t._2)))
            case s: List[String] => Some(s.map(t => ChildNodeMapKV(Map(x._1 -> t), Map[String, Any]())))
            case _ => None
          }
        )

        val nodeCrossProduct = nodeKV.reduce(
          (q1, q2) => for {
            a <- q1
            b <- q2
          } yield ChildNodeMapKV(
              a.flattenedKV ++ b.flattenedKV,
              a.childrenKV ++ b.childrenKV
            )
        )

        // Launch recursive node actors
        val treeTraversalFutures = nodeCrossProduct.map { x =>
          (context actorOf Props(new TreePredictionNode)) ?
            NodePredict(x.childrenKV, boundVars, nodeFreeVars ++ x.flattenedKV, model)
        }

        // Reduce all futures by combining the results
        val combinedFutures = Future.fold(treeTraversalFutures)(Seq[PredictionResult]()) {
          case (a: Seq[PredictionResult], b: Seq[PredictionResult]) => a ++ b
        }

        combinedFutures pipeTo sender()
      }
    }
  }

  def logisticFunction(t: Double): Double = t match {
    case x if x > 0.0 => 1.0 / (1.0 +  scala.math.exp(-t))
    case _ =>  scala.math.exp(t) / (1.0 +  scala.math.exp(t))
  }

  def lrPredict(w: SparseVector[Double], x: SparseVector[Double]): Double = {
    logisticFunction(w.t * x)
  }

  def crossProduct[T](row: Seq[Seq[T]], func: (T, T) => T) = {
    row.reduce(
      (q1, q2) => for {a <- q1; b <- q2} yield func(a, b)
    )
  }

  def combineFunc(a: Map[String, String], b: Map[String, String]): Map[String, String] = {
    println (a ++ b)
    a ++ b
  }

//  val futures = for (i ← 1 to 1000) yield Future(i * 2) // Create a sequence of Futures
//  val futureSum = Future.reduce(futures)(_ + _)
//  Await.result(futureSum, 1 second) must be(1001000)
}

object TreePredictionNode {
  case class NodePredict(childrenFreeVars: Map[String, Any], boundVars: Map[String, String],
                         nodeFreeVars: Map[String, String], model: ValidModel)
  case class ChildNodeMapKV(flattenedKV: Map[String, String], childrenKV: Map[String, Any])
  case class PredictionResult(varMap: Map[String, String], prediction: Double)
  case class PredictionResults(results: Seq[PredictionResult])
}
