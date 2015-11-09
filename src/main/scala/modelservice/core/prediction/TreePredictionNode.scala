package modelservice.core.prediction

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{OneForOneStrategy, Actor}
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.BoundedMessageQueueSemantics
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import breeze.linalg.SparseVector
import modelservice.core.prediction.TreePredictionActor.ValidModel

/**
 * Node actor in prediction tree
 */
class TreePredictionNode(predictionActors: PredictionActors) extends Actor with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  import TreePredictionNode._

  implicit val timeout: Timeout = 1.second
  import context.dispatcher

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0,
    withinTimeRange = 1.second) {
    case _: Exception => Stop
  }

  def receive = {
    case NodePredict(childrenFreeVars, boundVars, nodeFreeVars, model, decisionVars) => {
      // Base case: childrenFreeVars is empty (ie this is a leaf), so predict
      // on nodeFreeVars + bound vars and return the result
      if (childrenFreeVars.size == 0) {
        val leafVars = boundVars ++ nodeFreeVars
        val leafVarsStrings = leafVars.flatMap{x =>
          x._2 match {
            case y: String => Some(x.asInstanceOf[(String, String)])
            case _ => None
          }
        }
        val featureVector = model.hashFeatureManager.parseRow(leafVarsStrings)
        val prediction = lrPredict(model.weights, featureVector.mapActiveValues(_.toDouble))
        val expectedValue = model.hashFeatureManager.getExpectedValue(prediction, leafVars)
        sender() ! Seq(PredictionResult(decisionVars, expectedValue))
      } else {
        /**
         * Generate all the possible futures from this node with cross products of any branches beginning here
         *
         * Flatten child nodes directly accessible from the current node. ie:
         * {{{
         * {"color": ["red", "blue"], "shape": ["round", "square"], "publisher_id": {"abc123": { ... } ... }}
         * }}}
         * becomes
         * {{{
         * [[({"color": "red"}, {}), ({"color": "blue"}, {})],
         *  [({"shape": "round"}, {}), ({"shape": "square"}, {})],
         *  [({"publisher_id": "abc123"}, { ... })]]
         * }}}
         */
        val nodeKV = childrenFreeVars.toSeq.flatMap(x =>
          x._2 match {
            case s: Map[String, Map[String, Any]] => Some(s.toSeq.map(t =>
              ChildNodeMapKV(Map(x._1 -> t._1), t._2, Map[String, Any]())))
            case s: List[String] => Some(s.map(t => ChildNodeMapKV(Map(x._1 -> t), Map[String, Any](), Map(x._1 -> t))))
            case s: String => Some(List(ChildNodeMapKV(Map(x._1 -> x._2), Map[String, Any](), Map[String, Any]())))
            case d: Double => Some(List(ChildNodeMapKV(Map(x._1 -> x._2), Map[String, Any](), Map[String, Any]())))
            case _ => None
          }
        )

        /**
         * Generate the cross product of attribute child nodes. ie:
         * {{{
         * [[({"color": "red"}, {}), ({"color": "blue"}, {})],
         *  [({"shape": "round"}, {}), ({"shape": "square"}, {})],
         *  [({"publisher_id": "abc123"}, { ... })]]
         * }}}
         * becomes
         * {{{
         * [({"color": "red", "shape": "round", "publisher_id", "abc123"}, { ... }],
         *  ({"color": "red", "shape": "square", "publisher_id", "abc123"}, { ... }],
         *  ({"color": "blue", "shape": "round", "publisher_id", "abc123"}, { ... }],
         *  ({"color": "blue", "shape": "square", "publisher_id", "abc123"}, { ... }]
         * }}}
         */
        val nodeCrossProduct = crossProduct(combineChildNodes)(nodeKV)

        // Launch recursive node actors
        val treeTraversalFutures = nodeCrossProduct.map { x =>
          predictionActors.treePredictionNodes ?
            NodePredict(x.childrenKV, boundVars, nodeFreeVars ++ x.flattenedKV, model, decisionVars ++ x.decisionVars)
        }

        // Reduce all futures by combining the results
        val combinedFutures = Future.fold(treeTraversalFutures)(Seq[PredictionResult]()) {
          case (a: Seq[PredictionResult], b: Seq[PredictionResult]) => a ++ b
        }

        combinedFutures pipeTo sender()
      }
    }
  }
}

object TreePredictionNode {
  case class NodePredict(childrenFreeVars: Map[String, Any], boundVars: Map[String, Any],
                         nodeFreeVars: Map[String, Any], model: ValidModel, decisionVars: Map[String, Any])
  case class ChildNodeMapKV(flattenedKV: Map[String, Any], childrenKV: Map[String, Any], decisionVars: Map[String, Any])
  case class PredictionResult(varMap: Map[String, Any], prediction: Double)
  case class PredictionResults(results: Seq[PredictionResult])

  def logisticFunction(t: Double): Double = t match {
    case x if x > 0.0 => 1.0 / (1.0 +  scala.math.exp(-t))
    case _ =>  scala.math.exp(t) / (1.0 +  scala.math.exp(t))
  }

  def lrPredict(w: SparseVector[Double], x: SparseVector[Double]): Double = {
    logisticFunction(w.t * x)
  }

  /**
   * Create a cross product function
   * @param func a function to combine each pair of objects when generating the cross product
   * @tparam T
   * @return a new function that performs the cross product of a sequence of sequences
   */
  def crossProduct[T](func: (T, T) => T) = (row: Seq[Seq[T]]) => {
    row.reduce(
      (q1, q2) => for {
        a <- q1
        b <- q2
      } yield func(a, b)
    )
  }

  def simpleCombineFunc(a: Map[String, String], b: Map[String, String]): Map[String, String] = {
    a ++ b
  }

  /**
   * Combine each child node into a single child node
   * @param a
   * @param b
   * @return
   */
  def combineChildNodes(a: ChildNodeMapKV, b: ChildNodeMapKV): ChildNodeMapKV = {
    ChildNodeMapKV(
      a.flattenedKV ++ b.flattenedKV,
      a.childrenKV ++ b.childrenKV,
      a.decisionVars ++ b.decisionVars
    )
  }
}
