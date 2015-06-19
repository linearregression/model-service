package modelservice.core

import akka.actor._
import modelservice.storage.ModelBroker
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.http.HttpEntity

/**
 * Parse models
 */
class ModelParser extends Actor with ActorLogging {
  import ModelBroker._
  import ModelParser._

  def simpleGetMap(m: Map[String, Any], key: String): Map[String, Any] = {
    m.getOrElse(key, Map[String, Any]()).asInstanceOf[Map[String, Any]]
  }

  def receive = {
    case ParsedModelAndStore(rec: HttpEntity, modelStorage: ActorRef, client: ActorRef) =>
      try {
        log.info("Received HttpEntity")
        val basicModel = jsonToHashModel(parse(rec.asString).values.asInstanceOf[Map[String, Any]])
        log.info("Parsed record!")
        log.info(basicModel.toString)

        val modelBroker = context actorOf Props(new ModelBroker)
        modelBroker ! StoreModel(basicModel, modelStorage, client)
        context.stop(self)
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case _ => log.info("Cannot parse request")
  }

  // 2^k feature space
  def jsonToHashModel = jsonToModel(x => math.pow(2.toDouble, x.toDouble).toInt) _

  // k feature space
  def jsonToNumericModel = jsonToModel(x => x) _

  def jsonToModel(vectorSizeFunction: (Int) => Int)(rec: Map[String, Any]): BasicModel = {
    val featureManagerMap = simpleGetMap(rec, "feature_manager")
    val k = featureManagerMap.getOrElse("k", 0).asInstanceOf[BigInt].toInt
    val label = featureManagerMap.getOrElse("label", "").asInstanceOf[String]
    val singleFeatures = featureManagerMap.getOrElse("single_features", List[String]()).asInstanceOf[List[String]]
    val quadFeatures = featureManagerMap.getOrElse("quadratic_features", Seq[Seq[Seq[String]]]()).asInstanceOf[Seq[Seq[Seq[String]]]] match {
      case x: Seq[Seq[Seq[String]]] if x.length > 0 => Some(x)
      case _ => None
    }
    val basicFeatureManager = BasicFeatureManager(k, label, singleFeatures, quadFeatures)

    val modelMap = simpleGetMap(rec, "model_parameters")
    val modelData = modelMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]].toArray
    val modelIndex = modelMap.getOrElse("index", List[BigInt]()).asInstanceOf[List[BigInt]].toArray.map(_.toInt)
    val modelMaxFeatures = vectorSizeFunction(k)
    val modelNumFeatures = Some(modelIndex.length)
    val sparseModel = BasicSparseVector(modelIndex, modelData, modelMaxFeatures, modelNumFeatures)
    BasicModel(sparseModel, basicFeatureManager)
  }
}

object ModelParser {
  case class ParsedModelAndStore(rec: HttpEntity, modelStorage: ActorRef, client: ActorRef)
}
