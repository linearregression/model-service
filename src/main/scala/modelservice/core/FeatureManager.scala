package modelservice.core

import breeze.linalg.SparseVector

import scala.Numeric._
import scala.util.hashing._

trait FeatureManager[A, B] extends Serializable {
  def parseRow(row: Map[String, A])(implicit num: Numeric[B]): SparseVector[B]
  def getW(): SparseVector[Double]
}

class HashFeatureManager extends FeatureManager[String, Int] {
  var k = 0
  var singleFeatures = List[String]()
  var quadFeatures = List[String]()
  var quads = Seq[Seq[Seq[String]]]()
  var label = ""
  def allFeatures = {
    singleFeatures :::
      quadFeatures
  }
  var hashFunctions = Map[String, (String => Int)]()
  var numericValue = ""

  def withK(k: Int): HashFeatureManager = {
    this.k = k
    this
  }

  def withLabel(label: String): HashFeatureManager = {
    this.label = label
    this
  }

  def withSingleFeatures(singleFeatures: List[String]): HashFeatureManager = {
    this.singleFeatures = singleFeatures
    this.hashFunctions = this.allFeatures.map(
      x => (x, this.hashFactory(x))
    ).toMap
    this
  }

  def withQuadraticFeatures(quads: Seq[Seq[Seq[String]]]): HashFeatureManager = {
    this.quads = quads
    this.quadFeatures = this.quadProduct(
      this.quads,
      (x: String, y: String) => x + "_" + y
    ).toList
    this.hashFunctions = this.allFeatures.map(
      x => (x, this.hashFactory(x))
    ).toMap
    this
  }

  def withNumericValue(numericValue: String): HashFeatureManager = {
    this.numericValue = numericValue
    this
  }

  def getFeatures(row: Map[String, String], intercept: Boolean = true): List[Int] = {
    val filteredSingleFeatures = this.singleFeatures.flatMap{f =>
      row.get(f) match {
        case Some(fv) => Some((f, fv))
        case None => None
      }
    }
    val filteredQuads = this.quads.flatMap{x =>
      val rowList = x.flatMap {y =>
        val subList = y.flatMap{f =>
          row.get(f) match {
            case Some(fv) => Some((f, fv))
            case None => None
          }
        }
        subList.length match {
          case l if l > 0 => Some(subList)
          case _ => None
        }
      }
      rowList.length match {
        case l if l > 0 => Some(rowList)
        case _ => None
      }
    }
    val allKeys = filteredSingleFeatures ++ this.quadProduct(
      filteredQuads,
      (a: (String, String), b: (String, String)) => (a._1 + "_" + b._1, a._2 + "_" + b._2)
    )

    val hashedFeatures = allKeys.flatMap{
      case (k: String, v: String) => {
        this.hashFunctions.get(k) match {
          case Some(hf: (String => Int)) => Some(hf(v))
          case _ => None
        }
      }
      case _ => None
    }

    intercept match {
      case true => hashedFeatures :+ 0
      case false => hashedFeatures
    }
  }

  def getFeaturesSparse(row: Map[String, String], intercept: Boolean = true): SparseVector[Int] = {
    val sparseFeaturesIndex = this.getFeatures(row, intercept).sorted.toArray
    val numFeatures = sparseFeaturesIndex.length
    val sparseFeaturesData = (1 to numFeatures).map(x => 1).toArray
    new SparseVector(
      sparseFeaturesIndex,
      sparseFeaturesData,
      numFeatures,
      {1 << this.k}
    )
  }

  def hashFactory(key: String): (String => Int) = {
    val truncBytes = (1 << k) + (~1 + 1)
    val hashSpace = (1 << 16) + (~1 + 1)
    val hashSalt = hashSpace >> 2
    val hashSeed = (key.getBytes.map(c => c.toInt) :+ hashSalt).
      reduce((x, y) => (x * y) % hashSpace)

    (value => MurmurHash3.bytesHash(value.getBytes, hashSeed) & truncBytes)
  }

  def quadProduct[T](quads: Seq[Seq[Seq[T]]], func: (T, T) => T) = {
    quads.map(
      row => row.reduce(
        (q1, q2) => for {a <- q1; b <- q2} yield func(a, b))
    ).reduce(_ ++ _)
  }

  def getExpectedValue(probability: Double, row: Map[String, Any]): Double = {
    try {
      row.getOrElse(this.numericValue, 1.0).asInstanceOf[Double] * probability
    } catch {
      case e: Exception => probability
    }
  }

  def parseRow(row: Map[String, String])(implicit num: Numeric[Int]) = {
    this.getFeaturesSparse(row)
  }

  def getW() = {
    new SparseVector[Double](Array(), Array(), 1 << this.k)
  }
}