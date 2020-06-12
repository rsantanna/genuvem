package domain

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashSet
import scala.math.{pow, sqrt}

object LowComplexitySubsequences {

  private val logger = Logger.getLogger(LowComplexitySubsequences.getClass)

  private val bitsByAlphabetSize = 2
  private val subSequenceLength = 18
  private var bitsMask = (1 << bitsByAlphabetSize) - 1
  private val deviationLimit = 5

  private val alphabetSize = 4

  case class Subsequence(encoded: Int, standardDeviation: Double)

  def calculateStdDeviation(rdd: RDD[Int]): RDD[Subsequence] = rdd
    .map(v => Subsequence(v, sequenceStandardDeviation(v)))

  def calculateTotalStdDeviation(rdd: RDD[Subsequence]): Double = rdd
    .map(_.standardDeviation).reduce(_ + _)

  def getLowComplexitySubsequences(sc: SparkContext): Set[Int] = {

    logger.info("Low complexity subsequences set build has started.")

    val maxSize = pow(alphabetSize, subSequenceLength).toInt

    val subSequences = calculateStdDeviation(sc.parallelize(0 until maxSize))

    val avgStdDeviation = calculateTotalStdDeviation(subSequences) / maxSize

    val variance = subSequences
      .map(v => pow(v.standardDeviation - avgStdDeviation, 2))
      .reduce(_ + _) / maxSize

    val limit = avgStdDeviation + (sqrt(variance) * this.deviationLimit)

    val lowComplexitySequences = subSequences
      .filter(v => v.standardDeviation > limit)
      .map(_.encoded)
      .collect
      .toSet

    logger.info(s"Low complexity subsequences set build has generated a set of ${SizeEstimator.estimate(lowComplexitySequences)} bytes.")
    lowComplexitySequences
  }

  private def sequenceStandardDeviation(encoded: Int): Double = {
    var ac, cc, gc, tc = 0

    for (pos <- 0 until subSequenceLength) {
      val posInInt = subSequenceLength - pos
      val shift = posInInt * bitsByAlphabetSize
      var value = encoded >> (shift - bitsByAlphabetSize)
      value &= bitsMask
      value match {
        case 0 => ac += 1
        case 1 => cc += 1
        case 2 => gc += 1
        case 3 => tc += 1
      }
    }
    val m = subSequenceLength / 4.0
    val variance = (pow(ac - m, 2) + pow(cc - m, 2) + pow(gc - m, 2) + pow(tc - m, 2)) / 4.0
    sqrt(variance)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Genoogle Spark | Import FASTA")

    val sc = new SparkContext(sparkConf)
    val seqs = getLowComplexitySubsequences(sc)
  }
}
