package domain

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.math.{pow, sqrt}

class LowComplexitySubsequences(subsequenceLength: Int) extends Serializable {

  private val bitsByAlphabetSize = 2
  private var bitsMask = (1 << bitsByAlphabetSize) - 1
  private val deviationLimit = 5

  private val alphabetSize = 4

  private case class Subsequence(encoded: Int, standardDeviation: Double)

  def getLowComplexitySubsequences(sc: SparkContext): RDD[Int] = {
    val maxSize = pow(alphabetSize, subsequenceLength).toInt

    val subSequences = calculateStdDeviation(sc.parallelize(0 until maxSize))

    val avgStdDeviation = calculateTotalStdDeviation(subSequences) / maxSize

    val variance = subSequences
      .map(v => pow(v.standardDeviation - avgStdDeviation, 2))
      .reduce(_ + _) / maxSize

    val limit = avgStdDeviation + (sqrt(variance) * this.deviationLimit)

    val lowComplexitySequences = subSequences
      .filter(v => v.standardDeviation > limit)
      .map(v => v.encoded)

    lowComplexitySequences
  }

  private def calculateStdDeviation(rdd: RDD[Int]): RDD[Subsequence] = rdd
    .map(v => Subsequence(v, sequenceStandardDeviation(v)))

  private def calculateTotalStdDeviation(rdd: RDD[Subsequence]): Double = rdd
    .map(_.standardDeviation).reduce(_ + _)

  private def sequenceStandardDeviation(encoded: Int): Double = {
    var ac, cc, gc, tc = 0

    for (pos <- 0 until subsequenceLength) {
      val posInInt = subsequenceLength - pos
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
    val m = subsequenceLength / 4.0
    val variance = (pow(ac - m, 2) + pow(cc - m, 2) + pow(gc - m, 2) + pow(tc - m, 2)) / 4.0
    sqrt(variance)
  }
}

object LowComplexitySubsequences {
  def apply(subsequenceLength: Int): LowComplexitySubsequences = new LowComplexitySubsequences(subsequenceLength)

  def main(args: Array[String]): Unit = {
    val l = LowComplexitySubsequences(18)

    val spark = SparkSession
      .builder()
      .appName("Genoogle | Low Complexity Subsequences Filter")
      .master("local[*]")
      .getOrCreate()

    l.getLowComplexitySubsequences(spark.sparkContext)
  }
}
