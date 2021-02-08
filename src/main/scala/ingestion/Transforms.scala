package ingestion

import encoder.DNAEncoder
import filter.LowComplexitySubsequences
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Transforms {

  def readTokens(sc: SparkContext, inputPath: String): RDD[(String, Int, String)] = sc.textFile(inputPath)
    .map(s => s.split("\t"))
    .map { case Array(header, id, sequence) => (header, id.toInt, sequence) }

  def createFilter(sc: SparkContext, subsequenceLength: Int): RDD[Int] = LowComplexitySubsequences(subsequenceLength)
    .getLowComplexitySubsequences(sc)

  def loadFilter(sc: SparkContext, filterPath: String): Set[Int] = sc.textFile(filterPath)
    .map(_.toInt)
    .collect
    .toSet

  def encode(rdd: RDD[(String, Int, String)], encoder: DNAEncoder): RDD[(String, Int, Int, Long)] = rdd
    .map { case (header, position, sequence) => (header, position, sequence.length, encoder.encodeSubsequenceToInteger(sequence)) }

  def decode(rdd: RDD[(String, Int, Int, Long)], encoder: DNAEncoder): RDD[(String, Int, Int, String)] = rdd
    .map { case (header, position, length, encoded) => (header, position, length, encoder.decodeIntegerToString(encoded, length)) }

  def filterLowComplexity(rdd: RDD[(String, Int, Int, Int)], filterSet: Set[Int]): RDD[(String, Int, Int, Int)] = {
    val lowComplexitySubsequencesFilter = rdd.sparkContext.broadcast(filterSet)
    rdd.filter { case (_, _, _, encoded) => !lowComplexitySubsequencesFilter.value.contains(encoded) }
  }

  def reduce(rdd: RDD[(Int, Int)]): RDD[(Int, List[Int])] = rdd
    .map { case (position, encoded) => (encoded, List(position)) }
    .reduceByKey(_ ::: _)
}
