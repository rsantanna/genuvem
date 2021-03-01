package ingestion

import encoder.DNAEncoder
import filter.LowComplexitySubsequences
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Transforms {


  /**
   * Read all tokenized files within the inputPath. The files are expected to be in the Tab Separated Values format:
   * "$header\t$position\t$subsequence\n"
   *
   * i.e.:
   * L42023.1 Haemophilus influenzae Rd KW20, complete genome  1 TATGGCAATTA
   * L42023.1 Haemophilus influenzae Rd KW20, complete genome	2	AAATTGGTATC
   * L42023.1 Haemophilus influenzae Rd KW20, complete genome  3	AATGGTTTTGG
   *
   * @param sc
   * @param inputPath
   * @return
   */
  def readTokens(sc: SparkContext, inputPath: String): RDD[(String, Int, String)] = sc.textFile(inputPath)
    .map(s => s.split("\t"))
    .map { case Array(header, id, sequence) => (header, id.toInt, sequence) }

  /**
   * Given a subsequence length, this transformation uses spark to spawn a collection of integers from 0 to
   * subsequenceLength and apply a series of calculations to determine if the given integer represents a low
   * complexity subsequence, returning a set of numbers representing subsequences to be excluded from the index.
   *
   * @param sc
   * @param subsequenceLength
   * @return
   */
  def createFilter(sc: SparkContext, subsequenceLength: Int): RDD[Int] = LowComplexitySubsequences(subsequenceLength)
    .getLowComplexitySubsequences(sc)

  /**
   * Read a textfile containing perviously computed low complexity subsequences. The file is expected to contain
   * a single integer per line, each integer representing an encoded subsequence of arbitrary length.
   *
   * @param sc
   * @param filterPath
   * @return
   */
  def loadFilter(sc: SparkContext, filterPath: String): Set[Int] = sc.textFile(filterPath)
    .map(_.toInt)
    .collect
    .toSet

  /**
   * For each tuple of tokenized subsequences, return the length of the subsequence and its integer representations.
   *
   * Input:
   * (L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA)
   *
   * Output:
   * (L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA, 11, 3384380)
   *
   * @param rdd
   * @param encoder
   * @return
   */
  def encode(rdd: RDD[(String, Int, String)], encoder: DNAEncoder): RDD[(String, Int, String, Int, Int)] = rdd
    .map { case (header, position, sequence) => (header, position, sequence, sequence.length, encoder.encodeSubsequenceToInteger(sequence)) }

  /**
   * For each tuple of encoded subsequences, return the decoded string.
   *
   * Input:
   * (L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA, 11, 3384380)
   *
   * Output:
   * (L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA, 11, 3384380, TATGGCAATTA)
   *
   * @param rdd
   * @param encoder
   * @return
   */
  def decode(rdd: RDD[(String, Int, String, Int, Int)], encoder: DNAEncoder): RDD[(String, Int, String, Int, String)] = rdd
    .map { case (header, position, sequence, length, encoded) => (header, position, sequence, length, encoder.decodeIntegerToString(encoded, length)) }

  /**
   * Filter a RDD of encoded subsequences, removing those whose encoded representations appear on the filterSet.
   *
   * Input:
   * RDD[(L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA, 11, 3384380),
   * (L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, AAAAAAAAAAA, 11, 0)]
   *
   * Set[0, 1024, 2812586, ...]
   *
   * Output:
   * RDD[(L42023.1 Haemophilus influenzae Rd KW20, complete genome, 1, TATGGCAATTA, 11, 3384380)]
   *
   * @param rdd
   * @param filterSet
   * @return
   */
  def filterLowComplexity(rdd: RDD[(String, Int, String, Int, Int)], filterSet: Set[Int]): RDD[(String, Int, String, Int, Int)] = {
    val lowComplexitySubsequencesFilter = rdd.sparkContext.broadcast(filterSet)
    rdd.filter { case (_, _, _, _, encoded) => !lowComplexitySubsequencesFilter.value.contains(encoded) }
  }


}
