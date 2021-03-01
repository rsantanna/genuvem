import java.net.URI

import encoder.DNAEncoder
import ingestion.{FASTAFileLoader, Transforms}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Genlastic {

  private val subsequenceLength = 11

  def getSparkContext(master: String, appName: String): SparkContext = {
    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate()

    spark.sparkContext
  }

  def getSubsequenceFilter(fs: FileSystem, sc: SparkContext, filterPath: String): Set[Int] = {
    var filter: RDD[Int] = null

    if (!fs.exists(new Path(filterPath))) {
      filter = Transforms.createFilter(sc, subsequenceLength)
      filter.saveAsTextFile(filterPath)
    } else {
      filter = sc.textFile(filterPath)
        .map(x => x.toInt)
    }

    filter
      .collect
      .toSet
  }

  def clearLocation(fs: FileSystem, location: String): Unit = {
    val path = new Path(location)
    if (fs.exists(path)) {
      print(s"Deleting $path")
      fs.delete(path, true)
    }
  }

  /**
   * Genlastic main entrypoint
   *
   * @param args (0): the HDFS host and port (i.e.: http://hadoop-snc:9000)
   *             (1): the HDFS work directory (i.e.: /user/hduser/genlastic)
   */
  def main(args: Array[String]): Unit = {

    // Read input arguments
    val hdfsAddress = args(0)
    val hdfsWorkdir = args(1)

    val hdfsBaseDir = s"$hdfsAddress/$hdfsWorkdir"

    // Initialize Spark and HDFS context
    val sc = getSparkContext("local[*]", "Genlastic | Import FASTA")
    val fs = FileSystem.get(new URI(hdfsAddress), sc.hadoopConfiguration)

    // Clear output directories
    clearLocation(fs, s"$hdfsBaseDir/tokenized/")
    clearLocation(fs, s"$hdfsBaseDir/converted/")

    // Initialize File Loader and tokenize all FASTA files
    val loader = FASTAFileLoader(subsequenceLength)

    /**
     * The tokenization process is executed sequentially at the moment on `FASTAFileLoader` class.
     * Each FASTA file begins with a header started by ">" containing the sequence metadata. A nucleotide sequence is
     * composed by strings of A, T, C, G characters. Special characters like R or K denoting one or more nucleotides
     * may appear as well, but those will be replaced by "A" during the encoding process.
     *
     * Example of FASTA file content:
     *
     * >L42023.1 Haemophilus influenzae Rd KW20, complete genome
     * TATGGCAATTAAAATTGGTATCAATGGTTTTGGTCGTATCGGCCGTATCGTATTCCGTGCAGCACAACAC
     * CGTGATGACATTGAAGTTGTAGGTATTAACGACTTAATCGACGTTGAATACATGGCTTATATGTTGAAAT
     * ATGATTCAACTCACGGTCGTTTCGACGGCACTGTTGAAGTGAAAGATGGTAACTTAGTGGTTAATGGTAA
     * AACTATCCGTGTAACTGCAGAACGTGATCCAGCAAACTTAAACTGGGGTGCAATCGGTGTTGATATCGCT
     * GTTGAAGCGACTGGTTTATTCTTAACTGATGAAACTGCTCGTAAACATATCACTGCAGGCGCAAAAAAAG
     * TTGTATTAACTGGCCCATCTAAAGATGCAACCCCTATGTTCGTTCGTGGTGTAAACTTCAACGCATACGC
     * AGGTCAAGATATCGTTTCTAACGCATCTTGTACAACAAACTGTTTAGCTCCTTTAGCACGTGTTGTTCAT
     * GAAACTTTCGGTATCAAAGATGGTTTAATGACCACTGTTCACGCAACGACTGCAACTCAAAAAACTGTGG
     * ...
     */

    val fastaStatus = fs.listStatus(new Path(s"$hdfsBaseDir/fasta"))
    fastaStatus.foreach(fileStatus => loader.writeTokens(fs, fileStatus.getPath, hdfsWorkdir))

    /**
     * Filter: compute all low complexity subsequences using Spark. The filter step is necessary to improve the
     * search accuracy, removing subsequences that have low complexity and may denote non-coding regions of the DNA.
     * Examples of low complexity subsequences of length 11 and their integer representations are:
     *
     * (AAAAACAAAAA,1024)
     * (GGGTGGGGGGG,2812586)
     * (TTATTTTTTTT,3997695)
     * (AAAAAAAAAAA,0)
     * (AAAAATAAAAA,3072)
     * (GGGGGGGGGGC,2796201)
     * (TTTTTTTTTTG,4194302)
     * (GTGGGGGGGGG,3058346)
     * (GGGGGGGGTGG,2796218)
     * (TGTTTTTTTTT,3932159)
     */
    val filterSet = getSubsequenceFilter(fs, sc, s"$hdfsBaseDir/filter/filter_$subsequenceLength.flt")
    val encoder = DNAEncoder(subsequenceLength)

    /**
     * For details about transformations, check the Transforms class.
     */
    // Read tokenized files to Spark RDD
    val tokensRDD = loader.readTokens(sc, s"$hdfsBaseDir/tokenized/")

    // Encode each token (i.e.: TCTAGCCA) to integer (i.e.: 56468)
    val encoded = Transforms.encode(tokensRDD, encoder)

    // Filter all low complexity subsequences (i.e.: AAAAAAAA)
    val filtered = Transforms.filterLowComplexity(encoded, filterSet)

    // Decode the encoded subsequences filtered previously
    val filteredAndDecoded = Transforms.decode(filtered, encoder)

    // Check if the subsequence and the decoded one are equal. Any character not in [ATCG] is replaced by A, since the
    // encoder only support these four characters. Thus, for this validation step we need to do this workaround otherwise
    // tokens with wildchars like 'R' or 'K', may not be decoded back. The wildchars are special chars in sequences that
    // represent two or more nucleotides within the same position in the sequence.
    filteredAndDecoded
      .filter { case (_, _, subsequence, _, decoded) => subsequence.replaceAll("[^ATCG]", "A") != decoded }
      .foreach(println)
  }
}
