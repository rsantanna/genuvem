package ingestion

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI

import domain.LowComplexitySubsequences
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class FASTAFileLoader(subsequenceLength: Int) extends Serializable {
  private val bitsByAlphabetSize = 2

  private case class Subsequence(encoded: Int, standardDeviation: Double)

  /**
   * Seek the first ">" character of the file indicating a FASTA header
   *
   * @param reader the reader
   * @throws IOException Premature stream end or stream does not appear to contain FASTA formatted data
   * @return String with the header
   */
  @throws[IOException]
  private def seekSequenceHeader(reader: BufferedReader): String = {
    var line = reader.readLine
    if (line == null) {
      throw new IOException("Premature stream end")
    }
    while (line.length() == 0) {
      line = reader.readLine
      if (line == null) {
        throw new IOException("Premature stream end")
      }
    }
    if (!line.startsWith(">")) {
      throw new IOException("Stream does not appear to contain FASTA formatted data: " + line)
    }
    line
  }

  private def getBitsFromChar(symbol: Char): Int = symbol match {
    case 'A' | 'a' => 0
    case 'C' | 'c' => 1
    case 'G' | 'g' => 2
    case 'T' | 't' => 3
    case _ => 0
  }

  private def encodeSubsequenceToInteger(subSymbolList: String): Int = {
    var encoded = 0
    for (i <- 1 to subSymbolList.length) {
      encoded = encoded | (getBitsFromChar(subSymbolList.charAt(i - 1)) << ((subsequenceLength - i) * bitsByAlphabetSize))
    }
    encoded
  }

  def tokenizeSequence(fs: FileSystem, inputPath: Path, outputPath: Path): Unit = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))
    val writer = fs.create(outputPath)

    var line = seekSequenceHeader(reader)

    var position = 1
    var tail: String = null
    do {
      line = reader.readLine

      if (line != null) {
        line = line.trim

        if (!line.startsWith(">")) {
          tail = if (tail == null) line else tail.concat(line)

          while (tail.length > subsequenceLength) {
            val subsequence = tail.substring(0, subsequenceLength)
            tail = tail.substring(subsequenceLength)

            writer.writeBytes(s"$position,$subsequence\n")
            position += 1
          }
        }
      }
    } while (line != null)

    if (tail != null && !tail.equals("")) {
      writer.writeBytes(s"$position,$tail\n")
    }
    reader.close()
    writer.close()
  }

  def createFilter(sc: SparkContext, outputPath: String): Unit = {
    LowComplexitySubsequences(subsequenceLength)
      .getLowComplexitySubsequences(sc)
      .saveAsTextFile(outputPath)
  }

  def encodeSequence(sc: SparkContext, inputPath: String, filterPath: String, outputPath: String): Unit = {
    val lowComplexitySubsequences = sc.textFile(filterPath)
      .map(_.toInt)
      .collect
      .toSet

    val filter = sc.broadcast(lowComplexitySubsequences)

    sc.textFile(inputPath)
      .map(_.split(","))
      .map(v => (v(0).toInt, encodeSubsequenceToInteger(v(1))))
      .filter(v => !filter.value.contains(v._2))
      .saveAsTextFile(outputPath)
  }

  def indexifySequence(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath)
      .map(s => s.split(","))
  }
}

object FASTAFileLoader {
  def apply(subSequenceLength: Int): FASTAFileLoader = new FASTAFileLoader(subSequenceLength)

  def main(args: Array[String]): Unit = {
    if (System.getProperty("HADOOP_USER_NAME") == null) {
      System.setProperty("HADOOP_USER_NAME", "hduser")
    }

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Genoogle Spark | Import FASTA")
      .getOrCreate()

    val sc = spark.sparkContext
    val fs = FileSystem.get(new URI("hdfs://hadoop-snc:9000"), sc.hadoopConfiguration)

    //    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/L42023.1.fasta")
    //    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/ecoli.nt")
    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/Mus_musculus.GRCm38.dna.alt.fa")

    val loader = FASTAFileLoader(18)

    val tokenizedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/tokenized/${inputPath.getName}.tks")
    if (fs.exists(tokenizedPath)) {
      fs.delete(tokenizedPath, true)
    }
    loader.tokenizeSequence(fs, inputPath, tokenizedPath)

    val filterPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/filter/filter_18.flt")
    if (!fs.exists(filterPath)) {
      loader.createFilter(sc, filterPath.toString)
    }

    val convertedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/converted/${inputPath.getName}.cod")
    if (fs.exists(convertedPath)) {
      fs.delete(convertedPath, true)
    }
    loader.encodeSequence(spark.sparkContext, tokenizedPath.toString, filterPath.toString, convertedPath.toString)
  }

}