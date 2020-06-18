package ingestion

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI

import domain.LowComplexitySubsequences
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

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

  private def encodeSubSequenceToInteger(subSymbolList: String): Int = {
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

  def encodeSequence(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val sc = spark.sparkContext

    val lowComplexitySubsequences = LowComplexitySubsequences(subsequenceLength)
      .getLowComplexitySubsequences(spark)
      .collect()
      .toSet

    val filter = sc.broadcast(lowComplexitySubsequences)

    sc.textFile(inputPath)
      .map(_.split(","))
      .map(v => (encodeSubSequenceToInteger(v(1)), v(0).toInt))
      .filter(v => filter.value.contains(v._1))
      .saveAsTextFile(outputPath)
  }
}

object FASTAFileLoader {
  def apply(subSequenceLength: Int): FASTAFileLoader = new FASTAFileLoader(subSequenceLength)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hduser")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Genoogle Spark | Import FASTA")
      .getOrCreate()

    val sc = spark.sparkContext
    val fs = FileSystem.get(new URI("hdfs://hadoop-snc:9000"), sc.hadoopConfiguration)

    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/Mus_musculus.GRCm38.dna.alt.fa")

    val tokenizedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/tokenized/${inputPath.getName}.tks")
    if (fs.exists(tokenizedPath)) {
      fs.delete(tokenizedPath, true)
    }

    val convertedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/converted/${inputPath.getName}.cod")
    if (fs.exists(convertedPath)) {
      fs.delete(convertedPath, true)
    }

    val loader = FASTAFileLoader(18)
    loader.tokenizeSequence(fs, inputPath, tokenizedPath)
    loader.encodeSequence(spark, tokenizedPath.toString, convertedPath.toString)
  }

}