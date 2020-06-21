package ingestion

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI

import optimization.DNAEncoder
import filter.LowComplexitySubsequences
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class FASTAFileLoader(subsequenceLength: Int) extends Serializable {

  private val encoder = DNAEncoder(subsequenceLength)

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

  /**
   * Read a FASTA formatted file from HDFS and tokenize it, ignoring any headers in the middle
   *
   * @param fs         HDFS handler
   * @param inputPath  Path of existing FASTA file
   * @param outputPath Path of the tokenized file to be created
   */
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

  def createFilter(sc: SparkContext): RDD[Int] = {
    LowComplexitySubsequences(subsequenceLength)
      .getLowComplexitySubsequences(sc)
  }

  def encodeSequence(sc: SparkContext, inputPath: String, filterPath: String): RDD[(Int, Int)] = {
    val lowComplexitySubsequences = sc.textFile(filterPath)
      .map(_.toInt)
      .collect
      .toSet

    val filter = sc.broadcast(lowComplexitySubsequences)

    sc.textFile(inputPath)
      .map(_.split(","))
      .map(v => (v(0).toInt, encoder.encodeSubsequenceToInteger(v(1))))
      .filter(v => !filter.value.contains(v._2))
  }

  def indexifySequence(sc: SparkContext, inputPath: String): Unit = {
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

    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/L42023.1.fasta")
    //    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/ecoli.nt")
    //    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/Mus_musculus.GRCm38.dna.alt.fa")

    val loader = FASTAFileLoader(18)

    val tokenizedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/tokenized/${inputPath.getName}.tks")
    if (fs.exists(tokenizedPath)) {
      fs.delete(tokenizedPath, true)
    }
    loader.tokenizeSequence(fs, inputPath, tokenizedPath)

    val filterPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/filter/filter_18.flt")
    if (!fs.exists(filterPath)) {
      loader.createFilter(sc)
        .saveAsTextFile(filterPath.toString)
    }

    val convertedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/converted/${inputPath.getName}.cod")
    if (fs.exists(convertedPath)) {
      fs.delete(convertedPath, true)
    }
    loader.encodeSequence(spark.sparkContext, tokenizedPath.toString, filterPath.toString)
      .saveAsTextFile(convertedPath.toString)
  }

}