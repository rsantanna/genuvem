package ingestion

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI

import domain.LowComplexitySubsequences
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class FASTAFileLoader(subsequenceLength: Int) extends Serializable {
  //  private val logger: Logger = Logger.getLogger(classOf[FASTAFileLoader])

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

    try {
      var line = seekSequenceHeader(reader)
      val header = line
      //      logger.info("Processing sequence: " + header)

      var position = 1
      var tail: String = null
      do {
        line = reader.readLine

        if (line != null) {

          line = line.trim

          if (line.startsWith(">")) {
            //            logger.info(s"New header at $position: $line")
          } else {
            //            logger.debug(line)

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
      //    } catch {
      //      case e: Throwable => logger.error(s"${e.getClass.getName} has been thrown while reading ${inputPath.getName}: ${e.getMessage}")
      //    } finally {
      reader.close()
      writer.close()
    }
  }

  def encodeSequence(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val filter = LowComplexitySubsequences(subsequenceLength).getLowComplexitySubsequences(sc)
    sc.broadcast(filter)

    val file = sc.textFile(inputPath)
      .map(_.split(","))
      .map(v => (v(0).toInt, encodeSubSequenceToInteger(v(1))))
      .filter(s => !filter.contains(s._2))

    file.saveAsTextFile(outputPath)
  }
}

object FASTAFileLoader {
  def apply(subSequenceLength: Int): FASTAFileLoader = new FASTAFileLoader(subSequenceLength)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hduser")

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Genoogle Spark | Import FASTA")

    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(new URI("hdfs://hadoop-snc:9000"), sc.hadoopConfiguration)

    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/L42023.1.fasta")

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
    loader.encodeSequence(sc, tokenizedPath.toString, convertedPath.toString)
  }

}