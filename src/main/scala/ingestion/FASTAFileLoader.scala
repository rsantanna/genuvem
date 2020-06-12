package ingestion

import java.io.{BufferedReader, ByteArrayOutputStream, IOException, InputStreamReader}
import java.net.URI

import org.apache.avro.io.EncoderFactory
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object FASTAFileLoader {

  val logger: Logger = Logger.getLogger(FASTAFileLoader.getClass)

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

  private def tokenizeSequence(fs: FileSystem, inputPath: Path, outputPath: Path, maxLength: Int): Unit = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))
    val writer = fs.create(outputPath)

    try {
      var line = seekSequenceHeader(reader)
      val header = line
      logger.info("Processing sequence: " + header)

      var position = 1
      var tail: String = null
      do {
        line = reader.readLine

        if (line != null) {

          line = line.trim

          if (line.startsWith(">")) {
            logger.info(s"New header at $position: $line")
          } else {
            logger.debug(line)

            tail = if (tail == null) line else tail.concat(line)

            while (tail.length > maxLength) {
              val subsequence = tail.substring(0, maxLength)
              tail = tail.substring(maxLength)

              writer.writeBytes(s"$position,$subsequence\n")
              position += 1
            }
          }
        }
      } while (line != null)

      if (tail != null && !tail.equals("")) {
        writer.writeBytes(s"$position,$tail\n")
      }
    } catch {
      case e: Throwable => logger.error(s"${e.getClass.getName} has been thrown while reading ${inputPath.getName}: ${e.getMessage}")
    } finally {
      reader.close()
      writer.close()
    }
  }


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hduser")

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Genoogle Spark | Import FASTA")

    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(new URI("hdfs://hadoop-snc:9000"), sc.hadoopConfiguration)

    val inputPath = new Path("hdfs://hadoop-snc:9000/user/hduser/fasta/Mus_musculus.GRCm38.dna.alt.fa")
    val outputPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/converted/${inputPath.getName}.pos")

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    tokenizeSequence(fs, inputPath, outputPath, 13)
    logger.info(s"File ${outputPath.getName} created.")
  }

}
