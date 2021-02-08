package ingestion

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.URI
import java.util.zip.GZIPInputStream
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


//noinspection DuplicatedCode
class FASTAFileLoader(subsequenceLength: Int) extends Serializable {

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
   * Get the InputStream according to file type and compression.
   *
   * @param fs         the HDFS handler
   * @param inputPath  the HDFS path to the file
   * @param compressed boolean flag: `true` if file is compressed, `false` otherwise
   * @return the InputStream
   */
  private def getReader(fs: FileSystem, inputPath: Path, compressed: Boolean = false): BufferedReader = {
    var inputStream: InputStream = fs.open(inputPath)
    if (inputPath.getName.toLowerCase.endsWith(".gz")) {
      inputStream = new GZIPInputStream(inputStream)
    }
    new BufferedReader(new InputStreamReader(inputStream))
  }

  /**
   * Read a FASTA formatted file from HDFS and tokenize it, ignoring any headers in the middle
   *
   * @param reader Buffered Reader
   * @param writer Buffered Writer
   */
  private def tokenizeSequence(reader: BufferedReader, writer: FSDataOutputStream): Unit = {
    var line = seekSequenceHeader(reader)
    var header = line.replace(">", "")

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

            writer.writeBytes(s"$header\t$position\t$subsequence\n") // protobuf?
            position += 1
          }
        } else {
          if (tail != null && !tail.equals("")) {
            writer.writeBytes(s"$header\t$position\t$tail\n")
          }
          header = line.replace(">", "")
          position = 0
        }
      }
    } while (line != null)

    if (tail != null && !tail.equals("")) {
      writer.writeBytes(s"$header\t$position\t$tail\n")
    }
  }

  /**
   *
   * @param fs        the HDFS handler
   * @param inputPath the HDFS path to the file
   * @param workdir   the base directory
   */
  def load(fs: FileSystem, inputPath: Path, workdir: String): Unit = {
    println(s"Reading file $inputPath")

    val tokenizedPath = new Path(s"$workdir/tokenized/${inputPath.getName}.tks")
    if (fs.exists(tokenizedPath)) {
      fs.delete(tokenizedPath, true)
    }

    val reader = getReader(fs, inputPath)
    val writer = fs.create(tokenizedPath)

    tokenizeSequence(reader, writer)

    reader.close()
    writer.close()
  }
}

object FASTAFileLoader {
  def apply(subsequenceLength: Int): FASTAFileLoader = new FASTAFileLoader(subsequenceLength)
}