import java.net.URI

import ingestion.{FASTAFileLoader, Transforms}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object Genlastic {

  def main(args: Array[String]): Unit = {
    val address = args(0)
    val workdir = args(1)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Genlastic | Import FASTA")
      .getOrCreate()

    val sc = spark.sparkContext
    val fs = FileSystem.get(new URI(address), sc.hadoopConfiguration)

    val subsequenceLength = 11
    val loader = FASTAFileLoader(subsequenceLength)

    val status = fs.listStatus(new Path(s"$address/$workdir/fasta"))
    status.foreach(fileStatus => loader.load(fs, fileStatus.getPath, workdir))

    val filterPath = new Path(s"$address/$workdir/filter/filter_$subsequenceLength.flt")
    if (!fs.exists(filterPath)) {
      Transforms.createFilter(sc, subsequenceLength)
        .saveAsTextFile(filterPath.toString)
    }


    //
    //    val convertedPath = new Path(s"hdfs://hadoop-snc:9000/user/hduser/converted/${inputPath.getName}.cod")
    //    if (fs.exists(convertedPath)) {
    //      fs.delete(convertedPath, true)
    //    }

    // val filterSet = loader.loadFilter(sc, filterPath.toString)

    //    val tokensRDD = loader.readTokens(sc, tokenizedPath.toString)
    //    val encoded = loader.encode(tokensRDD)
    // val filtered = loader.filterLowComplexity(encoded, filterSet)
    // val filteredAndDecoded = loader.decode(filtered)

    //    encoded
    //      .map { case (header, position, length, encoded) => s"$header\t$position\t$length\t$encoded" }
    //      .saveAsTextFile(convertedPath.toString)
  }
}
