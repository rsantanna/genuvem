import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import fastdoop._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

object SparkTest {

  def openShortFASTA(sc: SparkContext, inputPath: String, inputConf: Configuration) = {
    val rdd = sc.newAPIHadoopFile(inputPath, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], inputConf)
    rdd.values
  }

  def openLongFASTA(sc: SparkContext, inputPath: String, inputConf: Configuration): RDD[PartialSequence] = {
    val rdd = sc.newAPIHadoopFile(inputPath, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence], inputConf)
    rdd.values
  }

  def main(args: Array[String]) {

    val inputPath = args(0)
    val outputPath = args(1)

    val path = new File(outputPath)

    if (path.exists()){
      FileUtils.deleteDirectory(path)
    }

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Genoogle Spark")

    val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")

//    val inputConf = sc.hadoopConfiguration
//    //inputConf.setInt("look_ahead_buffer_size", 4096)
//
//    val rdd = openShortFASTA(sc, inputPath, inputConf)
//
//    val summary = rdd.map(s => "Key: " + s.getKey + "\nSequence: " + s.getValue.substring(0, 70))
//    summary.saveAsTextFile(outputPath)

    //    // Load the text into a Spark RDDln, which is a distributed representation of each line of text
        val textFile = sc.textFile(inputPath)

    val seqs = textFile.flatMap(l => l.split("\n"))
      .filter(s => s.startsWith(">"))

    seqs.saveAsTextFile(outputPath)

    //
    //    // Nucleotide count
    //    val counts = textFile.filter(!_.startsWith(">"))
    //      .flatMap(_.split(""))
    //      .map(n => (n.toUpperCase(), 1))
    //      .reduceByKey(_ + _)
    //
    //    print(counts)
    //    counts.saveAsTextFile(outputPath)
  }

}
