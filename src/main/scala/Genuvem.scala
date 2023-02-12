import org.apache.spark.sql.SparkSession
import java.util.UUID.randomUUID

object Genuvem {
  def main(args: Array[String]): Unit = {

    // Pipeline parameters
    val databank = args(0) // "sars-cov-2-2021"
    val queryFile =  args(1) // "/search/sars-cov-1-query.fasta"

    val queryPath = "file:///app/genoogle/queries/exp2/" + queryFile
    val scriptPath = "/app/genoogle/run_genoogle.sh"

    // Initialize Session
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("Genuvem")
      .getOrCreate

    val sc = spark.sparkContext
    val nodeCount = sc.getConf.getInt("spark.executor.instances", 1)
    println(s"nodeCount is $nodeCount")

    // Generate unique id and build the args array
    val runId = randomUUID.toString
    val runArgs = Seq(scriptPath, databank, runId, queryFile)
    val envs = Map("GENOOGLE_HOME" -> sys.env("GENOOGLE_HOME"))

    // Set '>' as delimiter for FASTA files
    val conf = sc.hadoopConfiguration
    conf.set("textinputformat.record.delimiter", ">")

    // Add '>' back to each sequence read from the query file
    // and repartition the RDD by the number of available nodes
    val rdd = sc.textFile(queryPath)
      .map(x => x.trim())
      .filter(x => x.nonEmpty)
      .map(x => '>' + x)
      .repartition(nodeCount)

    // Run Genoogle and collect the HDFS path of the output files
    val outputFiles = rdd.pipe(runArgs, envs).collect

    println(s"Run $runId finished successfully. HDFS output files:")
    outputFiles.map(println)
  }
}
