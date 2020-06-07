package ingestion

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FASTAFileLoader {

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "hduser")

    val hdfs = FileSystem.get(new URI("hdfs://hadoop-snc:9000"), new Configuration())

    val srcPath = new Path("D:/Downloads/Mus_musculus.GRCm38.dna.alt.fa")
    val dstPath = new Path("fasta/" + srcPath.getName)

    hdfs.copyFromLocalFile(srcPath, dstPath)


  }

}
