name := "genuvem"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.14"

val sparkVersion = "3.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided",
  "com.databricks" % "spark-xml_2.12" % "0.16.0"
)