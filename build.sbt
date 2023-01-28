ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.1.3" % "provided",
  "org.apache.spark" % "spark-sql_2.12" % "3.1.3" % "provided",
  "com.databricks" % "spark-xml_2.12" % "0.14.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "genuvem"
  )
