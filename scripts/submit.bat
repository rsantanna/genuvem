@echo off

gcloud dataproc jobs submit spark ^
    --class Genuvem ^
    --jars=..\out\artifacts\genuvem_scala_jar\genuvem-scala.jar ^
    --cluster genuvem-cluster-4 ^
    --region us-central1 ^
    --properties "spark.submit.deployMode=client,spark.executor.instances=4,spark.executor.cores=2,spark.task.cpus=2,spark.executor.memory=12g,spark.driver.memory=12g,spark.jars.packages=com.databricks:spark-xml_2.12:0.14.0" ^
    -- ^
    "sars-cov-2-2021" ^
    "sars-cov-2-2022-4.fasta" ^
     4
