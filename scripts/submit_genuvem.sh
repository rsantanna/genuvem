#!/usr/bin/env bash

CLUSTER_ID=$1
NODE_COUNT=$2

echo "Cluster $CLUSTER_ID has $NODE_COUNT workers available. Submitting Spark job with $NODE_COUNT executors."
echo "Parameters: " ${@:3}

gcloud dataproc jobs submit spark \
    --class Genuvem \
    --jars=../out/artifacts/genuvem_scala_jar/genuvem-scala.jar \
    --cluster $CLUSTER_ID \
    --region us-central1 \
    --properties "spark.submit.deployMode=client,spark.executor.instances=$NODE_COUNT,spark.executor.cores=2,spark.task.cpus=2,spark.executor.memory=12g,spark.driver.memory=12g,spark.jars.packages=com.databricks:spark-xml_2.12:0.14.0" \
    -- \
    ${@:3}