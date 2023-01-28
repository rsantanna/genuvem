#!/usr/bin/env bash
#
# This script spins up a Dataproc Cluster
CLUSTER_ID=$1
NUM_WORKERS=$2
MACHINE_TYPE=e2-highmem-2
REGION=us-central1

RESOURCES_BUCKET=genuvem-resources
DATAPROC_BUCKET=genuvem-dataproc
TEMP_BUCKET=genuvem-temp


gcloud dataproc clusters create $CLUSTER_ID \
    --region $REGION \
    --network default \
    --no-address \
    --bucket=$DATAPROC_BUCKET \
    --temp-bucket=$TEMP_BUCKET \
    --master-machine-type $MACHINE_TYPE \
    --master-boot-disk-size 100 \
    --num-workers $NUM_WORKERS \
    --worker-machine-type $MACHINE_TYPE \
    --worker-boot-disk-size 100 \
    --image-version 2.0-ubuntu18 \
    --optional-components ZEPPELIN \
    --enable-component-gateway \
    --properties "zeppelin-env:GENOOGLE_HOME=/app/genoogle,spark-env:GENOOGLE_HOME=/app/genoogle,hadoop-env:GENOOGLE_HOME=/app/genoogle" \
    --initialization-actions "gs://$RESOURCES_BUCKET/scripts/bootstrap.sh"
