#!/usr/bin/env bash

inputs=(8)

NODE_COUNT=$1
CLUSTER_ID="genuvem-cluster-${NODE_COUNT}"

OUTPUT_FILE="../metrics/${CLUSTER_ID}.csv"
printf "Sequence Count,Node Count,Execution ID,Elapsed Seconds\n" >> $OUTPUT_FILE

./create_cluster.sh $CLUSTER_ID $NODE_COUNT 2>&1 | tee -a ./logs/$CLUSTER_ID.log

for INPUT_SIZE in "${inputs[@]}"; do
  QUERY_FILE="sars-cov-2-2022-${INPUT_SIZE}.fasta"

  for (( j=1; j<=6; j++ )); do
    SECONDS=0
    ./submit_genuvem.sh $CLUSTER_ID $NODE_COUNT "sars-cov-2-2021" $QUERY_FILE $NODE_COUNT 2>&1 | tee -a logs/$CLUSTER_ID.log
    printf "%d,%d,Execution %d,%d\n" $INPUT_SIZE $NODE_COUNT $j $SECONDS 2>&1 | tee -a $OUTPUT_FILE
  done

  gcloud dataproc jobs submit pig --cluster $CLUSTER_ID --region us-central1 --execute "fs -cp /runs/${QUERY_FILE}/ gs://genuvem-resources/results-scala/${CLUSTER_ID}/${QUERY_FILE}" 2>&1 | tee -a logs/$CLUSTER_ID.log
done

gcloud dataproc jobs submit pig --cluster $CLUSTER_ID --region us-central1 --execute "fs -cp /runs/ gs://genuvem-resources/results-scala/${CLUSTER_ID}" 2>&1 | tee -a logs/$CLUSTER_ID.log
gcloud dataproc clusters delete $CLUSTER_ID --region us-central1 --quiet 2>&1 | tee -a logs/$CLUSTER_ID.log
