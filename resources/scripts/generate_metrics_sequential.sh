#!/usr/bin/env bash

QUERY_FILE=$1
INPUT_SIZE=$2

OUTPUT_FILE="./metrics/${QUERY_FILE}.csv"

mkdir -p ./metrics
mkdir -p ./logs

for (( j=1; j<=6; j++ )); do
  SECONDS=0
  ./run_genoogle_sequential.sh "sars-cov-2-2021" `uuid` $QUERY_FILE  2>&1 | tee -a ./logs/$QUERY_FILE.log
  printf "%d,%d,Execution %d,%d\n" $INPUT_SIZE 1 $j $SECONDS 2>&1 | tee -a $OUTPUT_FILE
done

gsutil cp $OUTPUT_FILE gs://genuvem-resources/sequential-metrics/$QUERY_FILE