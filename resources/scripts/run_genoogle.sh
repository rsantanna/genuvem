#!/bin/bash

# Parameters
DATABANK=$1
RUN_ID=$2
QUERY_NAME=$3

# Create local log dir
LOCAL_LOG_DIR=$GENOOGLE_HOME/logs/runs
LOCAL_LOG_FILE=$LOCAL_LOG_DIR/$RUN_ID.log

if [ ! -d $LOCAL_LOG_DIR ]; then
        mkdir -p $LOCAL_LOG_DIR
fi
touch $LOCAL_LOG_FILE

# Check
if [ -z $GENOOGLE_HOME ]; then
        echo "FATAL: Genoogle is not installed or \$GENOOGLE_HOME is not set." >> $LOCAL_LOG_FILE
        exit 1
fi

# Set Path
export PATH=$PATH:$HADOOP_HOME/bin

# Change working directory
cd $GENOOGLE_HOME

# File Paths
LOCAL_RUN_DIR=$GENOOGLE_HOME/runs/$QUERY_NAME/$RUN_ID
HDFS_RUN_DIR=/runs/$QUERY_NAME/$RUN_ID

BATCH_FILE=$LOCAL_RUN_DIR/commands.bat
QUERY_FILE=$LOCAL_RUN_DIR/query_split.fa

OUT_FILE_NAME=output.$HOSTNAME
LOCAL_OUT_FILE=$LOCAL_RUN_DIR/$OUT_FILE_NAME
HDFS_OUT_FILE=$HDFS_RUN_DIR/$OUT_FILE_NAME.xml

# Log Variables
echo "-- Environment ----------------------------------------------------------" >> $LOCAL_LOG_FILE
echo "USER = $USER" >> $LOCAL_LOG_FILE
echo "LOCAL_RUN_DIR = $LOCAL_RUN_DIR" >> $LOCAL_LOG_FILE
echo "HDFS_RUN_DIR = $HDFS_RUN_DIR" >> $LOCAL_LOG_FILE
echo "BATCH_FILE = $BATCH_FILE" >> $LOCAL_LOG_FILE
echo "QUERY_FILE = $QUERY_FILE" >> $LOCAL_LOG_FILE
echo "LOCAL_OUT_FILE = $LOCAL_OUT_FILE" >> $LOCAL_LOG_FILE
echo "HDFS_OUT_FILE = $HDFS_OUT_FILE" >> $LOCAL_LOG_FILE
echo "-------------------------------------------------------------------------" >> $LOCAL_LOG_FILE

# Create local run dir
if [ ! -d $LOCAL_RUN_DIR ]; then
        mkdir -p $LOCAL_RUN_DIR
fi

# Write command to batch file
GENOOGLE_CMD="search $DATABANK $QUERY_FILE $LOCAL_OUT_FILE\nexit"
echo -e $GENOOGLE_CMD > $BATCH_FILE

# Create or truncate the query file
> $QUERY_FILE

# Append lines from the input to the existing query file
while read LINE; do
   echo ${LINE} >> $QUERY_FILE
done

# Run Genoogle in Batch Mode
java \
 -server \
 -Xms4086m \
 -Xmx12288m \
 -classpath "${GENOOGLE_HOME}/genoogle.jar:${GENOOGLE_HOME}/lib/*" \
  bio.pih.genoogle.Genoogle -b $BATCH_FILE &>> $LOCAL_LOG_FILE

if [ ! -f $LOCAL_OUT_FILE.xml ]; then
        echo "FATAL: Output file does not exist. Please, check Genoogle logs." >> $LOCAL_LOG_FILE
        exit 2
fi

# Upload local file to HDFS
hdfs dfs -mkdir -p $HDFS_RUN_DIR &>> $LOCAL_LOG_FILE
hdfs dfs -put $LOCAL_OUT_FILE.xml $HDFS_OUT_FILE &>> $LOCAL_LOG_FILE

# Return the HDFS path
echo $HDFS_OUT_FILE

