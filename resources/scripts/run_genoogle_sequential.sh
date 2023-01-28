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

BATCH_FILE=$LOCAL_RUN_DIR/commands.bat
QUERY_FILE="/app/genoogle/queries/exp2/$QUERY_NAME"

OUT_FILE_NAME=output.$HOSTNAME
LOCAL_OUT_FILE=$LOCAL_RUN_DIR/$OUT_FILE_NAME

# Create local run dir
if [ ! -d $LOCAL_RUN_DIR ]; then
        mkdir -p $LOCAL_RUN_DIR
fi

# Write command to batch file
GENOOGLE_CMD="search $DATABANK $QUERY_FILE $LOCAL_OUT_FILE\nexit"
echo -e $GENOOGLE_CMD > $BATCH_FILE

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

