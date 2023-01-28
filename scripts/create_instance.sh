#!/bin/sh
#
# This script spins up a GCE instance with 2 vCPUs and 8 GB of memory.

INSTANCE_NAME=$1
MACHINE_TYPE=e2-highmem-2

gcloud compute instances create $INSTANCE_NAME \
    --machine-type=$MACHINE_TYPE \
    --zone us-central1-c \
    --create-disk=auto-delete=yes,boot=yes,device-name=genoogle-pd-standard,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20220419,mode=rw,size=30,type=projects/genoogle/zones/us-central1-a/diskTypes/pd-standard \
    --metadata-from-file startup-script=../resources/scripts/bootstrap.sh