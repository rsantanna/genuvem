#!/usr/bin/env bash
source ./genuvem-env.sh

# Create Buckets
if ! gsutil ls "gs://$RESOURCES_BUCKET" > /dev/null 2>&1; then
  gcloud storage buckets create "gs://$RESOURCES_BUCKET" --location=us-central1
else
  echo "Bucket $RESOURCES_BUCKET already exists. Skipping..."
fi

if ! gsutil ls "gs://$DATAPROC_BUCKET" > /dev/null 2>&1; then
  gcloud storage buckets create "gs://$DATAPROC_BUCKET" --location=us-central1
else
  echo "Bucket $DATAPROC_BUCKET already exists. Skipping..."
fi

if ! gsutil ls "gs://$TEMP_BUCKET" > /dev/null 2>&1; then
  gcloud storage buckets create "gs://$TEMP_BUCKET" --location=us-central1
else
  echo "Bucket $TEMP_BUCKET already exists. Skipping..."
fi

# Sync files
echo "Syncing resources..."
gsutil rsync -r resources "gs://$RESOURCES_BUCKET/"
