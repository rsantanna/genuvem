# Genuvem

Genuvem is a platform for executing [Genoogle](https://github.com/felipealbrecht/Genoogle) in a cloud environment based
on [SparkBLAST](https://github.com/sparkblastproject/v2).

Genoogle is software for similar DNA sequences searching developed by Felipe Albrecht: please refer to
the [original repository](https://github.com/felipealbrecht/Genoogle) for more information about the software.

Check the page [Reference Guidelines](docs/references.md) if you're writing about those.
The experiment results are available as [CSV](docs/medicoes.csv).

## Local Environment

Please refer to [Local Environment](docs/environment.md) for instructions to run Genoogle and simulate a Hadoop Single
Node Cluster.

## Prerequisites

This project depends on [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).
Please, install it and set up your GCP credentials.

Make sure your default subnet has access to the internet and other Google Cloud services.
You can enable a Cloud NAT using the default configurations if your subnet is not configured yet.

Please, refer to the [Genoogle](https://github.com/felipealbrecht/Genoogle) project page for details on how to
add and encode FASTA files, and how to tweak the configuration parameters.

## Setup

Now, you can run the setup script located in the `scripts/` folder.

**Disclaimer:** This script will create the necessary buckets if they don't exist and sync the resources folder with the
resources bucket, which can incur in costs.

```bash
./resources/scripts/setup.sh
```

## Create a Dataproc Cluster

You can use the script `scripts/create_cluster.sh` passing the number of worker nodes as an argument to set up a
Dataproc Cluster ready to run Genuvem:

```bash
./resources/scripts/create_cluster.sh <node count>
```

Example for an 8-node cluster:

```bash
./resources/scripts/create_cluster.sh 8
```

## Submit Job

For an optimal experience, it is recommended to run Genoogle on large executors. The command below shows how to submit a
Genuvem search on a cluster with 4 nodes and 2 CPUs, 16 GB of memory at each node.

```bash
spark-submit \
    --master yarn \
    --num-executors 4 \
    --driver-memory 12g \
    --executor-memory 12g \
    --executor-cores 2 \
    --conf spark.task.cpus=2 \
    --class Genuvem \
    genuvem.jar \
    sars-cov-2-2021 \
    sars-cov-1.fasta
```

Or you can submit a Dataproc job from your local machine:

```bash
gcloud dataproc jobs submit spark \
  --cluster genuvem-cluster-2 \
  --region us-central1 \
  --properties "spark.executor.instances=2,spark.executor.cores=2,spark.task.cpus=2,spark.executor.memory=12g,spark.driver.memory=12g" \
  --jar genuvem.jar \
  -- \
  sars-cov-2-2021 \
  sars-cov-2-2022-4.fasta

```

## Interactive Environment

![This image shows Genuvem running on Zeppelin. The screen is divided in three parts: a form for entering Genoogle's
input parameters, a table with search results and a simple interface showing the selected alignment.
](docs/images/zeppelin.png "Genuvem on Zeppelin")

The Dataproc cluster comes with a [Zeppelin](https://zeppelin.apache.org/) server ready to use. Zeppelin provides an
interactive
notebook environment, similar to [Jupyter](https://jupyter.org/), which can be used to easily develop and execute Spark
scripts.

You can find a Zeppelin notebook in the `notebook/` folder, which can be imported into the cluster. This notebook
contains a Spark script to execute and visualize the query results, and also visualize the actual alignments.

The Zeppelin web interface is available from the Dataproc cluster console.
Just open the Cluster Details and click on the Web Interfaces tab. Zeppelin should be the last link in the Component
Gateway list.

## Legacy

This repository keeps the history of initial iterations in the following read-only branches:
- [v1-hadoop](tree/v1-hadoop): used for an initial assessment of Hadoop MapReduce and the Fastdoop library.
- [v2-genlastic](tree/v2-genlastic): intended to reimplement Genoogle on Spark, it has a fully-functional FASTA
  ingestion process, including encoding, decoding and low complexity subsequence filtering.