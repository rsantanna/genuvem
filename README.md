# Genuvem

Genuvem is a wrapper for executing Genoogle on cloud based on [SparkBLAST](https://github.com/sparkblastproject/v2).

Genoogle is software for similar DNA sequences searching developed by Felipe Albrecht: please refer to
the [original repository](https://github.com/felipealbrecht/Genoogle) for more information about the software.

## Prerequisites

This project depends on [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).
Please, install it and setup your GCP credentials.

You must add the encoded databanks to the `resources/files/fasta/` folder and update the configuration
file `resources/files/conf/genoogle.xml` accordingly. Please, refer to the Genoogle project page for details on how to
encode FASTA files and tweak the configuration parameters.

Make sure your default subnet has access to the internet and other Google Cloud services.
You can enable a Cloud NAT using the default configurations if your subnet is not configured yet.

## Setup

Once your GCP credentials are correctly set up, you can run the setup script located in the `scripts/` folder.

```bash
./scripts/setup.sh
```

This script will create the necessary buckets if they don't exist and sync the resources folder with the resources
bucket.

## Create a Dataproc Cluster

You can use the script `scripts/create_cluster.sh` passing the number of worker nodes as an argument to set up a
Dataproc Cluster ready to run Genuvem:

```bash
./scripts/create_cluster.sh <node count>
```

Example for an 8-node cluster:

```bash
./scripts/create_cluster.sh 8
```

## Submit Job

You can submit a job by calling `scripts/submit_genuvem.sh` and passing the cluster name as parameter. The script
will query the cluster metadata to find the optimal number of executors for your Spark job.

```bash
./scripts/submit_genuvem.sh <cluster name>
```

Example:
```bash
./scripts/submit_genuvem.sh my-dataproc-cluster
```

## Interactive Environment

The cluster comes with a [Zeppelin](https://zeppelin.apache.org/) server ready to use. Zeppelin provides an interactive
notebook environment, similar to [Jupyter](https://jupyter.org/), which can be used to easily develop and execute Spark
scripts.

You can find a Zeppelin notebook in the `notebooks/` folder, which can be imported into the cluster. This notebook
contains a Spark script to execute and visualize the query results, and also visualize the actual alignments.

The Zeppelin web interface is available from the Dataproc cluster console.
Just open the Cluster Details and click on the Web Interfaces tab. Zeppelin should be the last link in the Component
Gateway list.