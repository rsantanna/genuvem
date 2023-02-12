# Genuvem

Genuvem is a platform for executing [Genoogle](https://github.com/felipealbrecht/Genoogle) in a cloud environment based
on [SparkBLAST](https://github.com/sparkblastproject/v2).

Genoogle is software for similar DNA sequences searching developed by Felipe Albrecht: please refer to
the [original repository](https://github.com/felipealbrecht/Genoogle) for more information about the software.

Check the page [Reference Guidelines](docs/references.md) if you're writing about those.

## Local Environment

Please refer to [Local Environment](docs/environment.md) for instructions to run Genoogle and simulate a Hadoop Single Node Cluster.

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
./resources/scripts/setup.sh
```

This script will create the necessary buckets if they don't exist and sync the resources folder with the resources
bucket.

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
    --class Genuvem \
    --jars genuvem.jar \
    --cluster genuvem-cluster \
    --region us-central1 \
    --properties "spark.executor.instances=4,spark.executor.cores=2,spark.task.cpus=2,spark.executor.memory=12g,spark.driver.memory=12g" \
    -- \
    sars-cov-2-2021 \
    sars-cov-1.fasta
```

## Interactive Environment

![This image shows Genuvem running on Zeppelin. The screen is divided in three parts: a form for entering Genoogle's
input parameters, a table with search results and a simple interface showing the selected alignment.
](docs/images/zeppelin.png "Genuvem on Zeppelin")

The Dataproc cluster comes with a [Zeppelin](https://zeppelin.apache.org/) server ready to use. Zeppelin provides an interactive
notebook environment, similar to [Jupyter](https://jupyter.org/), which can be used to easily develop and execute Spark
scripts.

You can find a Zeppelin notebook in the `notebooks/` folder, which can be imported into the cluster. This notebook
contains a Spark script to execute and visualize the query results, and also visualize the actual alignments.

The Zeppelin web interface is available from the Dataproc cluster console.
Just open the Cluster Details and click on the Web Interfaces tab. Zeppelin should be the last link in the Component
Gateway list.

## Cite this project! ❤️

When writing about this project, please refer to:


