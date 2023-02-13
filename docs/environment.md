# Local Environment

The local environment depends on [Docker](https://www.docker.com) and [docker-compose](https://docs.docker.com/compose).
Please, install it before proceeding.

### Add FASTA files

To add more sequences, place them inside the `files/databanks/` folder and edit the configuration file
in `conf/genoogle.xml` accordingly.

The snippet below shows how to create a `split-databank` named `my_collection`, with a single databank `my_sequence`
represented by the file `my_sequence.fasta`.
Finally, it is necessary to run Genoogle in Encode Mode to process the new file (see the next section).

*conf/genoogle.xml:*

```xml

<genoogle:conf xmlns:genoogle="http://genoogle.pih.bio.br">
    <!-- ... other configurations ... -->

    <genoogle:databanks>
        <!-- ... other databanks ... -->

        <genoogle:split-databanks name="my_collection" path="databanks" mask="111010010100110111"
                                  number-of-sub-databanks="1" sub-sequence-length="11" low-complexity-filter="5">
            <genoogle:databank name="my_sequence" path="my_sequence.fasta"/>
        </genoogle:split-databanks>

    </genoogle:databanks>
</genoogle:conf>
```

## Running Genoogle with docker compose

##### Interactive shell

```bash
docker compose run genoogle
```

##### Batch Mode

```bash
docker compose run genoogle -b <BATCH_FILE>
```

##### Encode Mode

```bash
docker compose run genoogle -g
```

## Hadoop Single Node Cluster

This project provides a Docker image containing Hadoop, Spark and Zeppelin for development and testing.
This image also comes with Genoogle pre-installed and ready to use.

### Set up GCP Credentials

It is recommended creating a [service account](https://cloud.google.com/iam/docs/service-accounts) specific to this
project.
Once your GCP credentials are correctly set up, create a file named `.env` on the root directory of this project and
the following line:

```shell
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

### Running the single node cluster with docker-compose

To spin up the cluster, execute:

```bash
docker compose up -d
```

Access the services via browser:

- [YARN](http://localhost:8088/)
- [HDFS](http://localhost:50070/)
- [Zeppelin](http://localhost:8080/)

## Testing the Wrapper

Genuvem executes Genoogle at each Spark by piping the input queries into the
script [run_genoogle.sh](../scripts/run_genoogle.sh).
You can test the script without having to run an actual job.

1. Attach to a running container:
```shell
docker attach hadoop-snc
```

2. Once inside the container, replace the parameters and run this command:
```shell
cat $QUERY_FILE | /app/genoogle/run_genoogle.sh $DATABANK $RUN_ID
```
