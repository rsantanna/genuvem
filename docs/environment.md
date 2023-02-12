# Local Environment

The local environment depends on [Docker](https://www.docker.com) and [docker-compose](https://docs.docker.com/compose).
Please, install it before proceeding.

### Add FASTA files

To add more sequences, place them inside the `resources/fasta/` folder and edit the configuration file in `resources/conf/genoogle.xml` accordingly.

The snippet below shows how to create a `split-databank` named `my_collection`, with a single databank `my_sequence` represented by the file `my_sequence.fasta`.
Finally, it is necessary to run Genoogle in Encode Mode to process the new file (see the next section).

*conf/genoogle.xml:*
```xml
<genoogle:conf xmlns:genoogle="http://genoogle.pih.bio.br">
    <!-- ... other configurations ... -->

    <genoogle:databanks>
        <!-- ... other databanks ... -->

        <genoogle:split-databanks name="my_collection" path="files/fasta" mask="111010010100110111" number-of-sub-databanks="1" sub-sequence-length="11" low-complexity-filter="5">
            <genoogle:databank name="my_sequence" path="my_sequence.fasta" />
        </genoogle:split-databanks>

    </genoogle:databanks>
</genoogle:conf>
```

## Running Genoogle with docker-compose
##### Interactive shell

```bash
docker-compose run genoogle
```

##### Batch Mode

```bash
docker-compose run genoogle -b <BATCH_FILE>
```

##### Encode Mode

```bash
docker-compose run genoogle -g
```

## Running Hadoop Single Node Cluster

This project provides a Docker image containing Hadoop, Spark and Zeppelin for development and testing.
This image also comes with Genoogle pre-installed and ready to use.
To spin up the cluster, execute:

```bash
docker-compose up
```

Access the services via browser:
- [YARN](http://localhost:8088/)
- [HDFS](http://localhost:50070/)
- [Zeppelin](http://localhost:8080/)