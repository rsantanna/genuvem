from pyspark.sql import SparkSession
import uuid
import os
import sys
import argparse

parser = argparse.ArgumentParser(prog="Genuvem")
parser.add_argument("--databank", "-d", type=str, default="sars-cov-2-2021")
parser.add_argument("--query", "-q", type=str, default="sars-cov-1.fasta")
args = parser.parse_args(sys.argv[1:])

databank = args.databank
query_path = "/app/genoogle/queries/exp2/" + args.query
script_path = "/app/genoogle/run_genoogle.sh"

# Initialize Session
spark = SparkSession.builder.master("yarn").appName('Genuvem').getOrCreate()
sc = spark.sparkContext

# Get the number of available nodes
node_count = args.partitions

# Generate unique id and build the args array
run_id = str(uuid.uuid4())
run_args = [script_path, databank, run_id, args.query]
envs = {"GENOOGLE_HOME": os.getenv("GENOOGLE_HOME")}

# Set '>' as delimiter for FASTA files
conf = sc._jsc.hadoopConfiguration()
conf.set("textinputformat.record.delimiter", ">")

# Add '>' back to each sequence read from the query file
# and repartition the RDD by the number of available nodes
rdd = sc \
    .textFile(query_path) \
    .map(lambda x: x.strip()) \
    .filter(lambda x: x) \
    .map(lambda x: '>' + x) \
    .repartition(node_count)

# Run Genoogle and collect the HDFS path of the output files
output_files = rdd.pipe(" ".join(run_args), envs).collect()

print(f"Run {run_id} finished successfully. HDFS output files:")
print(output_files)
