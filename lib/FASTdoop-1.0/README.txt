INTRODUCTION
============

FASTdoop is a generic Hadoop library for the management of FASTA and FASTQ files. It includes
three input reader formats with associated record readers. These readers are optimized to
read data efficiently from FASTA/FASTQ files in a variety of settings. They are:

- FASTAshortInputFileFormat: optimized to read a collection of short sequences from a FASTA file.
- FASTAlongInputFileFormat: optimized to read a very large sequence (even gigabytes long) from a FASTA file.
- FASTQInputFileFormat: optimized to read a collection of short sequences from a FASTQ file.

The FASTdoop 1.0 distribution comes as a single compressed archive, downloadable from the following link:
http://www.di.unisa.it/Bioinformatics/tools/FASTdoop/FASTdoop-1.0/FASTdoop-1.0.zip. 
It includes both the source code as well as a binary precompiled version ready to be run.



Using FASTdoop
==============
As a preliminary step, in order to use FASTdoop in an Hadoop application, the FASTdoop jar
file must be included in the classpath of the virtual machines used to run that application. 
Then, it is possible to use one of the readers coming with FASTdoop by running the standard
setInputFormatClass method, as in the following example:

		...
		Configuration conf = getConf();
		String jobname = "Test";
		Job job = Job.getInstance(conf, jobname);
		job.setInputFormatClass(FASTAshortInputFileFormat.class);
		...
		
Notice that FASTAshortInputFileFormat and FASTQInputFileFormat takes no parameters while 
FASTAlongInputFileFormat allows the user to specify, when processing a split, how much
characters of the following input split should be analyzed as well. This option has been 
added to handle cases like k-mer counting where a sequence of characters may begin in a
split and end in the following one. A usage example of the three readers is provided in
the directory src/fastoop/test.


BUILDING FASTdoop
===========

As an alternative to using the provided jar, it is possible to build FASTdoop from scratch
starting from the source files available with the FASTdoop distribution. The compilation
process uses the ant software (see http://ant.apache.org/). Be also sure to have
the $HADOOP_HOME environment variable set at the Hadoop installation path and, then,
run the following commands from the shell:

cd FASTdoop-1.0/
ant build clean
ant build
ant build createjar

At the end of the compilation, the FASTdoop-1.0.jar file will be created in the current
directory.

Notice: FASTdoop has been developed and tested against version 2.7.2 of Hadoop on both
Linux and MacOs



USAGE EXAMPLES
==============

FASTdoop comes with three test classes that can be used to parse the content of FASTA/FASTQ
files. The source code of these classes is available in the src directory. The following 
examples assume that the java classpath environment variable is set to include the jar files
of a standard Hadoop installation (>=2.7.2).

Example 1: Print on screen all the short sequences contained in FASTdoop-1.0/data/short.fasta

java -cp FASTdoop-1.0.jar fastdoop.test.TestFShort data/short.fasta


Example 2: Print on screen the long sequence contained in fastdoop-1.0/data/long.fasta

java -cp FASTdoop-1.0.jar fastdoop.test.TestFLong data/long.fasta


Example 3:  Print on screen all the short sequences contained in fastdoop-1.0/data/short.fastq

java -cp FASTdoop-1.0.jar fastdoop.test.TestFQ data/short.fastq
