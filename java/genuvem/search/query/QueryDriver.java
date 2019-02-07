package genuvem.search.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import genuvem.comparator.QueryGroupingComparator;
import genuvem.io.HighScoringPairWritable;
import genuvem.io.QueryKeyWritable;
import genuvem.partitioner.QueryPartitioner;

public class QueryDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Query";

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = super.getConf();
		Job job = getJobInstance(conf, inputPath, outputPath);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	private Job getJobInstance(Configuration conf, Path inputPath, Path outputPath) throws IOException {
		Job job = Job.getInstance(conf, JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(outputPath, true);

		SequenceFileInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(QueryDriver.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(QueryKeyWritable.class);
		job.setMapOutputValueClass(HighScoringPairWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(HighScoringPairWritable.class);

		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);

		job.setPartitionerClass(QueryPartitioner.class);
		job.setGroupingComparatorClass(QueryGroupingComparator.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new QueryDriver(), args);
		System.exit(exitCode);
	}
}
