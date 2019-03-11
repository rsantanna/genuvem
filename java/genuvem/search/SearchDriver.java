package genuvem.search;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import genuvem.comparator.QuerySequenceComparator;
import genuvem.io.FilterKeyWritable;
import genuvem.io.HighScoringPairWritable;
import genuvem.io.SearchKeyWritable;
import genuvem.partitioner.QueryPartitioner;
import genuvem.search.filter.FilterMapper;
import genuvem.search.filter.FilterReducer;
import genuvem.search.query.QueryMapper;
import genuvem.search.query.QueryReducer;

public class SearchDriver extends Configured implements Tool {

	private static final String QUERY_JOB_NAME = "Genuvem | Query";
	private static final String FILTER_JOB_NAME = "Genuvem | Filter";

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path filteredPath = new Path(args[2]);

		Configuration conf = super.getConf();
		Job job = getQueryJobInstance(conf, inputPath, outputPath);

		if (job.waitForCompletion(true)) {

			job = getFilterJobInstance(conf, outputPath, filteredPath);
			return job.waitForCompletion(true) ? 0 : 1;

		} else {
			return 1;
		}
	}

	private Job getQueryJobInstance(Configuration conf, Path inputPath, Path outputPath) throws IOException {
		Job job = Job.getInstance(conf, QUERY_JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(outputPath, true);

		SequenceFileInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(SearchDriver.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(SearchKeyWritable.class);
		job.setMapOutputValueClass(HighScoringPairWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(HighScoringPairWritable.class);

		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);

		job.setPartitionerClass(QueryPartitioner.class);
		job.setGroupingComparatorClass(QuerySequenceComparator.class);

		return job;
	}

	private Job getFilterJobInstance(Configuration conf, Path inputPath, Path outputPath) throws IOException {
		Job job = Job.getInstance(conf, FILTER_JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(outputPath, true);

		SequenceFileInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(SearchDriver.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(FilterKeyWritable.class);
		job.setMapOutputValueClass(HighScoringPairWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(HighScoringPairWritable.class);

		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);

		job.setPartitionerClass(QueryPartitioner.class);
		job.setGroupingComparatorClass(QuerySequenceComparator.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SearchDriver(), args);
		System.exit(exitCode);
	}
}
