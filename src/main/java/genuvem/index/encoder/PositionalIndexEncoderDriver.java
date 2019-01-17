package genuvem.index.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fastdoop.FASTAlongInputFileFormat;
import genuvem.io.IndexKeyWritable;
import genuvem.io.IntArrayWritable;

public class PositionalIndexEncoderDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Positional Inverted Index Encoder";

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = super.getConf();
		Job job = getJobInstance(conf, inputPath, outputPath);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	private Job getJobInstance(Configuration conf, Path inputPath, Path outputPath) throws IOException {

		boolean readableOutput = conf.getBoolean("readableOutput", false);

		Job job = Job.getInstance(conf, JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(outputPath, true);

		FASTAlongInputFileFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(PositionalIndexEncoderDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);

		if (readableOutput) {
			job.setOutputFormatClass(TextOutputFormat.class);
		} else {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}

		job.setMapOutputKeyClass(IndexKeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IndexKeyWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(PositionalIndexEncoderMapper.class);
		job.setReducerClass(PositionalIndexEncoderReducer.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new PositionalIndexEncoderDriver(), args);
		System.exit(exitCode);
	}
}
