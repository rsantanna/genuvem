package genuvem.index.positional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fastdoop.FASTAlongInputFileFormat;
import genuvem.io.IndexKeyWritable;
import genuvem.io.IntArrayWritable;

public class PositionalIndexBuilderDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Positional Inverted Index Builder";

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(outputPath, true);

		FASTAlongInputFileFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(PositionalIndexBuilderDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);

		job.setMapOutputKeyClass(IndexKeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IndexKeyWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(PositionalIndexBuilderMapper.class);
		job.setReducerClass(PositionalIndexBuilderReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PositionalIndexBuilderDriver(), args);
		System.exit(exitCode);
	}
}
