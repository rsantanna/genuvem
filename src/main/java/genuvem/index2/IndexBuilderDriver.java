package genuvem.index2;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fastdoop.FASTAlongInputFileFormat;
import genuvem.io.IndexEntryWritable;

public class IndexBuilderDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Inverted Index Builder";

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

		job.setJarByClass(IndexBuilderDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);

		createMultipleOutputs(inputPath, job, fs);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndexEntryWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IndexBuilderMapper.class);
		job.setReducerClass(IndexBuilderReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);

	}

	private void createMultipleOutputs(Path inputPath, Job job, FileSystem fs)
			throws FileNotFoundException, IOException {
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(inputPath, false);

		while (remoteIterator.hasNext()) {
			LocatedFileStatus fileStatus = remoteIterator.next();
			Path filePath = fileStatus.getPath();

			MultipleOutputs.addNamedOutput(job, "idx" + filePath.getName(), TextOutputFormat.class, Text.class,
					Text.class);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new IndexBuilderDriver(), args);
		System.exit(exitCode);
	}
}
