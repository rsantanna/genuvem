package genuvem.index;

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

public class InvertedIndexBuilderDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Inverted Index Builder";

	@Override
	public int run(String[] args) throws Exception {

		String inputPath = args[1];
		String outputPath = args[2];

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(outputPath), true);

		FASTAlongInputFileFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(InvertedIndexBuilderDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		// setting up an output file for each sequence within the input directory
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(inputPath), false);

		while (remoteIterator.hasNext()) {
			LocatedFileStatus fileStatus = remoteIterator.next();
			Path filePath = fileStatus.getPath();

			MultipleOutputs.addNamedOutput(job, "idx" + filePath.getName(), TextOutputFormat.class, Text.class,
					Text.class);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndexEntryWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(InvertedIndexBuilderMapper.class);
		job.setReducerClass(InvertedIndexBuilderReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new InvertedIndexBuilderDriver(), args);
		System.exit(exitCode);
	}
}
