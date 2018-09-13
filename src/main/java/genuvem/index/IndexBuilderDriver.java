package genuvem.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import fastdoop.FASTAlongInputFileFormat;
import fastdoop.PartialSequence;

public class IndexBuilderDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("k", 50);

		Job job = Job.getInstance(conf, "FASTdoop Test Long");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(IndexBuilderDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PartialSequence.class);

		job.setMapperClass(IndexBuilderMapper.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
