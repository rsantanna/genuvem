package genuvem.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import fastdoop.FASTAlongInputFileFormat;
import genuvem.io.IndexEntryWritable;

public class QueryDriver extends Configured implements Tool {

	private static final String JOB_NAME = "Genuvem | Query";

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, JOB_NAME);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		FASTAlongInputFileFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJarByClass(QueryDriver.class);

		job.setInputFormatClass(FASTAlongInputFileFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndexEntryWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
