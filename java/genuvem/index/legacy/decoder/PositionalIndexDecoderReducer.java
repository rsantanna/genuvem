package genuvem.index.legacy.decoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PositionalIndexDecoderReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	private int kmerLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt("kmerlength", 16);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (key.get() % kmerLength == 0) {

			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

}
