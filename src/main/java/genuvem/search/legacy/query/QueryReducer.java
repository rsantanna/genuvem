package genuvem.search.legacy.query;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import genuvem.io.IntIntWritable;

public class QueryReducer extends Reducer<IntWritable, IntIntWritable, IntWritable, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<IntIntWritable> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();

		for (IntIntWritable w : values) {
			sb.append(w + " ");
		}

		context.write(key, new Text(sb.toString()));
	}
}
