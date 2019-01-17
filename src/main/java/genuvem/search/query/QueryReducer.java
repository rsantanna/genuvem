package genuvem.search.query;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import genuvem.io.SequencePositionWritable;

public class QueryReducer extends Reducer<IntWritable, SequencePositionWritable, IntWritable, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<SequencePositionWritable> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();

		for (SequencePositionWritable w : values) {
			sb.append(w + " ");
		}

		context.write(key, new Text(sb.toString()));
	}
}
