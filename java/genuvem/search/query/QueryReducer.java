package genuvem.search.query;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.QueryKeyWritable;

public class QueryReducer extends Reducer<QueryKeyWritable, HighScoringPairWritable, IntWritable, HighScoringPairWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(QueryKeyWritable key, Iterable<HighScoringPairWritable> values, Context context)
			throws IOException, InterruptedException {

		for (HighScoringPairWritable value : values) {
			context.write(key.getSequenceId(), value);
		}

	}
}
