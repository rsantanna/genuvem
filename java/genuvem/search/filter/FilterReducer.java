package genuvem.search.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import genuvem.io.FilterKeyWritable;
import genuvem.io.HighScoringPairWritable;

public class FilterReducer
		extends Reducer<FilterKeyWritable, HighScoringPairWritable, IntWritable, HighScoringPairWritable> {

	int maxHsps;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		maxHsps = conf.getInt("maxHsps", Integer.MAX_VALUE);
	}

	@Override
	protected void reduce(FilterKeyWritable key, Iterable<HighScoringPairWritable> values, Context context)
			throws IOException, InterruptedException {

		int i = 0;
		for (HighScoringPairWritable value : values) {
			if (i < maxHsps) {
				context.write(key.getSequenceId(), value);
				i++;
			} else {
				break;
			}
		}
	}
}
