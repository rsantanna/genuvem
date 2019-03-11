package genuvem.search.filter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.FilterKeyWritable;
import genuvem.io.HighScoringPairWritable;

public class FilterMapper
		extends Mapper<IntWritable, HighScoringPairWritable, FilterKeyWritable, HighScoringPairWritable> {

	FilterKeyWritable filterKey;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		filterKey = new FilterKeyWritable();
	}

	@Override
	protected void map(IntWritable key, HighScoringPairWritable value, Context context)
			throws IOException, InterruptedException {
		filterKey.setSequenceId(key);
		filterKey.setHsp(value);

		context.write(filterKey, value);
	}

}
