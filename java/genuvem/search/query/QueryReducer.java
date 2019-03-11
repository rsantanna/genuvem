package genuvem.search.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.SearchKeyWritable;

public class QueryReducer
		extends Reducer<SearchKeyWritable, HighScoringPairWritable, IntWritable, HighScoringPairWritable> {

	int kmerLength;
	int maxHspDistance;
	int minLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt("kmerlength", 16);
		maxHspDistance = conf.getInt("maxdistance", kmerLength * 3);
		minLength = conf.getInt("minlength", kmerLength);
	}

	@Override
	protected void reduce(SearchKeyWritable key, Iterable<HighScoringPairWritable> values, Context context)
			throws IOException, InterruptedException {

		List<HighScoringPairWritable> list = new ArrayList<HighScoringPairWritable>();

		for (HighScoringPairWritable hsp : values) {
			if (!tryMerge(hsp, list)) {
				list.add(hsp.clone());
			}
		}

		for (HighScoringPairWritable hsp : list) {
			if (hsp.getLength() >= minLength) {
				context.write(key.getSequenceId(), hsp);
			}
		}
	}

	private boolean tryMerge(HighScoringPairWritable hsp, List<HighScoringPairWritable> list) {
		boolean merge = false;

		for (HighScoringPairWritable existing : list) {
			if (hsp.getDatabaseSequenceStart() - existing.getDatabaseSequenceEnd() <= maxHspDistance
					&& hsp.getQueryStart() - existing.getQueryEnd() <= maxHspDistance) {
				existing.setQueryEnd(hsp.getQueryEnd());
				existing.setDatabaseSequenceEnd(hsp.getDatabaseSequenceEnd());

				merge = true;
			}
		}

		return merge;
	}

}
