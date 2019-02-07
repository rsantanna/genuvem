package genuvem.search.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.QueryKeyWritable;

public class QueryMapper extends Mapper<IntWritable, MapWritable, QueryKeyWritable, HighScoringPairWritable> {

	private String query;
	private int kmerLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		query = conf.get("query");
		kmerLength = conf.getInt("kmerlength", 16);
	}

	@Override
	protected void map(IntWritable sequenceId, MapWritable map, Context context)
			throws IOException, InterruptedException {

		Text subsequence = new Text();

		HighScoringPairWritable hsp = new HighScoringPairWritable();
		QueryKeyWritable key = new QueryKeyWritable();

		for (int i = 0; i <= query.length() - kmerLength; i++) {

			subsequence.set(query.substring(i, i + kmerLength));

			if (map.containsKey(subsequence)) {
				ArrayWritable intArray = (ArrayWritable) map.get(subsequence);

				for (Writable w : intArray.get()) {

					int databaseSequencePosition = ((IntWritable) w).get();

					hsp.setQueryStart(i);
					hsp.setQueryEnd(i + kmerLength - 1);

					hsp.setDatabaseSequenceStart(databaseSequencePosition);
					hsp.setDatabaseSequenceEnd(databaseSequencePosition + kmerLength - 1);

					key.setSequenceId(sequenceId);
					key.setHsp(hsp);

					context.write(key, hsp);
				}
			}
		}
	}
}
