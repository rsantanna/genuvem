package genuvem.search.legacy.query2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.IntIntWritable;

public class QueryMapper extends Mapper<IntWritable, MapWritable, IntWritable, IntIntWritable> {

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

		IntWritable queryIndex = new IntWritable();
		IntIntWritable matchPosition = new IntIntWritable();

		Text subsequence = new Text();

		for (int i = 0; i <= query.length() - kmerLength; i++) {

			subsequence.set(query.substring(i, i + kmerLength));

			if (map.containsKey(subsequence)) {
				queryIndex.set(i);

				ArrayWritable a = (ArrayWritable) map.get(subsequence);

				for (Writable w : a.get()) {
					matchPosition.setInt1(sequenceId);
					matchPosition.setInt2((IntWritable) w);

					context.write(queryIndex, matchPosition);
				}
			}
		}
		
	}
}
