package genuvem.search.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.IndexKeyWritable;
import genuvem.io.IntArrayWritable;
import genuvem.io.SequencePositionWritable;

public class QueryMapper extends Mapper<IndexKeyWritable, IntArrayWritable, IntWritable, SequencePositionWritable> {

	private String query;
	private int kmerLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		query = conf.get("query");
		kmerLength = conf.getInt("genuvem.kmerlength", 16);
	}

	@Override
	protected void map(IndexKeyWritable key, IntArrayWritable value, Context context)
			throws IOException, InterruptedException {
		Text indexSubsequence = key.getSubsequence();

		IntWritable queryIndex = new IntWritable();
		SequencePositionWritable matchPosition = new SequencePositionWritable();

		for (int i = 0; i <= query.length() - kmerLength; i++) {

			String subsequence = query.substring(i, i + kmerLength);

			if (subsequence.equals(indexSubsequence.toString())) {
				queryIndex.set(i);

				for (Writable w : value.get()) {
					matchPosition.setSequenceId(key.getSequenceId());
					matchPosition.setPosition((IntWritable) w);
					
					context.write(queryIndex, matchPosition);
				}
			}
		}
	}
}
