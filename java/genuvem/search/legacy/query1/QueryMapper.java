package genuvem.search.legacy.query1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.TextIntWritable;
import genuvem.io.IntArrayWritable;
import genuvem.io.IntIntWritable;

public class QueryMapper extends Mapper<TextIntWritable, IntArrayWritable, IntWritable, IntIntWritable> {

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
	protected void map(TextIntWritable key, IntArrayWritable value, Context context)
			throws IOException, InterruptedException {
		Text indexSubsequence = key.getText();

		IntWritable queryIndex = new IntWritable();
		IntIntWritable matchPosition = new IntIntWritable();

		for (int i = 0; i <= query.length() - kmerLength; i++) {

			String subsequence = query.substring(i, i + kmerLength);

			if (subsequence.equals(indexSubsequence.toString())) {
				queryIndex.set(i);

				for (Writable w : value.get()) {
					matchPosition.setInt1(key.getInteger());
					matchPosition.setInt2((IntWritable) w);
					
					context.write(queryIndex, matchPosition);
				}
			}
		}
	}
}
