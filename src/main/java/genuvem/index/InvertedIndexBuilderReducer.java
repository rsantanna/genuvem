package genuvem.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import genuvem.io.IndexEntryWritable;

public class InvertedIndexBuilderReducer extends Reducer<Text, IndexEntryWritable, Text, Text> {

	private Logger logger = Logger.getLogger(InvertedIndexBuilderReducer.class);

	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IndexEntryWritable> values, Context context)
			throws IOException, InterruptedException {

		HashMap<IntWritable, StringBuilder> map = new HashMap<IntWritable, StringBuilder>();

		logger.info("Start building output at reducer for key " + key + ".");
		for (IndexEntryWritable value : values) {
			IntWritable sequenceId = value.getSequenceId();

			if (!map.containsKey(sequenceId)) {
				map.put(sequenceId, new StringBuilder());
			}

			StringBuilder sb = map.get(sequenceId);
			sb.append(value.getPosition() + " ");
		}

		for (Entry<IntWritable, StringBuilder> entry : map.entrySet()) {

			IntWritable sequenceId = entry.getKey();
			StringBuilder sb = entry.getValue();

			mos.write("idx" + sequenceId.get(), key, new Text(sb.toString()));
		}

		logger.info("Reducer for key " + key + " has finished.");
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
	}

}
