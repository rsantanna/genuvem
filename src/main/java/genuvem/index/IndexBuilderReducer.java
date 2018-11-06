package genuvem.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import genuvem.io.IndexEntryWritable;

public class IndexBuilderReducer
		extends Reducer<Text, IndexEntryWritable, Text, Text> {

	private Logger logger = Logger.getLogger(IndexBuilderReducer.class);

	@Override
	protected void reduce(Text key, Iterable<IndexEntryWritable> values,
			Context context) throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();

		// ArrayList<IndexEntryWritable> list = new
		// ArrayList<IndexEntryWritable>();
		// for (IndexEntryWritable entry : values) {
		// list.add(entry);
		// }
		//
		// logger.info("Reducer for key " + key + " will sort " + list.size()
		// + " entries.");
		// Collections.sort(list);

		logger.info("Start building output at reducer for key " + key + ".");
		for (IndexEntryWritable value : values) {
			sb.append(value.toString() + " ");
		}

		context.write(key, new Text(sb.toString()));

		logger.info("Reducer for key " + key + " has finished.");
	}

}
