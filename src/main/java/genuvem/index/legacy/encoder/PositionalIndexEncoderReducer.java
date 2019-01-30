package genuvem.index.legacy.encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import genuvem.io.TextIntWritable;
import genuvem.io.IntArrayWritable;

public class PositionalIndexEncoderReducer
		extends Reducer<TextIntWritable, IntWritable, TextIntWritable, IntArrayWritable> {

	private Logger logger = Logger.getLogger(PositionalIndexEncoderReducer.class);

	@Override
	protected void reduce(TextIntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		logger.debug("Start building output list at reducer for key " + key + ".");

		List<IntWritable> list = new ArrayList<IntWritable>();

		for (IntWritable iw : values) {
			list.add(new IntWritable(iw.get()));
		}

		IntArrayWritable outValue = new IntArrayWritable(list.toArray(new IntWritable[list.size()]));

		context.write(key, outValue);

		logger.debug("Reducer for key " + key + " has finished. Out value is " + outValue + ".");

	}

}
