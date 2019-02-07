package genuvem.index.legacy.decoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import genuvem.io.TextIntWritable;
import genuvem.io.IntArrayWritable;

public class PositionalIndexDecoderMapper extends Mapper<TextIntWritable, IntArrayWritable, IntWritable, Text> {

	private int filteredSequenceId;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		filteredSequenceId = conf.getInt("sequenceId", 0);
	}

	@Override
	protected void map(TextIntWritable key, IntArrayWritable value, Context context)
			throws IOException, InterruptedException {

		IntWritable sequenceIdWritable = key.getInteger();

		if (filteredSequenceId == sequenceIdWritable.get()) {
			for (Writable w : value.get()) {
				context.write((IntWritable) w, key.getText());
			}
		}

	}

}
