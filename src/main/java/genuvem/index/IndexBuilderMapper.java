package genuvem.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fastdoop.PartialSequence;
import genuvem.io.IndexEntryWritable;

public class IndexBuilderMapper
		extends Mapper<Text, PartialSequence, Text, IndexEntryWritable> {

	private Logger logger = Logger.getLogger(IndexBuilderMapper.class);

	private int kmerLength;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt("genuvem.kmerlength", 16);
	}

	@Override
	protected void map(Text key, PartialSequence value, Context context)
			throws IOException, InterruptedException {

		int header = Integer.parseInt(value.getHeader());

		logger.info("Cleaning up line breaks in file " + header + ".");
		String seq = value.getValue().replace("\n", "");

		int currentIndex = 0;

		logger.info("Starting mapping index entries in file " + header + ".");
		while (seq.length() >= currentIndex + kmerLength) {
			String subSeq = seq.substring(currentIndex,
					currentIndex + kmerLength);

			Text outKey = new Text();
			outKey.set(subSeq);

			IndexEntryWritable outValue = new IndexEntryWritable();
			outValue.setSequenceId(new IntWritable(header));
			outValue.setPosition(new IntWritable(currentIndex));

			context.write(outKey, outValue);

			currentIndex += 1;
		}

		if (currentIndex < seq.length()) {
			String subSeq = seq.substring(currentIndex, seq.length());

			Text outKey = new Text();
			outKey.set(subSeq);

			IndexEntryWritable outValue = new IndexEntryWritable();
			outValue.setSequenceId(new IntWritable(header));
			outValue.setPosition(new IntWritable(currentIndex));

			context.write(outKey, outValue);
		}

		logger.info("Mapper for file " + header + " has finished.");
	}

}
