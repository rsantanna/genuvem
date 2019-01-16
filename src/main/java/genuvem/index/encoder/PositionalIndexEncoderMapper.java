package genuvem.index.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fastdoop.PartialSequence;
import genuvem.io.IndexKeyWritable;

public class PositionalIndexEncoderMapper extends Mapper<Text, PartialSequence, IndexKeyWritable, IntWritable> {

	private Logger logger = Logger.getLogger(PositionalIndexEncoderMapper.class);

	private int kmerLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt("genuvem.kmerlength", 16);
	}

	@Override
	protected void map(Text key, PartialSequence value, Context context) throws IOException, InterruptedException {

		int header = Integer.parseInt(value.getHeader());

		logger.debug("Cleaning up line breaks in file " + header + ".");
		String seq = value.getValue().replace("\n", "");

		int currentIndex = 0;

		logger.debug("Starting mapping index entries in file " + header + ".");
		while (seq.length() >= currentIndex + kmerLength) {
			String subSeq = seq.substring(currentIndex, currentIndex + kmerLength);

			IndexKeyWritable outKey = new IndexKeyWritable();
			outKey.setSubsequence(new Text(subSeq));
			outKey.setSequenceId(new IntWritable(header));

			IntWritable outValue = new IntWritable(currentIndex);

			logger.debug("Output at index " + currentIndex + " is [" + outKey + ", " + outValue + "]");
			context.write(outKey, outValue);

			currentIndex++;
		}

		if (currentIndex < seq.length()) {
			String subSeq = seq.substring(currentIndex, seq.length());

			IndexKeyWritable outKey = new IndexKeyWritable();
			outKey.setSubsequence(new Text(subSeq));
			outKey.setSequenceId(new IntWritable(header));

			IntWritable outValue = new IntWritable(currentIndex);

			logger.debug("Output at index " + currentIndex + " is [" + outKey + ", " + outValue + "]");
			context.write(outKey, outValue);
		}

		logger.debug("Mapper for file " + header + " has finished.");
	}

}
