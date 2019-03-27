package genuvem.index.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fastdoop.PartialSequence;
import genuvem.io.TextIntWritable;

public class IndexEncoderMapper extends Mapper<Text, PartialSequence, IntWritable, TextIntWritable> {

	private Logger logger = Logger.getLogger(IndexEncoderMapper.class);

	private int kmerLength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt("kmerlength", 16);
	}

	@Override
	protected void map(Text key, PartialSequence value, Context context) throws IOException, InterruptedException {

		int header = Integer.parseInt(value.getHeader());
		IntWritable outKey = new IntWritable(header);

		logger.debug("Cleaning up headers");
		String seq = value.getValue().replaceAll(">.*$?m", "");

		logger.debug("Cleaning up line breaks and whitespaces in file " + header + ".");
		seq = seq.replaceAll("[ \t\r\n]", "");
		
		int currentIndex = 0;

		logger.debug("Starting mapping index entries in file " + header + ".");
		while (seq.length() >= currentIndex + kmerLength) {
			String subSeq = seq.substring(currentIndex, currentIndex + kmerLength).toUpperCase();

			TextIntWritable outValue = new TextIntWritable();
			outValue.setText(new Text(subSeq));
			outValue.setInteger(new IntWritable(currentIndex));

			logger.debug("Output at index " + currentIndex + " is [" + outKey + ", " + outValue + "]");
			context.write(outKey, outValue);

			currentIndex += kmerLength;
		}

		if (currentIndex < seq.length()) {
			String subSeq = seq.substring(currentIndex, seq.length()).toUpperCase();

			TextIntWritable outValue = new TextIntWritable();
			outValue.setText(new Text(subSeq));
			outValue.setInteger(new IntWritable(currentIndex));

			logger.debug("Output at index " + currentIndex + " is [" + outKey + ", " + outValue + "]");
			context.write(outKey, outValue);
		}

		logger.debug("Mapper for file " + header + " has finished.");
	}

}
