package genuvem.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fastdoop.PartialSequence;

public class IndexBuilderMapper extends Mapper<Text, PartialSequence, Text, Text> {

	private static final int K_MER_LENGTH = 16;

	@Override
	protected void map(Text key, PartialSequence value, Context context) throws IOException, InterruptedException {

		String header = value.getHeader();
		String seq = value.getValue().replace("\n", "");

		Text outKey = new Text();
		Text outVal = new Text();

		int currentIndex = 0;

		while (seq.length() >= currentIndex + K_MER_LENGTH) {
			outKey.set(header + String.format("_%05d_", currentIndex) + value.getStartValue() + "_" + value.getEndValue());

			String subSeq = seq.substring(currentIndex, currentIndex + K_MER_LENGTH);
			outVal.set(subSeq);
			context.write(outKey, outVal);

			currentIndex += K_MER_LENGTH;
		}

		if (currentIndex < seq.length()) {
			outKey.set(header + String.format("_%05d", currentIndex));

			String subSeq = seq.substring(currentIndex, seq.length());
			outVal.set(subSeq);
			context.write(outKey, outVal);
		}

	}

}
