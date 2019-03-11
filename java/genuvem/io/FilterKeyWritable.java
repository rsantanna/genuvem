package genuvem.io;

import org.apache.hadoop.io.IntWritable;

public class FilterKeyWritable extends SearchKeyWritable {

	public FilterKeyWritable() {
		super(new IntWritable(), new HighScoringPairWritable());
	}

	public FilterKeyWritable(IntWritable sequenceId, HighScoringPairWritable hsp) {
		super(sequenceId, hsp);
	}

	@Override
	public int compareTo(SearchKeyWritable that) {

		if (that instanceof FilterKeyWritable) {
			HighScoringPairWritable hsp1 = (HighScoringPairWritable) this.getHsp();
			HighScoringPairWritable hsp2 = (HighScoringPairWritable) that.getHsp();

			int result = this.getSequenceId().compareTo(that.getSequenceId());
			result = result != 0 ? result : -hsp1.getLength().compareTo(hsp2.getLength());
			result = result != 0 ? result : hsp1.getQueryStart().compareTo(hsp2.getQueryStart());
			result = result != 0 ? result : hsp1.getQueryEnd().compareTo(hsp2.getQueryEnd());
			result = result != 0 ? result : hsp1.getDatabaseSequenceStart().compareTo(hsp2.getDatabaseSequenceStart());
			result = result != 0 ? result : hsp1.getDatabaseSequenceEnd().compareTo(hsp2.getDatabaseSequenceEnd());

			return result;
		} else {
			return super.compareTo(that);
		}

	}

}
