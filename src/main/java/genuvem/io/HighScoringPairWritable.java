package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HighScoringPairWritable implements Writable, WritableComparable<HighScoringPairWritable> {

	private TextIntWritable queryPosition;
	private TextIntWritable matchPosition;

	public HighScoringPairWritable(TextIntWritable queryPosition, TextIntWritable matchPosition) {
		this.queryPosition = queryPosition;
		this.matchPosition = matchPosition;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		queryPosition.readFields(in);
		matchPosition.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		queryPosition.write(out);
		matchPosition.write(out);
	}

	public TextIntWritable getQueryPosition() {
		return queryPosition;
	}

	public void setQueryPosition(TextIntWritable queryPosition) {
		this.queryPosition = queryPosition;
	}

	public TextIntWritable getMatchPosition() {
		return matchPosition;
	}

	public void setMatchPosition(TextIntWritable matchPosition) {
		this.matchPosition = matchPosition;
	}

	@Override
	public int compareTo(HighScoringPairWritable that) {
		int result = getQueryPosition().compareTo(that.getQueryPosition());
		return result != 0 ? result : getMatchPosition().compareTo(that.getMatchPosition());
	}
}
