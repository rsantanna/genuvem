package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HighScoringPairWritable implements Writable, WritableComparable<HighScoringPairWritable> {

	private IndexKeyWritable queryPosition;
	private IndexKeyWritable matchPosition;

	public HighScoringPairWritable(IndexKeyWritable queryPosition, IndexKeyWritable matchPosition) {
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

	public IndexKeyWritable getQueryPosition() {
		return queryPosition;
	}

	public void setQueryPosition(IndexKeyWritable queryPosition) {
		this.queryPosition = queryPosition;
	}

	public IndexKeyWritable getMatchPosition() {
		return matchPosition;
	}

	public void setMatchPosition(IndexKeyWritable matchPosition) {
		this.matchPosition = matchPosition;
	}

	@Override
	public int compareTo(HighScoringPairWritable that) {
		int result = getQueryPosition().compareTo(that.getQueryPosition());
		return result != 0 ? result : getMatchPosition().compareTo(that.getMatchPosition());
	}
}
