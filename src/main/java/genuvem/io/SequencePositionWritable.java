package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SequencePositionWritable implements Writable, WritableComparable<SequencePositionWritable> {

	private IntWritable sequenceId;
	private IntWritable position;

	public SequencePositionWritable() {
		this(new IntWritable(), new IntWritable());
	}

	public SequencePositionWritable(IntWritable sequenceId, IntWritable position) {
		this.sequenceId = sequenceId;
		this.position = position;
	}

	public IntWritable getSequenceId() {
		return sequenceId;
	}

	public void setSequenceId(IntWritable sequenceId) {
		this.sequenceId = sequenceId;
	}

	public IntWritable getPosition() {
		return position;
	}

	public void setPosition(IntWritable position) {
		this.position = position;
	}

	@Override
	public int compareTo(SequencePositionWritable that) {
		int result = getSequenceId().compareTo(that.getSequenceId());
		return result != 0 ? result : getPosition().compareTo(that.getPosition());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sequenceId.readFields(in);
		position.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sequenceId.write(out);
		position.write(out);
	}

	@Override
	public String toString() {
		return "[ " + getSequenceId().toString() + " @ " + getPosition().toString() + " ]";
	}

}
