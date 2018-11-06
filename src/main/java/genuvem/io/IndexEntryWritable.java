package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IndexEntryWritable
		implements Writable, WritableComparable<IndexEntryWritable> {

	private IntWritable sequenceId;
	private IntWritable position;

	public IndexEntryWritable() {
		sequenceId = new IntWritable(0);
		position = new IntWritable(0);
	}

	public IndexEntryWritable(IntWritable sequenceId, IntWritable position) {
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
		return sequenceId.toString() + ":" + position.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IndexEntryWritable) {
			return false;
		} else {
			IndexEntryWritable that = (IndexEntryWritable) obj;
			return getSequenceId().equals(that.getSequenceId())
					&& getPosition().equals(that.getPosition());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		result = prime * result + getSequenceId().hashCode();
		result = prime * result + getPosition().hashCode();

		return result;
	}

	@Override
	public int compareTo(IndexEntryWritable that) {
		return getPosition().compareTo(that.getPosition());
	}
}
