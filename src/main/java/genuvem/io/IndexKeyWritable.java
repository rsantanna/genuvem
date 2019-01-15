package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IndexKeyWritable implements Writable, WritableComparable<IndexKeyWritable> {

	private Text subsequence;
	private IntWritable sequenceId;

	public IndexKeyWritable() {
		subsequence = new Text();
		sequenceId = new IntWritable(0);
	}

	public IndexKeyWritable(Text subsequence, IntWritable sequenceId) {
		this.subsequence = subsequence;
		this.sequenceId = sequenceId;
	}

	public Text getSubsequence() {
		return subsequence;
	}

	public void setSubsequence(Text subsequence) {
		this.subsequence = subsequence;
	}

	public IntWritable getSequenceId() {
		return sequenceId;
	}

	public void setSequenceId(IntWritable sequenceId) {
		this.sequenceId = sequenceId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		subsequence.readFields(in);
		sequenceId.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		subsequence.write(out);
		sequenceId.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sequenceId == null) ? 0 : sequenceId.hashCode());
		result = prime * result + ((subsequence == null) ? 0 : subsequence.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexKeyWritable other = (IndexKeyWritable) obj;
		if (sequenceId == null) {
			if (other.sequenceId != null)
				return false;
		} else if (!sequenceId.equals(other.sequenceId))
			return false;
		if (subsequence == null) {
			if (other.subsequence != null)
				return false;
		} else if (!subsequence.equals(other.subsequence))
			return false;
		return true;
	}

	@Override
	public int compareTo(IndexKeyWritable that) {
		int result = getSubsequence().compareTo(that.getSubsequence());
		return result != 0 ? result : getSequenceId().compareTo(that.getSequenceId());
	}

	@Override
	public String toString() {
		return getSubsequence() + " @ " + getSequenceId();
	}
}
