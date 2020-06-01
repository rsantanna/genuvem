package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SearchKeyWritable implements Writable, WritableComparable<SearchKeyWritable> {

	private IntWritable sequenceId;
	private HighScoringPairWritable hsp;

	public SearchKeyWritable() {
		this(new IntWritable(), new HighScoringPairWritable());
	}

	public SearchKeyWritable(IntWritable sequenceId, HighScoringPairWritable hsp) {
		this.sequenceId = sequenceId;
		this.hsp = hsp;
	}

	public IntWritable getSequenceId() {
		return sequenceId;
	}

	public void setSequenceId(IntWritable sequenceId) {
		this.sequenceId = sequenceId;
	}

	public HighScoringPairWritable getHsp() {
		return hsp;
	}

	public void setHsp(HighScoringPairWritable hsp) {
		this.hsp = hsp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sequenceId.readFields(in);
		hsp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sequenceId.write(out);
		hsp.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hsp == null) ? 0 : hsp.hashCode());
		result = prime * result + ((sequenceId == null) ? 0 : sequenceId.hashCode());
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
		SearchKeyWritable other = (SearchKeyWritable) obj;
		if (hsp == null) {
			if (other.hsp != null)
				return false;
		} else if (!hsp.equals(other.hsp))
			return false;
		if (sequenceId == null) {
			if (other.sequenceId != null)
				return false;
		} else if (!sequenceId.equals(other.sequenceId))
			return false;
		return true;
	}

	@Override
	public int compareTo(SearchKeyWritable that) {
		int result = this.getSequenceId().compareTo(that.getSequenceId());
		return result != 0 ? result : this.getHsp().compareTo(that.getHsp());
	}

}
