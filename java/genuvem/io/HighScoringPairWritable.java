package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HighScoringPairWritable implements Writable, WritableComparable<HighScoringPairWritable> {

	private IntWritable queryStart;
	private IntWritable queryEnd;
	private IntWritable databaseSequenceStart;
	private IntWritable databaseSequenceEnd;
	private IntWritable length;

	public HighScoringPairWritable() {
		this(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable());
	}

	public HighScoringPairWritable(IntWritable queryStart, IntWritable queryEnd, IntWritable databaseSequenceStart,
			IntWritable databaseSequenceEnd, IntWritable length) {
		this.queryStart = queryStart;
		this.queryEnd = queryEnd;
		this.databaseSequenceStart = databaseSequenceStart;
		this.databaseSequenceEnd = databaseSequenceEnd;
		this.length = length;
	}

	public IntWritable getQueryStart() {
		return queryStart;
	}

	public void setQueryStart(IntWritable queryStart) {
		this.queryStart = queryStart;
	}

	public IntWritable getQueryEnd() {
		return queryEnd;
	}

	public void setQueryEnd(IntWritable queryEnd) {
		this.queryEnd = queryEnd;
	}

	public IntWritable getDatabaseSequenceStart() {
		return databaseSequenceStart;
	}

	public void setDatabaseSequenceStart(IntWritable databaseSequenceStart) {
		this.databaseSequenceStart = databaseSequenceStart;
	}

	public IntWritable getDatabaseSequenceEnd() {
		return databaseSequenceEnd;
	}

	public void setDatabaseSequenceEnd(IntWritable databaseSequenceEnd) {
		this.databaseSequenceEnd = databaseSequenceEnd;
	}

	public IntWritable getLength() {
		return length;
	}

	public void setLength(IntWritable length) {
		this.length = length;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		queryStart.readFields(in);
		queryEnd.readFields(in);
		databaseSequenceStart.readFields(in);
		databaseSequenceEnd.readFields(in);
		length.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		queryStart.write(out);
		queryEnd.write(out);
		databaseSequenceStart.write(out);
		databaseSequenceEnd.write(out);
		length.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((databaseSequenceEnd == null) ? 0 : databaseSequenceEnd.hashCode());
		result = prime * result + ((databaseSequenceStart == null) ? 0 : databaseSequenceStart.hashCode());
		result = prime * result + ((length == null) ? 0 : length.hashCode());
		result = prime * result + ((queryEnd == null) ? 0 : queryEnd.hashCode());
		result = prime * result + ((queryStart == null) ? 0 : queryStart.hashCode());
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
		HighScoringPairWritable other = (HighScoringPairWritable) obj;
		if (databaseSequenceEnd == null) {
			if (other.databaseSequenceEnd != null)
				return false;
		} else if (!databaseSequenceEnd.equals(other.databaseSequenceEnd))
			return false;
		if (databaseSequenceStart == null) {
			if (other.databaseSequenceStart != null)
				return false;
		} else if (!databaseSequenceStart.equals(other.databaseSequenceStart))
			return false;
		if (length == null) {
			if (other.length != null)
				return false;
		} else if (!length.equals(other.length))
			return false;
		if (queryEnd == null) {
			if (other.queryEnd != null)
				return false;
		} else if (!queryEnd.equals(other.queryEnd))
			return false;
		if (queryStart == null) {
			if (other.queryStart != null)
				return false;
		} else if (!queryStart.equals(other.queryStart))
			return false;
		return true;
	}

	@Override
	public int compareTo(HighScoringPairWritable that) {
		int result;

		result = this.getQueryStart().compareTo(that.getQueryStart());
		result = result != 0 ? result : this.getQueryEnd().compareTo(that.getQueryEnd());
		result = result != 0 ? result : this.getDatabaseSequenceStart().compareTo(that.getDatabaseSequenceStart());
		result = result != 0 ? result : this.getDatabaseSequenceEnd().compareTo(that.getDatabaseSequenceEnd());
		result = result != 0 ? result : this.getLength().compareTo(that.getLength());

		return result;
	}

	@Override
	public String toString() {
		return "HighScoringPairWritable [queryStart=" + queryStart + ", queryEnd=" + queryEnd
				+ ", databaseSequenceStart=" + databaseSequenceStart + ", databaseSequenceEnd=" + databaseSequenceEnd
				+ ", length=" + length + "]";
	}

}
