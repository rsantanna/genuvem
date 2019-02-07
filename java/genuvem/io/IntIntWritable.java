package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntIntWritable implements Writable, WritableComparable<IntIntWritable> {

	private IntWritable int1;
	private IntWritable int2;

	public IntIntWritable() {
		this(new IntWritable(), new IntWritable());
	}

	public IntIntWritable(IntWritable int1, IntWritable int2) {
		this.int1 = int1;
		this.int2 = int2;
	}

	public IntWritable getInt1() {
		return int1;
	}

	public void setInt1(IntWritable int1) {
		this.int1 = int1;
	}

	public IntWritable getInt2() {
		return int2;
	}

	public void setInt2(IntWritable int2) {
		this.int2 = int2;
	}

	@Override
	public int compareTo(IntIntWritable that) {
		int result = getInt1().compareTo(that.getInt1());
		return result != 0 ? result : getInt2().compareTo(that.getInt2());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int1.readFields(in);
		int2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int1.write(out);
		int2.write(out);
	}

	@Override
	public String toString() {
		return "[ " + getInt1().toString() + " @ " + getInt2().toString() + " ]";
	}

}
