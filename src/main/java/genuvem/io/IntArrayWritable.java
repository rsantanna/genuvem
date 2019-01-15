package genuvem.io;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class);
		set(values);
	}

	@Override
	public String toString() {
		return Arrays.deepToString(get());
	}

}
