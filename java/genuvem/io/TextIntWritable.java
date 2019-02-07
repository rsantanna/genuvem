package genuvem.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextIntWritable implements Writable, WritableComparable<TextIntWritable> {

	private Text text;
	private IntWritable integer;

	public TextIntWritable() {
		text = new Text();
		integer = new IntWritable(0);
	}

	public TextIntWritable(Text text, IntWritable integer) {
		this.text = text;
		this.integer = integer;
	}

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	public IntWritable getInteger() {
		return integer;
	}

	public void setInteger(IntWritable integer) {
		this.integer = integer;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		integer.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
		integer.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((integer == null) ? 0 : integer.hashCode());
		result = prime * result + ((text == null) ? 0 : text.hashCode());
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
		TextIntWritable other = (TextIntWritable) obj;
		if (integer == null) {
			if (other.integer != null)
				return false;
		} else if (!integer.equals(other.integer))
			return false;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}

	@Override
	public int compareTo(TextIntWritable that) {
		int result = getText().compareTo(that.getText());
		return result != 0 ? result : getInteger().compareTo(that.getInteger());
	}

	@Override
	public String toString() {
		return getText() + " @ " + getInteger();
	}
}
