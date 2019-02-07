package genuvem.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import genuvem.io.QueryKeyWritable;

public class QueryGroupingComparator extends WritableComparator {

	public QueryGroupingComparator() {
		super(QueryKeyWritable.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		QueryKeyWritable key1 = (QueryKeyWritable) a;
		QueryKeyWritable key2 = (QueryKeyWritable) b;
		return key1.getSequenceId().compareTo(key2.getSequenceId());
	}
}
