package genuvem.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import genuvem.io.SearchKeyWritable;

public class QuerySequenceComparator extends WritableComparator {

	public QuerySequenceComparator() {
		super(SearchKeyWritable.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		SearchKeyWritable key1 = (SearchKeyWritable) a;
		SearchKeyWritable key2 = (SearchKeyWritable) b;
		return key1.getSequenceId().compareTo(key2.getSequenceId());
	}
}
