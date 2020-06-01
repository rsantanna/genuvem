package genuvem.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.SearchKeyWritable;

public class QueryHspComparator extends WritableComparator {

	public QueryHspComparator() {
		super(SearchKeyWritable.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		SearchKeyWritable key1 = (SearchKeyWritable) a;
		SearchKeyWritable key2 = (SearchKeyWritable) b;

		HighScoringPairWritable hsp1 = (HighScoringPairWritable) key1.getHsp();
		HighScoringPairWritable hsp2 = (HighScoringPairWritable) key2.getHsp();

		int result = -hsp1.getLength().compareTo(hsp2.getLength());

		result = result != 0 ? result : hsp1.getQueryStart().compareTo(hsp2.getQueryStart());
		result = result != 0 ? result : hsp1.getQueryEnd().compareTo(hsp2.getQueryEnd());
		result = result != 0 ? result : hsp1.getDatabaseSequenceStart().compareTo(hsp2.getDatabaseSequenceStart());
		result = result != 0 ? result : hsp1.getDatabaseSequenceEnd().compareTo(hsp2.getDatabaseSequenceEnd());

		return result;
	}
}
