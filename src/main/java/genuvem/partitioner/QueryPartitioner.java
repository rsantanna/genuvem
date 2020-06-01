package genuvem.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.SearchKeyWritable;

public class QueryPartitioner extends Partitioner<SearchKeyWritable, HighScoringPairWritable> {

	@Override
	public int getPartition(SearchKeyWritable key, HighScoringPairWritable value, int numPartitions) {
		return key.getSequenceId().hashCode() % numPartitions;
	}

}
