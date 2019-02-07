package genuvem.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import genuvem.io.HighScoringPairWritable;
import genuvem.io.QueryKeyWritable;

public class QueryPartitioner extends Partitioner<QueryKeyWritable, HighScoringPairWritable> {

	@Override
	public int getPartition(QueryKeyWritable key, HighScoringPairWritable value, int numPartitions) {
		return key.getSequenceId().hashCode() % numPartitions;
	}

}
