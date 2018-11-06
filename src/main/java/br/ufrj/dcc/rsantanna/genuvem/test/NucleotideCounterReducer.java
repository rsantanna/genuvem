package br.ufrj.dcc.rsantanna.genuvem.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NucleotideCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable value : values){
			sum += value.get();
		}
		
		IntWritable result = new IntWritable(sum);
		context.write(key, result);
	}

}
