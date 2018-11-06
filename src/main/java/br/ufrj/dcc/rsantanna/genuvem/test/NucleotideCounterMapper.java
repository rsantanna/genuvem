package br.ufrj.dcc.rsantanna.genuvem.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NucleotideCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final IntWritable ONE = new IntWritable(1);
	
	private final Text A = new Text("A");
	private final Text T = new Text("T");
	private final Text C = new Text("C");
	private final Text G = new Text("G");

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		if (value != null && value.charAt(0) != '>'){
			for (int i = 0; i < value.getLength(); i++){
				
				int charAt = value.charAt(i);
				
				Text nucleotide = null;
				switch(charAt){
				case 'A':
					nucleotide = A;
					break;
				case 'T':
					nucleotide = T;
					break;
				case 'C':
					nucleotide = C;
					break;
				case 'G':
					nucleotide = G;
					break;
				}

				context.write(nucleotide, ONE);
			}
		}
	}

}
