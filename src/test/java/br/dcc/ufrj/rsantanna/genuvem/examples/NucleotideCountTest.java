package br.dcc.ufrj.rsantanna.genuvem.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import br.dcc.ufrj.rsantanna.genuvem.examples.NucleotideCount.NucleotideMapper;

public class NucleotideCountTest {

	@Test
	public void test() throws IOException {
		Text key	= new Text("Teste");		
		Text value 	= new Text("");

		new MapDriver<Object, Text, Text, IntWritable>()
			.withMapper(new NucleotideMapper())
			.withInput(key, value)
			.runTest();
	}
}
