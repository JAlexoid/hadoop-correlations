package eu.activelogic.key.correlation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemporalKeyCombiner implements
		Reducer<Text, SimpleArray, Text, SimpleArray> {

	@Override
	public void configure(JobConf job) {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void reduce(Text key, Iterator<SimpleArray> values,
			OutputCollector<Text, SimpleArray> output,
			Reporter reporter) throws IOException {

		// Double counting?
		
		while(values.hasNext()){
			output.collect(key, values.next());
		}
		
		
		
		
		
	}

}
