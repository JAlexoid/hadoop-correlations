package eu.activelogic.key.correlation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemporalKeyReducer implements Reducer<Text, SimpleArray, Text, SimpleArray> {

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
		
		ArrayList<Text[]> calls = new ArrayList<>();
		ArrayList<Text[]> smses = new ArrayList<>();
		
		reporter.incrCounter("REDUCE", "KEYS", 1);
		
		while(values.hasNext()){
			reporter.incrCounter("REDUCE", "ENTRIES", 1);
			SimpleArray v = values.next();
			try {
				int i = Integer.parseInt((v.get()[2]).toString());
				if(1 == i ){
					calls.add((Text[]) v.toArray());
					reporter.incrCounter("REDUCE", "CALLS", 1);
				} else if(i == 3) {
					smses.add((Text[]) v.toArray());
					reporter.incrCounter("REDUCE", "SMSES", 1);
				} else {
					reporter.incrCounter("REDUCE", "UNKNOWN", 1);
				}
			} catch (NumberFormatException e) {
				reporter.incrCounter("REDUCE", "ERROR", 1);
				return;
			}
		}
		
		
		for(Text[] vr : smses){
			reporter.incrCounter("REDUCE", "LOOP", 1);
			long time = Long.parseLong(vr[0].toString(), 16);
			long lastCall = Long.MIN_VALUE;
			Text[] callValue = null;
			
			for(Text[] cs: calls){
				long callTime = Long.parseLong(cs[0].toString(), 16);
				if(time >= callTime && lastCall < callTime){
					lastCall = callTime;
					callValue = cs;
				}
			}
			
			
			if(callValue != null){
				calls.remove(callValue);
				
				reporter.incrCounter("REDUCE", "OUTPUT", 1);
				long imsi = Long.parseLong(vr[1].toString(), 16);
				long callTime = Long.parseLong(callValue[0].toString(), 16);
				String number = callValue[4].toString();
				
				StringBuilder sb = new StringBuilder();
				sb.append("Call:");
				sb.append(number);
				sb.append(" @ ");
				sb.append(callTime);
				sb.append(" -> SMS @ +");
				sb.append((time-callTime));
				
				Text k = new Text(Long.toHexString(imsi)+Long.toHexString(time));
				
				output.collect(k, new SimpleArray(vr));
				output.collect(k, new SimpleArray(callValue));
				
			}
			
		}
		
	}

}
