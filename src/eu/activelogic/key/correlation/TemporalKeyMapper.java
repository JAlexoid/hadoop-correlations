package eu.activelogic.key.correlation;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TemporalKeyMapper implements
		Mapper<LongWritable, Text, Text, SimpleArray> {

	TemporalCorrelatingKey keyGenCall = new TemporalCorrelatingKey(7 * 60, 0);
	TemporalCorrelatingKey keyGenSMS = new TemporalCorrelatingKey(7 * 60, -1 * 7 * 60);

	@Override
	public void configure(JobConf job) {

	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, SimpleArray> output, Reporter reporter)
			throws IOException {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String[] items = value.toString().split("\t");
		

		Long imsi;
		long time;
		int type;
		String number = items[3];
		int amount;
		try {
			time = sdf.parse(items[6]).getTime() / 1000l;
			imsi = Long.valueOf(items[1]);
			type = Integer.parseInt(items[5]);
			amount = Integer.parseInt(items[4]);
		} catch (Exception e) {
			reporter.incrCounter("MAP", "ERROR", 1);
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append(Long.toHexString(imsi));
		sb.append(number);
		sb.append("-");
		String keyBase = sb.toString();

		Text[] out = new Text[5];
		out[0] = new Text(Long.toHexString(time));
		out[1] = new Text(Long.toHexString(imsi));
		out[2] = new Text(Integer.toString(type));
		out[3] = new Text(Integer.toHexString(amount));
		out[4] = new Text(number);
		
		
		SimpleArray outputArray = new SimpleArray(out);

		Text okey;
		switch (type) {
		case 3:
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenSMS.generateBaseKey(time)));
			okey = new Text(sb.toString());
			output.collect(okey, outputArray);
			
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenSMS.generateTargetKey(time)));
			okey = new Text(sb.toString());
			output.collect(okey, outputArray);
			
			break;
		case 1:
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenCall.generateBaseKey(time)));
			okey = new Text(sb.toString());

			output.collect(okey, outputArray);
			break;
		default:

		}

	}

	@Override
	public void close() throws IOException {

	}

}
